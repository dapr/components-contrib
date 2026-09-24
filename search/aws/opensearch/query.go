/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package opensearch

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"unicode"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	common "github.com/dapr/components-contrib/common/component/aws/opensearch"
	"github.com/dapr/components-contrib/search"
)

const defaultTopK = 20

type continuation struct {
	Fingerprint string            `json:"q"`
	After       []json.RawMessage `json:"a"`
}

type searchResult struct {
	TimedOut bool `json:"timed_out"`
	Shards   struct {
		Failed int `json:"failed"`
	} `json:"_shards"`
	Hits *struct {
		Total *struct {
			Value    uint64 `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []struct {
			ID        string              `json:"_id"`
			Source    json.RawMessage     `json:"_source"`
			Score     float64             `json:"_score"`
			Sort      []json.RawMessage   `json:"sort"`
			Highlight map[string][]string `json:"highlight"`
		} `json:"hits"`
	} `json:"hits"`
}

func (o *OpenSearch) Search(ctx context.Context, req *search.SearchRequest) (*search.SearchResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	path, err := common.IndexPath(req.Index)
	if err != nil {
		return nil, err
	}
	body, limit, fingerprint, err := buildQuery(req)
	if err != nil {
		return nil, err
	}
	client, err := o.ready()
	if err != nil {
		return nil, err
	}
	mapping, err := searchIndexMapping(ctx, client, path)
	if err != nil {
		return nil, err
	}
	sorts, err := sortFields(mapping, req.Sort)
	if err != nil {
		return nil, err
	}
	body["sort"] = sorts
	if req.ContinuationToken != "" {
		after, decodeErr := decodeContinuation(req.ContinuationToken, fingerprint, len(sorts))
		if decodeErr != nil {
			return nil, decodeErr
		}
		body["search_after"] = after
	}
	var result searchResult
	if err = client.Do(ctx, "POST", path+"/_search", body, &result); err != nil {
		return nil, err
	}
	if result.TimedOut {
		return nil, status.Error(codes.DeadlineExceeded, "OpenSearch search timed out")
	}
	if result.Shards.Failed != 0 {
		return nil, status.Error(codes.Unavailable, "OpenSearch search has failed shards")
	}
	if result.Hits == nil {
		return nil, status.Error(codes.Internal, "OpenSearch omitted search hits")
	}
	response := &search.SearchResponse{Hits: make([]search.Hit, 0, min(limit, len(result.Hits.Hits)))}
	if result.Hits.Total != nil {
		response.TotalHits = &result.Hits.Total.Value
		switch result.Hits.Total.Relation {
		case "eq":
			response.TotalHitsRelation = search.TotalHitsRelationExact
		case "gte":
			response.TotalHitsRelation = search.TotalHitsRelationLowerBound
		default:
			return nil, status.Error(codes.Internal, "OpenSearch returned an unknown total-hits relation")
		}
	}
	for i, hit := range result.Hits.Hits {
		if i == limit {
			break
		}
		doc, decodeErr := decodeDocument(hit.ID, hit.Source, req.IncludeContent, req.ReturnFields)
		if decodeErr != nil {
			return nil, decodeErr
		}
		highlights := make(map[string]string, len(hit.Highlight))
		for field, fragments := range hit.Highlight {
			if !strings.HasPrefix(field, "content.") {
				return nil, status.Error(codes.Internal, "OpenSearch returned a highlight outside document content")
			}
			highlights[strings.TrimPrefix(field, "content.")] = strings.Join(fragments, " … ")
		}
		response.Hits = append(response.Hits, search.Hit{Document: doc, Score: hit.Score, Highlights: highlights})
	}
	if len(result.Hits.Hits) > limit {
		after := result.Hits.Hits[limit-1].Sort
		if len(after) != len(sorts) {
			return nil, status.Error(codes.Internal, "OpenSearch omitted pagination sort values")
		}
		data, marshalErr := json.Marshal(continuation{Fingerprint: fingerprint, After: after})
		if marshalErr != nil {
			return nil, status.Errorf(codes.Internal, "encode continuation: %v", marshalErr)
		}
		response.ContinuationToken = base64.RawURLEncoding.EncodeToString(data)
	}
	return response, nil
}

func buildQuery(req *search.SearchRequest) (map[string]any, int, string, error) {
	if req.Text != "" && len(req.Native) != 0 {
		return nil, 0, "", status.Error(codes.InvalidArgument, "text and native are mutually exclusive")
	}
	for _, fields := range [][]string{req.ReturnFields, req.SearchFields, req.HighlightFields} {
		for _, field := range fields {
			if err := validateField(field); err != nil {
				return nil, 0, "", err
			}
		}
	}
	for _, clause := range req.Sort {
		if err := validateField(clause.Field); err != nil {
			return nil, 0, "", err
		}
		if clause.Order != search.SortOrderUnspecified && clause.Order != search.SortOrderAsc && clause.Order != search.SortOrderDesc {
			return nil, 0, "", status.Error(codes.InvalidArgument, "invalid sort order")
		}
	}
	limit := int(req.TopK)
	if limit == 0 {
		limit = defaultTopK
	}
	if limit > 9999 {
		return nil, 0, "", status.Error(codes.InvalidArgument, "top_k must not exceed 9999")
	}
	var query any = map[string]any{"match_all": map[string]any{}}
	if req.Text != "" {
		fields := []string{"content.*"}
		if len(req.SearchFields) > 0 {
			fields = make([]string, len(req.SearchFields))
			for i, field := range req.SearchFields {
				fields[i] = "content." + field
			}
		}
		query = map[string]any{"multi_match": map[string]any{"query": req.Text, "fields": fields, "lenient": true}}
	}
	if len(req.Native) > 0 {
		if len(req.SearchFields) > 0 {
			return nil, 0, "", status.Error(codes.InvalidArgument, "search_fields cannot restrict a native query; specify fields in the native Query DSL")
		}
		for _, key := range []string{"from", "size", "search_after", "sort", "pit", "scroll", "offset", "limit", "page", "hitsPerPage"} {
			if _, exists := req.Native[key]; exists {
				return nil, 0, "", status.Errorf(codes.InvalidArgument, "native %q conflicts with portable pagination or sorting", key)
			}
		}
		for key := range req.Native {
			switch key {
			case "query":
			default:
				return nil, 0, "", status.Errorf(codes.Unimplemented, "unsupported native search option %q; supply an OpenSearch query object", key)
			}
		}
		if native, ok := req.Native["query"].(map[string]any); ok && len(native) > 0 {
			query = native
		} else {
			return nil, 0, "", status.Error(codes.InvalidArgument, "native.query must be a non-empty OpenSearch query object")
		}
	}
	if len(req.Filter) > 0 {
		filter, err := common.TranslateFilter(req.Filter, "content.")
		if err != nil {
			return nil, 0, "", err
		}
		query = map[string]any{"bool": map[string]any{"must": []any{query}, "filter": []any{filter}}}
	}
	body := map[string]any{"query": query, "size": limit + 1, "track_total_hits": true, "track_scores": true}
	if len(req.HighlightFields) > 0 {
		fields := make(map[string]any, len(req.HighlightFields))
		for _, field := range req.HighlightFields {
			fields["content."+field] = map[string]any{}
		}
		body["highlight"] = map[string]any{"fields": fields}
	}
	shape := *req
	shape.ContinuationToken, shape.Metadata = "", nil
	shape.TopK = uint32(limit)
	data, err := json.Marshal(shape)
	if err != nil {
		return nil, 0, "", status.Errorf(codes.InvalidArgument, "encode search query: %v", err)
	}
	hash := sha256.Sum256(data)
	return body, limit, hex.EncodeToString(hash[:]), nil
}

func validateField(field string) error {
	if field == "" {
		return status.Error(codes.InvalidArgument, "field paths must not be empty")
	}
	for _, part := range strings.Split(field, ".") {
		if part == "" || strings.IndexFunc(part, func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '-'
		}) >= 0 {
			return status.Errorf(codes.InvalidArgument, "invalid field path %q", field)
		}
	}
	return nil
}

type fieldMapping struct {
	Type       string                  `json:"type"`
	Properties map[string]fieldMapping `json:"properties"`
	Fields     map[string]fieldMapping `json:"fields"`
	Meta       struct {
		Kind string `json:"dapr_kind"`
	} `json:"_meta"`
}

func searchIndexMapping(ctx context.Context, client *common.Client, path string) (fieldMapping, error) {
	var mapping map[string]struct {
		Mappings fieldMapping `json:"mappings"`
	}
	if err := client.Do(ctx, "GET", path+"/_mapping", nil, &mapping); err != nil {
		return fieldMapping{}, err
	}
	if len(mapping) != 1 {
		return fieldMapping{}, status.Error(codes.Internal, "expected a single OpenSearch index mapping")
	}
	for _, index := range mapping {
		if index.Mappings.Meta.Kind != "search" {
			return fieldMapping{}, status.Error(codes.FailedPrecondition, "OpenSearch index is not a Dapr search index")
		}
		return index.Mappings, nil
	}
	return fieldMapping{}, status.Error(codes.Internal, "OpenSearch omitted index mapping")
}

func sortFields(root fieldMapping, clauses []search.SortClause) ([]any, error) {
	sorts := make([]any, 0, len(clauses)+1)
	if len(clauses) == 0 {
		sorts = append(sorts, map[string]any{"_score": "desc"})
	} else {
		for _, clause := range clauses {
			field := root
			name := "content." + clause.Field
			for _, part := range strings.Split(name, ".") {
				field = field.Properties[part]
				if field.Type == "nested" {
					return nil, status.Error(codes.Unimplemented, "sorting nested OpenSearch fields is unsupported")
				}
			}
			if field.Type == "text" {
				if field.Fields["keyword"].Type != "keyword" {
					return nil, status.Errorf(codes.Unimplemented, "field %q has no sortable keyword mapping", clause.Field)
				}
				name += ".keyword"
			}
			direction := "asc"
			if clause.Order == search.SortOrderDesc {
				direction = "desc"
			}
			sorts = append(sorts, map[string]any{name: map[string]any{"order": direction, "unmapped_type": "keyword"}})
		}
	}
	// _id has no doc values in OpenSearch; the envelope's keyword ID is the tie-breaker.
	return append(sorts, map[string]any{"id": "asc"}), nil
}

func decodeContinuation(token, fingerprint string, sortCount int) ([]json.RawMessage, error) {
	if len(token) > 16384 {
		return nil, status.Error(codes.InvalidArgument, "continuation token is too large")
	}
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid continuation token")
	}
	var cursor continuation
	if err = json.Unmarshal(data, &cursor); err != nil || cursor.Fingerprint != fingerprint || len(cursor.After) != sortCount {
		return nil, status.Error(codes.InvalidArgument, "continuation token does not match this query")
	}
	for _, value := range cursor.After {
		var scalar any
		if err = json.Unmarshal(value, &scalar); err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid continuation sort value")
		}
		switch scalar.(type) {
		case nil, string, float64, bool:
		default:
			return nil, status.Error(codes.InvalidArgument, "invalid continuation sort value")
		}
	}
	return cursor.After, nil
}
