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
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	common "github.com/dapr/components-contrib/common/component/aws/opensearch"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/kit/logger"
)

// OpenSearch implements lexical search using an AWS SigV4-authenticated OpenSearch endpoint.
type OpenSearch struct {
	log    logger.Logger
	mu     sync.RWMutex
	client *common.Client
	closed bool
}

func NewOpenSearch(log logger.Logger) search.Search {
	return &OpenSearch{log: log}
}

func (o *OpenSearch) Init(ctx context.Context, meta search.Metadata) error {
	client, err := common.NewClient(ctx, meta.Properties, o.log)
	if err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.client != nil {
		if err = o.client.Close(); err != nil {
			return err
		}
	}
	o.client, o.closed = client, false
	return nil
}

func (o *OpenSearch) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil
	}
	o.closed = true
	if o.client != nil {
		return o.client.Close()
	}
	return nil
}

func (o *OpenSearch) ready() (*common.Client, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.closed || o.client == nil {
		return nil, status.Error(codes.FailedPrecondition, "OpenSearch search component is not initialized or is closed")
	}
	return o.client, nil
}

func (o *OpenSearch) GetComponentMetadata() (info metadata.MetadataMap) {
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(common.Metadata{}), &info, metadata.SearchType)
	for name, field := range info {
		switch strings.ToLower(name) {
		case "logger", "properties":
			delete(info, name)
		case "endpoint", "timeout":
		default:
			field.Ignored = true
			info[name] = field
		}
	}
	return info
}

func (o *OpenSearch) CreateIndex(ctx context.Context, req *search.CreateIndexRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	path, err := common.IndexPath(req.Index)
	if err != nil {
		return err
	}
	body := map[string]any{
		"mappings": map[string]any{
			"_meta":          map[string]any{"dapr_kind": "search"},
			"date_detection": false,
			"dynamic_templates": []any{map[string]any{"strings": map[string]any{
				"match_mapping_type": "string",
				"mapping": map[string]any{"type": "text", "fields": map[string]any{
					"keyword": map[string]any{"type": "keyword"},
				}},
			}}},
			"properties": map[string]any{
				"id":       map[string]any{"type": "keyword"},
				"content":  map[string]any{"type": "object"},
				"raw":      map[string]any{"type": "binary"},
				"metadata": map[string]any{"type": "object", "enabled": false},
			},
		},
	}
	for key, value := range req.Metadata {
		if key != "settings" {
			return status.Errorf(codes.InvalidArgument, "unsupported index metadata %q; use settings with an OpenSearch settings JSON object", key)
		}
		var settings map[string]any
		if err = json.Unmarshal([]byte(value), &settings); err != nil || settings == nil {
			return status.Error(codes.InvalidArgument, "settings must be a JSON object")
		}
		body["settings"] = settings
	}
	client, err := o.ready()
	if err != nil {
		return err
	}
	return acknowledged(ctx, client, "PUT", path, body)
}

func (o *OpenSearch) GetIndex(ctx context.Context, req *search.GetIndexRequest) (*search.GetIndexResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	path, err := common.IndexPath(req.Index)
	if err != nil {
		return nil, err
	}
	client, err := o.ready()
	if err != nil {
		return nil, err
	}
	if _, err = searchIndexMapping(ctx, client, path); err != nil {
		return nil, err
	}
	var count struct {
		Count  uint64 `json:"count"`
		Shards struct {
			Failed int `json:"failed"`
		} `json:"_shards"`
	}
	if err = client.Do(ctx, "GET", path+"/_count", nil, &count); err != nil {
		return nil, err
	}
	if count.Shards.Failed != 0 {
		return nil, status.Error(codes.Unavailable, "OpenSearch count has failed shards")
	}
	return &search.GetIndexResponse{Index: req.Index, DocumentCount: count.Count}, nil
}

func (o *OpenSearch) ListIndexes(ctx context.Context, req *search.ListIndexesRequest) (*search.ListIndexesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	client, err := o.ready()
	if err != nil {
		return nil, err
	}
	indexes, err := client.ListIndexes(ctx, "search")
	if err != nil {
		return nil, err
	}
	return &search.ListIndexesResponse{Indexes: indexes}, nil
}

func (o *OpenSearch) DeleteIndex(ctx context.Context, req *search.DeleteIndexRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	path, err := common.IndexPath(req.Index)
	if err != nil {
		return err
	}
	client, err := o.ready()
	if err != nil {
		return err
	}
	if _, err = searchIndexMapping(ctx, client, path); err != nil {
		return err
	}
	return acknowledged(ctx, client, "DELETE", path, nil)
}

func acknowledged(ctx context.Context, client *common.Client, method, path string, body any) error {
	var response struct {
		Acknowledged bool `json:"acknowledged"`
	}
	if err := client.Do(ctx, method, path, body, &response); err != nil {
		return err
	}
	if !response.Acknowledged {
		return status.Error(codes.DeadlineExceeded, "OpenSearch index operation was not acknowledged; outcome is unknown")
	}
	return nil
}

// Raw retains whitespace, key order and number spellings that _source normalization can change.
type envelope struct {
	ID       string            `json:"id"`
	Content  json.RawMessage   `json:"content"`
	Raw      []byte            `json:"raw"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

func (o *OpenSearch) IndexDocuments(ctx context.Context, req *search.IndexDocumentsRequest) (*search.IndexDocumentsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	path, err := common.IndexPath(req.Index)
	if err != nil {
		return nil, err
	}
	ctx, cancel, err := common.WithWriteContext(ctx, req.Options)
	if err != nil {
		return nil, err
	}
	defer cancel()
	docs := make([]common.Document, len(req.Documents))
	seen := make(map[string]struct{}, len(docs))
	for i, doc := range req.Documents {
		if doc.ID == "" {
			return nil, status.Error(codes.InvalidArgument, "document IDs must not be empty")
		}
		if _, exists := seen[doc.ID]; exists {
			return nil, status.Errorf(codes.InvalidArgument, "duplicate document ID %q", doc.ID)
		}
		seen[doc.ID] = struct{}{}
		content := bytes.TrimSpace(doc.Content)
		if len(content) == 0 || content[0] != '{' || !json.Valid(content) {
			return nil, status.Errorf(codes.InvalidArgument, "document %q content must be a JSON object", doc.ID)
		}
		source, marshalErr := json.Marshal(envelope{ID: doc.ID, Content: doc.Content, Raw: doc.Content, Metadata: doc.Metadata})
		if marshalErr != nil {
			return nil, status.Errorf(codes.InvalidArgument, "encode document %q: %v", doc.ID, marshalErr)
		}
		docs[i] = common.Document{ID: doc.ID, Source: source}
	}
	client, err := o.ready()
	if err != nil {
		return nil, err
	}
	if _, err = searchIndexMapping(ctx, client, path); err != nil {
		return nil, err
	}
	failures, err := client.Bulk(ctx, req.Index, docs, nil)
	if err != nil {
		return nil, err
	}
	return &search.IndexDocumentsResponse{Ack: search.IndexAckCompleted, FailedItems: failures}, nil
}

func (o *OpenSearch) GetDocuments(ctx context.Context, req *search.GetDocumentsRequest) (*search.GetDocumentsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	path, err := common.IndexPath(req.Index)
	if err != nil {
		return nil, err
	}
	if err = validateIDs(req.IDs); err != nil {
		return nil, err
	}
	client, err := o.ready()
	if err != nil {
		return nil, err
	}
	if _, err = searchIndexMapping(ctx, client, path); err != nil {
		return nil, err
	}
	docs, err := client.GetDocuments(ctx, req.Index, req.IDs)
	if err != nil {
		return nil, err
	}
	result := &search.GetDocumentsResponse{Documents: make([]search.Document, 0, len(docs))}
	for _, doc := range docs {
		decoded, decodeErr := decodeDocument(doc.ID, doc.Source, req.IncludeContent, nil)
		if decodeErr != nil {
			return nil, decodeErr
		}
		result.Documents = append(result.Documents, decoded)
	}
	return result, nil
}

func (o *OpenSearch) DeleteDocuments(ctx context.Context, req *search.DeleteDocumentsRequest) (*search.DeleteDocumentsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	path, err := common.IndexPath(req.Index)
	if err != nil {
		return nil, err
	}
	if err = validateIDs(req.IDs); err != nil {
		return nil, err
	}
	ctx, cancel, err := common.WithWriteContext(ctx, req.Options)
	if err != nil {
		return nil, err
	}
	defer cancel()
	client, err := o.ready()
	if err != nil {
		return nil, err
	}
	if _, err = searchIndexMapping(ctx, client, path); err != nil {
		return nil, err
	}
	failures, err := client.Bulk(ctx, req.Index, nil, req.IDs)
	if err != nil {
		return nil, err
	}
	// DeleteDocuments has no per-item failure field: a partial failure must fail the call.
	if len(failures) > 0 {
		failure := failures[0]
		if failure.Error == nil || failure.Error.Code() == codes.OK {
			return nil, status.Errorf(codes.Internal, "delete document %q failed without a valid status", failure.ID)
		}
		return nil, failure.Error.Err()
	}
	return &search.DeleteDocumentsResponse{Ack: search.IndexAckCompleted}, nil
}

func validateIDs(ids []string) error {
	for _, id := range ids {
		if id == "" {
			return status.Error(codes.InvalidArgument, "document IDs must not be empty")
		}
	}
	return nil
}

func decodeDocument(id string, source json.RawMessage, include bool, fields []string) (search.Document, error) {
	var body envelope
	if err := json.Unmarshal(source, &body); err != nil || id == "" || len(bytes.TrimSpace(body.Content)) == 0 || bytes.TrimSpace(body.Content)[0] != '{' {
		return search.Document{}, status.Errorf(codes.Internal, "invalid OpenSearch source for document %q", id)
	}
	doc := search.Document{ID: id, Metadata: body.Metadata}
	if len(fields) > 0 {
		var object map[string]any
		decoder := json.NewDecoder(bytes.NewReader(body.Content))
		decoder.UseNumber()
		if err := decoder.Decode(&object); err != nil || object == nil {
			return doc, status.Errorf(codes.Internal, "invalid document content for %q", id)
		}
		projection := make(map[string]any)
		for _, field := range fields {
			project(projection, object, strings.Split(field, "."))
		}
		content, err := json.Marshal(projection)
		if err != nil {
			return doc, status.Errorf(codes.Internal, "encode projected content: %v", err)
		}
		doc.Content = content
	} else if include {
		if !json.Valid(body.Raw) || len(bytes.TrimSpace(body.Raw)) == 0 || bytes.TrimSpace(body.Raw)[0] != '{' {
			return doc, status.Errorf(codes.Internal, "missing or invalid raw content for document %q", id)
		}
		doc.Content = body.Raw
	}
	return doc, nil
}

func project(dst, src map[string]any, path []string) {
	value, ok := src[path[0]]
	if !ok {
		return
	}
	if len(path) == 1 {
		dst[path[0]] = value
		return
	}
	switch child := value.(type) {
	case map[string]any:
		target, ok := dst[path[0]].(map[string]any)
		if !ok {
			target = make(map[string]any)
			dst[path[0]] = target
		}
		project(target, child, path[1:])
	case []any:
		target, ok := dst[path[0]].([]any)
		if !ok {
			target = make([]any, len(child))
			dst[path[0]] = target
		}
		for i, item := range child {
			if object, ok := item.(map[string]any); ok {
				projected, ok := target[i].(map[string]any)
				if !ok {
					projected = make(map[string]any)
					target[i] = projected
				}
				project(projected, object, path[1:])
			}
		}
	}
}
