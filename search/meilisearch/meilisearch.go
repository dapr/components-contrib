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

package meilisearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	meilisearchgo "github.com/meilisearch/meilisearch-go"

	commonmeilisearch "github.com/dapr/components-contrib/common/component/meilisearch"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"
)

const defaultSearchLim = int64(20)

// Meilisearch implements the Dapr Search building block with Meilisearch.
type Meilisearch struct {
	logger logger.Logger

	mu     sync.RWMutex
	client meilisearchgo.ServiceManager
	md     commonmeilisearch.MeilisearchMetadata
	closed bool
}

// NewMeilisearch creates a Meilisearch search component.
func NewMeilisearch(logger logger.Logger) search.Search {
	return &Meilisearch{logger: logger}
}

// Init initializes the Meilisearch search component.
func (m *Meilisearch) Init(ctx context.Context, meta search.Metadata) error {
	md := commonmeilisearch.MeilisearchMetadata{}
	if err := kmeta.DecodeMetadata(meta.Properties, &md); err != nil {
		return fmt.Errorf("decode meilisearch metadata: %w", err)
	}
	client, err := commonmeilisearch.NewClient(md)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.client = client
	m.md = md
	m.closed = false
	_ = ctx
	return nil
}

// CreateIndex creates a Meilisearch index and applies searchable, filterable and sortable fields as settings.
func (m *Meilisearch) CreateIndex(ctx context.Context, req *search.CreateIndexRequest) (*search.CreateIndexResponse, error) {
	if req == nil || req.Index == "" {
		return nil, errors.New("index is required")
	}
	client, err := m.getClient()
	if err != nil {
		return nil, err
	}
	if _, err := client.CreateIndexWithContext(ctx, &meilisearchgo.IndexConfig{Uid: req.Index, PrimaryKey: commonmeilisearch.PrimaryKey}); err != nil {
		if _, getErr := client.GetIndexWithContext(ctx, req.Index); getErr != nil {
			return nil, fmt.Errorf("create meilisearch index %q: %w", req.Index, err)
		}
	}
	if len(req.Fields) > 0 {
		if _, err := client.Index(req.Index).UpdateSettingsWithContext(ctx, commonmeilisearch.SettingsFromFields(req.Fields)); err != nil {
			return nil, fmt.Errorf("update meilisearch settings for index %q: %w", req.Index, err)
		}
	}
	return &search.CreateIndexResponse{}, nil
}

// DropIndex deletes a Meilisearch index.
func (m *Meilisearch) DropIndex(ctx context.Context, req *search.DropIndexRequest) error {
	if req == nil || req.Index == "" {
		return errors.New("index is required")
	}
	client, err := m.getClient()
	if err != nil {
		return err
	}
	if _, err := client.DeleteIndexWithContext(ctx, req.Index); err != nil {
		return fmt.Errorf("delete meilisearch index %q: %w", req.Index, err)
	}
	return nil
}

// DescribeIndex returns Meilisearch index settings and document count.
func (m *Meilisearch) DescribeIndex(ctx context.Context, req *search.DescribeIndexRequest) (*search.DescribeIndexResponse, error) {
	if req == nil || req.Index == "" {
		return nil, errors.New("index is required")
	}
	client, err := m.getClient()
	if err != nil {
		return nil, err
	}
	idx, err := client.GetIndexWithContext(ctx, req.Index)
	if err != nil {
		return nil, fmt.Errorf("get meilisearch index %q: %w", req.Index, err)
	}
	stats, err := idx.GetStatsWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("get meilisearch stats for index %q: %w", req.Index, err)
	}
	settings, err := idx.GetSettingsWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("get meilisearch settings for index %q: %w", req.Index, err)
	}
	if stats.NumberOfDocuments < 0 {
		return nil, fmt.Errorf("get meilisearch stats for index %q: negative document count %d", req.Index, stats.NumberOfDocuments)
	}
	return &search.DescribeIndexResponse{
		Index:         idx.UID,
		Fields:        commonmeilisearch.FieldsFromSettings(settings),
		DocumentCount: uint64(stats.NumberOfDocuments),
		Properties:    map[string]string{"primaryKey": idx.PrimaryKey},
	}, nil
}

// ListIndexes lists Meilisearch indexes.
func (m *Meilisearch) ListIndexes(ctx context.Context, req *search.ListIndexesRequest) (*search.ListIndexesResponse, error) {
	_ = req
	client, err := m.getClient()
	if err != nil {
		return nil, err
	}
	res, err := client.ListIndexesWithContext(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("list meilisearch indexes: %w", err)
	}
	out := &search.ListIndexesResponse{Indexes: make([]string, 0, len(res.Results))}
	for _, idx := range res.Results {
		out.Indexes = append(out.Indexes, idx.UID)
	}
	return out, nil
}

// IndexDocuments adds or replaces documents in a Meilisearch index.
func (m *Meilisearch) IndexDocuments(ctx context.Context, req *search.IndexDocumentsRequest) (*search.IndexDocumentsResponse, error) {
	if req == nil || req.Index == "" {
		return nil, errors.New("index is required")
	}
	client, err := m.getClient()
	if err != nil {
		return nil, err
	}
	docs := make([]map[string]any, len(req.Documents))
	results := make([]search.OperationResult, len(req.Documents))
	for i, doc := range req.Documents {
		content := commonmeilisearch.CloneMap(doc.Content)
		content[commonmeilisearch.PrimaryKey] = doc.ID
		for k, v := range doc.Metadata {
			content[k] = v
		}
		docs[i] = content
		results[i] = search.OperationResult{ID: doc.ID, Success: true}
	}
	task, err := client.Index(req.Index).AddDocumentsWithContext(ctx, docs, &meilisearchgo.DocumentOptions{PrimaryKey: meilisearchgo.StringPtr(commonmeilisearch.PrimaryKey), TaskCustomMetadata: req.IdempotencyKey})
	if err != nil {
		return nil, fmt.Errorf("add meilisearch documents to index %q: %w", req.Index, err)
	}
	ack := commonmeilisearch.NormalizeAck(req.Ack)
	if ack == search.IndexAckDurable {
		if err := commonmeilisearch.WaitForTask(ctx, client, task.TaskUID); err != nil {
			commonmeilisearch.MarkFailed(results, err)
		}
	}
	return &search.IndexDocumentsResponse{Results: results, Ack: ack}, nil
}

// DeleteDocuments deletes documents by ID from a Meilisearch index.
func (m *Meilisearch) DeleteDocuments(ctx context.Context, req *search.DeleteDocumentsRequest) (*search.DeleteDocumentsResponse, error) {
	if req == nil || req.Index == "" {
		return nil, errors.New("index is required")
	}
	client, err := m.getClient()
	if err != nil {
		return nil, err
	}
	results := make([]search.OperationResult, len(req.IDs))
	for i, id := range req.IDs {
		results[i] = search.OperationResult{ID: id, Success: true}
	}
	task, err := client.Index(req.Index).DeleteDocumentsWithContext(ctx, req.IDs, nil)
	if err != nil {
		return nil, fmt.Errorf("delete meilisearch documents from index %q: %w", req.Index, err)
	}
	ack := commonmeilisearch.NormalizeAck(req.Ack)
	if ack == search.IndexAckDurable {
		if err := commonmeilisearch.WaitForTask(ctx, client, task.TaskUID); err != nil {
			commonmeilisearch.MarkFailed(results, err)
		}
	}
	return &search.DeleteDocumentsResponse{Results: results, Ack: ack}, nil
}

// Search searches a Meilisearch index.
func (m *Meilisearch) Search(ctx context.Context, req *search.SearchRequest) (*search.SearchResponse, error) {
	if req == nil || req.Index == "" {
		return nil, errors.New("index is required")
	}
	client, err := m.getClient()
	if err != nil {
		return nil, err
	}
	msReq, err := buildSearchRequest(req)
	if err != nil {
		return nil, err
	}
	res, err := client.Index(req.Index).SearchWithContext(ctx, req.Text, msReq)
	if err != nil {
		return nil, fmt.Errorf("search meilisearch index %q: %w", req.Index, err)
	}
	return searchResponseFromMeilisearch(res, req.IncludeContent)
}

// GetComponentMetadata returns the metadata of the component.
func (m *Meilisearch) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(commonmeilisearch.MeilisearchMetadata{}), &metadataInfo, metadata.SearchType)
	return metadataInfo
}

// Close closes the component.
func (m *Meilisearch) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *Meilisearch) getClient() (meilisearchgo.ServiceManager, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, errors.New("meilisearch search component is closed")
	}
	if m.client == nil {
		return nil, errors.New("meilisearch search component is not initialized")
	}
	return m.client, nil
}

func buildSearchRequest(req *search.SearchRequest) (*meilisearchgo.SearchRequest, error) {
	msReq := &meilisearchgo.SearchRequest{Query: req.Text, ShowRankingScore: true}
	if req.TopK > 0 {
		msReq.Limit = int64(req.TopK)
	} else {
		msReq.Limit = defaultSearchLim
	}
	if len(req.ReturnFields) > 0 {
		msReq.AttributesToRetrieve = req.ReturnFields
	} else if !req.IncludeContent {
		msReq.AttributesToRetrieve = []string{commonmeilisearch.PrimaryKey}
	}
	if len(req.SearchFields) > 0 {
		msReq.AttributesToSearchOn = req.SearchFields
	}
	if len(req.HighlightFields) > 0 {
		msReq.AttributesToHighlight = req.HighlightFields
	}
	if len(req.Sort) > 0 {
		msReq.Sort = sortClauses(req.Sort)
	}
	if len(req.Filter) > 0 {
		filter, err := commonmeilisearch.TranslateFilter(req.Filter)
		if err != nil {
			return nil, err
		}
		msReq.Filter = filter
	}
	if req.ContinuationToken != "" {
		offset, err := commonmeilisearch.DecodeContinuationToken(req.ContinuationToken)
		if err != nil {
			return nil, err
		}
		msReq.Offset = offset
	}
	if len(req.Native) > 0 {
		data, err := json.Marshal(req.Native)
		if err != nil {
			return nil, fmt.Errorf("marshal native meilisearch options: %w", err)
		}
		if err := json.Unmarshal(data, msReq); err != nil {
			return nil, fmt.Errorf("decode native meilisearch options: %w", err)
		}
	}
	return msReq, nil
}

func sortClauses(clauses []search.SortClause) []string {
	out := make([]string, 0, len(clauses))
	for _, clause := range clauses {
		order := "asc"
		if clause.Order == search.SortOrderDesc {
			order = "desc"
		}
		out = append(out, clause.Field+":"+order)
	}
	return out
}

func searchResponseFromMeilisearch(res *meilisearchgo.SearchResponse, includeContent bool) (*search.SearchResponse, error) {
	out := &search.SearchResponse{Hits: make([]search.Hit, 0, len(res.Hits))}
	if res.TotalHits > 0 {
		out.TotalHits = uint64(res.TotalHits)
	} else if res.EstimatedTotalHits > 0 {
		out.TotalHits = uint64(res.EstimatedTotalHits)
	}
	if res.Offset+int64(len(res.Hits)) < outTotalInt64(out.TotalHits) {
		out.ContinuationToken = commonmeilisearch.EncodeContinuationToken(res.Offset + int64(len(res.Hits)))
	}
	for _, hit := range res.Hits {
		mapped, err := hitFromMeilisearch(hit, includeContent)
		if err != nil {
			return nil, err
		}
		out.Hits = append(out.Hits, mapped)
	}
	return out, nil
}

func outTotalInt64(total uint64) int64 {
	if total > uint64(^uint(0)>>1) {
		return int64(^uint(0) >> 1)
	}
	return int64(total)
}

func hitFromMeilisearch(hit meilisearchgo.Hit, includeContent bool) (search.Hit, error) {
	content := map[string]any{}
	formatted := map[string]any{}
	out := search.Hit{Highlights: map[string]search.FieldHighlight{}}
	for key, raw := range hit {
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return out, fmt.Errorf("decode meilisearch hit field %q: %w", key, err)
		}
		switch key {
		case commonmeilisearch.PrimaryKey:
			out.Document.ID = fmt.Sprint(value)
		case "_rankingScore":
			out.Score = commonmeilisearch.NumberAsFloat(value)
		case "_formatted":
			if fm, ok := value.(map[string]any); ok {
				formatted = fm
			}
		default:
			if includeContent && !strings.HasPrefix(key, "_") {
				content[key] = value
			}
		}
	}
	if includeContent {
		out.Document.Content = content
	}
	for field, value := range formatted {
		out.Highlights[field] = search.FieldHighlight{Snippets: []search.Snippet{{Text: fmt.Sprint(value)}}}
	}
	if len(out.Highlights) == 0 {
		out.Highlights = nil
	}
	return out, nil
}
