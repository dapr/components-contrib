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
	"math"
	"reflect"
	"strings"
	"sync"

	meilisearchgo "github.com/meilisearch/meilisearch-go"

	commonmeilisearch "github.com/dapr/components-contrib/common/component/meilisearch"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/components-contrib/vector"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"
)

const (
	defaultEmbedder = "default"
	namespaceField  = "namespace"
)

// Meilisearch implements the Dapr Vector building block with Meilisearch.
type Meilisearch struct {
	logger logger.Logger
	client meilisearchgo.ServiceManager
	md     commonmeilisearch.MeilisearchMetadata
	mu     sync.RWMutex
	closed bool
}

// NewMeilisearch creates a Meilisearch vector component.
func NewMeilisearch(logger logger.Logger) vector.Vector {
	return &Meilisearch{logger: logger}
}

// Init initializes the Meilisearch vector component.
func (m *Meilisearch) Init(ctx context.Context, meta vector.Metadata) error {
	md := commonmeilisearch.MeilisearchMetadata{}
	if err := kmeta.DecodeMetadata(meta.Properties, &md); err != nil {
		return fmt.Errorf("decode meilisearch metadata: %w", err)
	}
	client, err := commonmeilisearch.NewClient(md)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.client = client
	m.md = md
	m.closed = false
	m.mu.Unlock()
	_ = ctx
	return nil
}

// CreateCollection creates a Meilisearch index configured for user-provided vectors.
func (m *Meilisearch) CreateCollection(ctx context.Context, req *vector.CreateCollectionRequest) (*vector.CreateCollectionResponse, error) {
	if req == nil || req.Collection == "" {
		return nil, errors.New("collection is required")
	}
	if err := m.ensureReady(ctx); err != nil {
		return nil, err
	}
	if err := validateMetric(req.Metric); err != nil {
		return nil, err
	}
	task, err := m.client.CreateIndexWithContext(ctx, &meilisearchgo.IndexConfig{Uid: req.Collection, PrimaryKey: commonmeilisearch.PrimaryKey})
	if err != nil {
		if _, getErr := m.client.GetIndexWithContext(ctx, req.Collection); getErr != nil {
			return nil, fmt.Errorf("create meilisearch collection %q: %w", req.Collection, err)
		}
	} else if waitErr := commonmeilisearch.WaitForTask(ctx, m.client, task.TaskUID); waitErr != nil {
		return nil, waitErr
	}
	settings := commonmeilisearch.SettingsFromFields(req.MetadataSchema)
	settings.Embedders = map[string]meilisearchgo.Embedder{
		defaultEmbedder: {Source: meilisearchgo.UserProvidedEmbedderSource, Dimensions: int(req.Dimension)},
	}
	task, err = m.client.Index(req.Collection).UpdateSettingsWithContext(ctx, settings)
	if err != nil {
		return nil, fmt.Errorf("update meilisearch embedders for collection %q: %w", req.Collection, err)
	}
	if err := commonmeilisearch.WaitForTask(ctx, m.client, task.TaskUID); err != nil {
		return nil, fmt.Errorf("wait for meilisearch settings update for collection %q: %w", req.Collection, err)
	}
	return &vector.CreateCollectionResponse{}, nil
}

// DropCollection deletes a Meilisearch collection.
func (m *Meilisearch) DropCollection(ctx context.Context, req *vector.DropCollectionRequest) error {
	if req == nil || req.Collection == "" {
		return errors.New("collection is required")
	}
	if err := m.ensureReady(ctx); err != nil {
		return err
	}
	if _, err := m.client.DeleteIndexWithContext(ctx, req.Collection); err != nil {
		return fmt.Errorf("delete meilisearch collection %q: %w", req.Collection, err)
	}
	return nil
}

// DescribeCollection returns Meilisearch collection settings and vector count.
func (m *Meilisearch) DescribeCollection(ctx context.Context, req *vector.DescribeCollectionRequest) (*vector.DescribeCollectionResponse, error) {
	if req == nil || req.Collection == "" {
		return nil, errors.New("collection is required")
	}
	if err := m.ensureReady(ctx); err != nil {
		return nil, err
	}
	idx, err := m.client.GetIndexWithContext(ctx, req.Collection)
	if err != nil {
		return nil, fmt.Errorf("get meilisearch collection %q: %w", req.Collection, err)
	}
	stats, err := idx.GetStatsWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("get meilisearch stats for collection %q: %w", req.Collection, err)
	}
	settings, err := idx.GetSettingsWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("get meilisearch settings for collection %q: %w", req.Collection, err)
	}
	dim := uint32(0)
	if embedder, ok := settings.Embedders[defaultEmbedder]; ok && embedder.Dimensions > 0 {
		if embedder.Dimensions > math.MaxUint32 {
			return nil, fmt.Errorf("meilisearch collection %q embedder dimension %d exceeds maximum uint32", req.Collection, embedder.Dimensions)
		}
		dim = uint32(embedder.Dimensions) //nolint:gosec // G115: embedder.Dimensions is checked against math.MaxUint32 above.
	}
	if stats.NumberOfDocuments < 0 {
		return nil, fmt.Errorf("meilisearch collection %q document count %d is negative", req.Collection, stats.NumberOfDocuments)
	}
	return &vector.DescribeCollectionResponse{
		Collection:     req.Collection,
		Dimension:      dim,
		Metric:         vector.DistanceMetricCosine,
		MetadataSchema: commonmeilisearch.FieldsFromSettings(settings),
		VectorCount:    uint64(stats.NumberOfDocuments),
		Properties:     map[string]string{"primaryKey": idx.PrimaryKey},
	}, nil
}

// ListCollections lists Meilisearch collections.
func (m *Meilisearch) ListCollections(ctx context.Context, req *vector.ListCollectionsRequest) (*vector.ListCollectionsResponse, error) {
	_ = req
	if err := m.ensureReady(ctx); err != nil {
		return nil, err
	}
	res, err := m.client.ListIndexesWithContext(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("list meilisearch collections: %w", err)
	}
	out := &vector.ListCollectionsResponse{Collections: make([]string, 0, len(res.Results))}
	for _, idx := range res.Results {
		out.Collections = append(out.Collections, idx.UID)
	}
	return out, nil
}

// Upsert adds or replaces vectors in a Meilisearch collection.
func (m *Meilisearch) Upsert(ctx context.Context, req *vector.UpsertRequest) (*vector.UpsertResponse, error) {
	if req == nil || req.Collection == "" {
		return nil, errors.New("collection is required")
	}
	if err := m.ensureReady(ctx); err != nil {
		return nil, err
	}
	docs := make([]map[string]any, len(req.Vectors))
	results := make([]search.OperationResult, len(req.Vectors))
	for i, vec := range req.Vectors {
		doc := commonmeilisearch.CloneMap(vec.Metadata)
		doc[commonmeilisearch.PrimaryKey] = vec.ID
		if vec.Namespace != "" {
			doc[namespaceField] = vec.Namespace
		}
		doc["_vectors"] = map[string][]float32{defaultEmbedder: vec.Values}
		docs[i] = doc
		results[i] = search.OperationResult{ID: vec.ID, Success: true}
	}
	task, err := m.client.Index(req.Collection).AddDocumentsWithContext(ctx, docs, &meilisearchgo.DocumentOptions{PrimaryKey: meilisearchgo.StringPtr(commonmeilisearch.PrimaryKey), TaskCustomMetadata: req.IdempotencyKey})
	if err != nil {
		return nil, fmt.Errorf("upsert meilisearch vectors into collection %q: %w", req.Collection, err)
	}
	ack := commonmeilisearch.NormalizeAck(req.Ack)
	if ack == search.IndexAckDurable {
		if err := commonmeilisearch.WaitForTask(ctx, m.client, task.TaskUID); err != nil {
			commonmeilisearch.MarkFailed(results, err)
		}
	}
	return &vector.UpsertResponse{Results: results, Ack: ack}, nil
}

// Get fetches vectors by ID.
func (m *Meilisearch) Get(ctx context.Context, req *vector.GetRequest) (*vector.GetResponse, error) {
	if req == nil || req.Collection == "" {
		return nil, errors.New("collection is required")
	}
	if err := m.ensureReady(ctx); err != nil {
		return nil, err
	}
	if len(req.IDs) > 0 {
		return m.getDocumentsByID(ctx, req)
	}
	query := &meilisearchgo.DocumentsQuery{RetrieveVectors: req.IncludeValues}
	if req.Namespace != "" {
		query.Filter = fmt.Sprintf("%s = %s", namespaceField, commonmeilisearch.FormatFilterValue(req.Namespace))
	}
	var res meilisearchgo.DocumentsResult
	if err := m.client.Index(req.Collection).GetDocumentsWithContext(ctx, query, &res); err != nil {
		return nil, fmt.Errorf("get meilisearch vectors from collection %q: %w", req.Collection, err)
	}
	out := &vector.GetResponse{Vectors: make([]vector.Vec, 0, len(res.Results))}
	for _, hit := range res.Results {
		vec, err := vecFromHit(hit, req.IncludeValues, req.IncludeMetadata)
		if err != nil {
			return nil, err
		}
		out.Vectors = append(out.Vectors, vec)
	}
	return out, nil
}

// Delete deletes vectors by IDs or filter.
func (m *Meilisearch) Delete(ctx context.Context, req *vector.DeleteRequest) (*vector.DeleteResponse, error) {
	if req == nil || req.Collection == "" {
		return nil, errors.New("collection is required")
	}
	if err := m.ensureReady(ctx); err != nil {
		return nil, err
	}
	ack := commonmeilisearch.NormalizeAck(req.Ack)
	var task *meilisearchgo.TaskInfo
	var results []search.OperationResult
	var err error
	hasIDs := len(req.Selector.IDs) > 0
	hasFilter := len(req.Selector.Filter) > 0
	if hasIDs && hasFilter {
		return nil, errors.New("delete selector ids and filter are mutually exclusive")
	}
	if hasIDs {
		ids := req.Selector.IDs
		if req.Namespace != "" {
			got, getErr := m.getDocumentsByID(ctx, &vector.GetRequest{Collection: req.Collection, Namespace: req.Namespace, IDs: req.Selector.IDs})
			if getErr != nil {
				return nil, getErr
			}
			ids = make([]string, 0, len(got.Vectors))
			for _, vec := range got.Vectors {
				ids = append(ids, vec.ID)
			}
		}
		results = make([]search.OperationResult, len(ids))
		for i, id := range ids {
			results[i] = search.OperationResult{ID: id, Success: true}
		}
		if len(ids) == 0 {
			return &vector.DeleteResponse{Results: results, Ack: ack}, nil
		}
		task, err = m.client.Index(req.Collection).DeleteDocumentsWithContext(ctx, ids, nil)
	} else if hasFilter {
		filter, ferr := commonmeilisearch.TranslateFilter(req.Selector.Filter)
		if ferr != nil {
			return nil, ferr
		}
		if req.Namespace != "" {
			filter = "(" + filter + ") AND " + namespaceField + " = " + commonmeilisearch.FormatFilterValue(req.Namespace)
		}
		task, err = m.client.Index(req.Collection).DeleteDocumentsByFilterWithContext(ctx, filter, nil)
	} else {
		return nil, errors.New("delete selector requires ids or filter")
	}
	if err != nil {
		return nil, fmt.Errorf("delete meilisearch vectors from collection %q: %w", req.Collection, err)
	}
	if ack == search.IndexAckDurable {
		if err := commonmeilisearch.WaitForTask(ctx, m.client, task.TaskUID); err != nil {
			commonmeilisearch.MarkFailed(results, err)
		}
	}
	return &vector.DeleteResponse{Results: results, DeletedCount: uint64(len(results)), Ack: ack}, nil
}

// Query performs nearest-neighbour vector search.
func (m *Meilisearch) Query(ctx context.Context, req *vector.QueryRequest) (*vector.QueryResponse, error) {
	if req == nil || req.Collection == "" {
		return nil, errors.New("collection is required")
	}
	if err := m.ensureReady(ctx); err != nil {
		return nil, err
	}
	if (len(req.QueryVector) > 0) == (req.QueryByID != "") {
		return nil, errors.New("exactly one of QueryVector or QueryByID must be set")
	}
	limit := int64(req.TopK)
	if limit == 0 {
		limit = 10
	}
	msReq := &meilisearchgo.SearchRequest{
		Limit:            limit,
		Vector:           req.QueryVector,
		Hybrid:           &meilisearchgo.SearchRequestHybrid{Embedder: defaultEmbedder, SemanticRatio: 1},
		ShowRankingScore: true,
		RetrieveVectors:  req.IncludeValues,
	}
	if req.ScoreThreshold != nil {
		msReq.RankingScoreThreshold = *req.ScoreThreshold
	}
	if len(req.Filter) > 0 {
		filter, err := commonmeilisearch.TranslateFilter(req.Filter)
		if err != nil {
			return nil, err
		}
		msReq.Filter = filter
	}
	if req.Namespace != "" {
		nsFilter := namespaceField + " = " + commonmeilisearch.FormatFilterValue(req.Namespace)
		if msReq.Filter == nil {
			msReq.Filter = nsFilter
		} else {
			msReq.Filter = "(" + fmt.Sprint(msReq.Filter) + ") AND " + nsFilter
		}
	}
	if req.ContinuationToken != "" {
		offset, err := commonmeilisearch.DecodeContinuationToken(req.ContinuationToken)
		if err != nil {
			return nil, err
		}
		msReq.Offset = offset
	}
	if req.QueryByID != "" {
		return m.queryByID(ctx, req, msReq)
	}
	res, err := m.client.Index(req.Collection).SearchWithContext(ctx, "", msReq)
	if err != nil {
		return nil, fmt.Errorf("query meilisearch collection %q: %w", req.Collection, err)
	}
	return vectorResponseFromMeilisearch(res, req.IncludeValues, req.IncludeMetadata)
}

// BatchQuery performs parallel nearest-neighbour queries.
func (m *Meilisearch) BatchQuery(ctx context.Context, req *vector.BatchQueryRequest) (*vector.BatchQueryResponse, error) {
	if req == nil || req.Collection == "" {
		return nil, errors.New("collection is required")
	}
	if err := m.ensureReady(ctx); err != nil {
		return nil, err
	}
	if len(req.Queries) > math.MaxUint32 {
		return nil, fmt.Errorf("query count %d exceeds maximum uint32", len(req.Queries))
	}
	out := &vector.BatchQueryResponse{Results: make([]vector.BatchQueryResult, len(req.Queries))}
	var wg sync.WaitGroup
	for i := range req.Queries {
		q := req.Queries[i]
		q.Collection = req.Collection
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := m.Query(ctx, &q)
			out.Results[i].QueryIndex = uint32(i) //nolint:gosec // G115: len(req.Queries) is checked against math.MaxUint32 above.
			if err != nil {
				out.Results[i].ErrorCode = "query_failed"
				out.Results[i].ErrorMessage = err.Error()
				return
			}
			out.Results[i].Hits = res.Hits
		}()
	}
	wg.Wait()
	return out, nil
}

// GetComponentMetadata returns the metadata of the component.
func (m *Meilisearch) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(commonmeilisearch.MeilisearchMetadata{}), &metadataInfo, metadata.VectorType)
	return metadataInfo
}

// Close closes the component.
func (m *Meilisearch) Close() error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	return nil
}

func (m *Meilisearch) ensureReady(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return errors.New("meilisearch vector component is closed")
	}
	if m.client == nil {
		return errors.New("meilisearch vector component is not initialized")
	}
	return nil
}

func (m *Meilisearch) getDocumentsByID(ctx context.Context, req *vector.GetRequest) (*vector.GetResponse, error) {
	idx := m.client.Index(req.Collection)
	out := &vector.GetResponse{Vectors: make([]vector.Vec, 0, len(req.IDs))}
	for _, id := range req.IDs {
		var hit meilisearchgo.Hit
		if err := idx.GetDocumentWithContext(ctx, id, &meilisearchgo.DocumentQuery{RetrieveVectors: req.IncludeValues}, &hit); err != nil {
			if isNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("get meilisearch vector %q from collection %q: %w", id, req.Collection, err)
		}
		vec, err := vecFromHit(hit, req.IncludeValues, req.IncludeMetadata)
		if err != nil {
			return nil, err
		}
		if req.Namespace != "" && vec.Namespace != req.Namespace {
			continue
		}
		out.Vectors = append(out.Vectors, vec)
	}
	return out, nil
}

func (m *Meilisearch) queryByID(ctx context.Context, req *vector.QueryRequest, msReq *meilisearchgo.SearchRequest) (*vector.QueryResponse, error) {
	param := &meilisearchgo.SimilarDocumentQuery{Id: req.QueryByID, Embedder: defaultEmbedder, Limit: msReq.Limit, RetrieveVectors: req.IncludeValues, ShowRankingScore: true}
	if msReq.Filter != nil {
		param.Filter = fmt.Sprint(msReq.Filter)
	}
	var res meilisearchgo.SimilarDocumentResult
	if err := m.client.Index(req.Collection).SearchSimilarDocumentsWithContext(ctx, param, &res); err != nil {
		return nil, fmt.Errorf("query similar meilisearch documents in collection %q: %w", req.Collection, err)
	}
	searchRes := &meilisearchgo.SearchResponse{Hits: res.Hits, EstimatedTotalHits: res.EstimatedTotalHits, Limit: res.Limit, Offset: res.Offset}
	return vectorResponseFromMeilisearch(searchRes, req.IncludeValues, req.IncludeMetadata)
}

func isNotFound(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not found") || strings.Contains(msg, "not_found") || strings.Contains(msg, "404")
}

func validateMetric(metric vector.DistanceMetric) error {
	switch metric {
	case vector.DistanceMetricUnspecified, vector.DistanceMetricCosine:
		return nil
	default:
		return fmt.Errorf("unsupported meilisearch vector distance metric %v: only cosine/userProvided vectors are supported", metric)
	}
}

func vectorResponseFromMeilisearch(res *meilisearchgo.SearchResponse, includeValues, includeMetadata bool) (*vector.QueryResponse, error) {
	out := &vector.QueryResponse{Hits: make([]vector.Hit, 0, len(res.Hits))}
	for _, hit := range res.Hits {
		vec, score, err := vecAndScoreFromHit(hit, includeValues, includeMetadata)
		if err != nil {
			return nil, err
		}
		out.Hits = append(out.Hits, vector.Hit{Vector: vec, Distance: 1 - score})
	}
	total := res.TotalHits
	if total == 0 {
		total = res.EstimatedTotalHits
	}
	if res.Offset+int64(len(res.Hits)) < total {
		out.ContinuationToken = commonmeilisearch.EncodeContinuationToken(res.Offset + int64(len(res.Hits)))
	}
	return out, nil
}

func vecFromHit(hit meilisearchgo.Hit, includeValues, includeMetadata bool) (vector.Vec, error) {
	vec, _, err := vecAndScoreFromHit(hit, includeValues, includeMetadata)
	return vec, err
}

func vecAndScoreFromHit(hit meilisearchgo.Hit, includeValues, includeMetadata bool) (vector.Vec, float64, error) {
	vec := vector.Vec{}
	metadata := map[string]any{}
	score := 0.0
	for key, raw := range hit {
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return vec, 0, fmt.Errorf("decode meilisearch hit field %q: %w", key, err)
		}
		switch key {
		case commonmeilisearch.PrimaryKey:
			vec.ID = fmt.Sprint(value)
		case namespaceField:
			vec.Namespace = fmt.Sprint(value)
		case "_rankingScore":
			score = commonmeilisearch.NumberAsFloat(value)
		case "_vectors":
			if includeValues {
				vec.Values = parseDefaultVector(value)
			}
		default:
			if includeMetadata && !strings.HasPrefix(key, "_") {
				metadata[key] = value
			}
		}
	}
	if includeMetadata {
		vec.Metadata = metadata
	}
	return vec, score, nil
}

func parseDefaultVector(value any) []float32 {
	vectors, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	arr, ok := vectors[defaultEmbedder].([]any)
	if !ok {
		embedder, ok := vectors[defaultEmbedder].(map[string]any)
		if !ok {
			return nil
		}
		embeddings, ok := embedder["embeddings"].([]any)
		if !ok || len(embeddings) == 0 {
			return nil
		}
		arr, ok = embeddings[0].([]any)
		if !ok {
			return nil
		}
	}
	out := make([]float32, 0, len(arr))
	for _, item := range arr {
		out = append(out, float32(commonmeilisearch.NumberAsFloat(item)))
	}
	return out
}
