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
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"

	meilisearchgo "github.com/meilisearch/meilisearch-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonmeilisearch "github.com/dapr/components-contrib/common/component/meilisearch"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/components-contrib/vector"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"
)

const (
	// DefaultEmbedder is the Meilisearch embedder holding a record's dense
	// vector.
	DefaultEmbedder = "default"

	// defaultTopK is the number of matches returned when a query does not set
	// TopK.
	defaultTopK = int64(10)

	// supportsQueuedAck reports that Meilisearch offers a native durable
	// queued acknowledgement through its asynchronous task API.
	supportsQueuedAck = true

	// Collection settings accepted in CreateCollection metadata.
	mdFilterableAttributes = "filterableAttributes"
	mdSortableAttributes   = "sortableAttributes"

	// metadataFilterPrefix is prepended to every portable filter path: record
	// metadata is stored under the MetadataField object, so `a.b` addresses
	// the Meilisearch attribute `daprMetadata.a.b`.
	metadataFilterPrefix = commonmeilisearch.MetadataField + "."
)

// Meilisearch implements the Dapr Vector building block with Meilisearch.
//
// Meilisearch stores user-provided embeddings in a document's `_vectors`
// field, so a vector collection is a Meilisearch index with one `userProvided`
// embedder. Meilisearch only supports cosine similarity for those embeddings.
type Meilisearch struct {
	logger logger.Logger

	mu         sync.RWMutex
	client     meilisearchgo.ServiceManager
	dispatcher *commonmeilisearch.TaskDispatcher
	md         commonmeilisearch.MeilisearchMetadata
	closed     bool
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
	defer m.mu.Unlock()
	m.client = client
	m.md = md
	m.dispatcher = commonmeilisearch.NewTaskDispatcher(md, client, m.logger)
	m.closed = false
	return nil
}

// CreateCollection creates a Meilisearch index configured for user-provided
// embeddings of the requested dimensions. Record metadata is filterable by
// default; additional Meilisearch settings travel in the request metadata. An
// existing collection is ALREADY_EXISTS.
func (m *Meilisearch) CreateCollection(ctx context.Context, req *vector.CreateCollectionRequest) error {
	if req == nil || req.Collection == "" {
		return status.Error(codes.InvalidArgument, "collection is required")
	}
	if req.Dimensions == 0 {
		return status.Error(codes.InvalidArgument, "dimensions must be greater than zero")
	}
	if err := validateMetric(req.Metric); err != nil {
		return err
	}
	client, _, err := m.ready()
	if err != nil {
		return err
	}

	task, err := client.CreateIndexWithContext(ctx, &meilisearchgo.IndexConfig{Uid: req.Collection, PrimaryKey: commonmeilisearch.PrimaryKey})
	if err != nil {
		return commonmeilisearch.StatusError(err, fmt.Sprintf("create meilisearch collection %q", req.Collection))
	}
	// Meilisearch reports an existing index as a failed creation task with
	// the `index_already_exists` code, which maps to ALREADY_EXISTS.
	err = commonmeilisearch.WaitForTask(ctx, client, task.TaskUID, fmt.Sprintf("create meilisearch collection %q", req.Collection))
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return status.Errorf(codes.AlreadyExists, "meilisearch collection %q already exists", req.Collection)
		}
		return err
	}

	settings := &meilisearchgo.Settings{
		Embedders: map[string]meilisearchgo.Embedder{
			DefaultEmbedder: {Source: meilisearchgo.UserProvidedEmbedderSource, Dimensions: int(req.Dimensions)},
		},
		FilterableAttributes: filterableAttributes(req.Metadata),
	}
	if sortable := commonmeilisearch.SplitList(req.Metadata[mdSortableAttributes]); len(sortable) > 0 {
		settings.SortableAttributes = sortable
	}
	settingsTask, err := client.Index(req.Collection).UpdateSettingsWithContext(ctx, settings)
	if err != nil {
		return commonmeilisearch.StatusError(err, fmt.Sprintf("configure meilisearch collection %q", req.Collection))
	}
	return commonmeilisearch.WaitForTask(ctx, client, settingsTask.TaskUID, fmt.Sprintf("configure meilisearch collection %q", req.Collection))
}

// GetCollection returns the record count, dimensions, metric and settings of
// a collection.
func (m *Meilisearch) GetCollection(ctx context.Context, req *vector.GetCollectionRequest) (*vector.GetCollectionResponse, error) {
	if req == nil || req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection is required")
	}
	client, _, err := m.ready()
	if err != nil {
		return nil, err
	}

	idx, err := client.GetIndexWithContext(ctx, req.Collection)
	if err != nil {
		return nil, commonmeilisearch.StatusError(err, fmt.Sprintf("get meilisearch collection %q", req.Collection))
	}
	stats, err := idx.GetStatsWithContext(ctx)
	if err != nil {
		return nil, commonmeilisearch.StatusError(err, fmt.Sprintf("get stats of meilisearch collection %q", req.Collection))
	}
	settings, err := idx.GetSettingsWithContext(ctx)
	if err != nil {
		return nil, commonmeilisearch.StatusError(err, fmt.Sprintf("get settings of meilisearch collection %q", req.Collection))
	}
	if stats.NumberOfDocuments < 0 {
		return nil, status.Errorf(codes.Internal, "meilisearch collection %q reported a negative record count", req.Collection)
	}

	var dimensions uint32
	if embedder, ok := settings.Embedders[DefaultEmbedder]; ok && embedder.Dimensions > 0 {
		if uint64(embedder.Dimensions) > math.MaxUint32 {
			return nil, status.Errorf(codes.Internal, "meilisearch collection %q reported dimensions outside the uint32 range", req.Collection)
		}
		dimensions = uint32(embedder.Dimensions)
	}
	properties := map[string]string{"primaryKey": idx.PrimaryKey}
	if len(settings.FilterableAttributes) > 0 {
		properties[mdFilterableAttributes] = strings.Join(settings.FilterableAttributes, ",")
	}
	if len(settings.SortableAttributes) > 0 {
		properties[mdSortableAttributes] = strings.Join(settings.SortableAttributes, ",")
	}

	return &vector.GetCollectionResponse{
		Collection:  idx.UID,
		RecordCount: uint64(stats.NumberOfDocuments),
		Properties:  properties,
		Dimensions:  dimensions,
		// Cosine is the only metric Meilisearch offers for user-provided
		// embeddings, so it is always the effective metric.
		Metric: vector.DistanceMetricCosine,
	}, nil
}

// ListCollections lists the collections of the store.
func (m *Meilisearch) ListCollections(ctx context.Context, req *vector.ListCollectionsRequest) (*vector.ListCollectionsResponse, error) {
	client, _, err := m.ready()
	if err != nil {
		return nil, err
	}
	_ = req
	res, err := client.ListIndexesWithContext(ctx, nil)
	if err != nil {
		return nil, commonmeilisearch.StatusError(err, "list meilisearch collections")
	}
	out := &vector.ListCollectionsResponse{Collections: make([]string, 0, len(res.Results))}
	for _, idx := range res.Results {
		out.Collections = append(out.Collections, idx.UID)
	}
	return out, nil
}

// DeleteCollection deletes a collection.
func (m *Meilisearch) DeleteCollection(ctx context.Context, req *vector.DeleteCollectionRequest) error {
	if req == nil || req.Collection == "" {
		return status.Error(codes.InvalidArgument, "collection is required")
	}
	client, _, err := m.ready()
	if err != nil {
		return err
	}
	task, err := client.DeleteIndexWithContext(ctx, req.Collection)
	if err != nil {
		return commonmeilisearch.StatusError(err, fmt.Sprintf("delete meilisearch collection %q", req.Collection))
	}
	return commonmeilisearch.WaitForTask(ctx, client, task.TaskUID, fmt.Sprintf("delete meilisearch collection %q", req.Collection))
}

// Upsert is a keyed upsert of dense vector records into a collection.
func (m *Meilisearch) Upsert(ctx context.Context, req *vector.UpsertRequest) (*vector.UpsertResponse, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, commonmeilisearch.ContextStatusError(ctx)
	}
	if req == nil || req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection is required")
	}
	client, dispatcher, err := m.ready()
	if err != nil {
		return nil, err
	}

	ids := make([]string, len(req.Records))
	for i, record := range req.Records {
		ids[i] = record.ID
	}
	if err = search.ValidateWriteIDs(ids); err != nil {
		return nil, err
	}
	if err = search.ValidateIndexingOptions(ctx, req.Options, supportsQueuedAck); err != nil {
		return nil, err
	}

	// Item failures identified before the task is enqueued. A Meilisearch task
	// is atomic, so these are the only item failures the component can report.
	failed := make([]search.FailedItem, 0)
	docs := make([]map[string]any, 0, len(req.Records))
	for _, record := range req.Records {
		if len(record.Values) == 0 {
			failed = append(failed, commonmeilisearch.NewFailedItem(record.ID, codes.InvalidArgument, "the record has no vector values"))
			continue
		}
		doc := map[string]any{
			commonmeilisearch.PrimaryKey:   record.ID,
			commonmeilisearch.VectorsField: map[string][]float32{DefaultEmbedder: record.Values},
		}
		if payload := commonmeilisearch.EncodePayload(record.Payload); payload != nil {
			doc[commonmeilisearch.PayloadField] = payload
		}
		if meta := commonmeilisearch.EncodeRecordMetadata(record.Metadata); meta != nil {
			doc[commonmeilisearch.MetadataField] = meta
		}
		docs = append(docs, doc)
	}
	if len(docs) == 0 {
		return &vector.UpsertResponse{FailedItems: failed, Ack: search.IndexAckCompleted}, nil
	}

	ack, err := commonmeilisearch.EnqueueWrite(ctx, dispatcher, req.Options, fmt.Sprintf("upsert vectors into meilisearch collection %q", req.Collection),
		func(ctx context.Context) (*meilisearchgo.TaskInfo, error) {
			return client.Index(req.Collection).AddDocumentsWithContext(ctx, docs,
				&meilisearchgo.DocumentOptions{PrimaryKey: meilisearchgo.StringPtr(commonmeilisearch.PrimaryKey)})
		})
	if err != nil {
		return nil, err
	}
	return &vector.UpsertResponse{FailedItems: failed, Ack: ack}, nil
}

// Get fetches records by ID. Found records are returned in request order;
// records that are not found are omitted.
func (m *Meilisearch) Get(ctx context.Context, req *vector.GetRequest) (*vector.GetResponse, error) {
	if req == nil || req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection is required")
	}
	client, _, err := m.ready()
	if err != nil {
		return nil, err
	}
	if len(req.IDs) == 0 {
		return &vector.GetResponse{Records: []vector.Record{}}, nil
	}

	query := &meilisearchgo.DocumentsQuery{Ids: req.IDs, Limit: int64(len(req.IDs)), RetrieveVectors: req.IncludeValues}
	var res meilisearchgo.DocumentsResult
	if err := client.Index(req.Collection).GetDocumentsWithContext(ctx, query, &res); err != nil {
		return nil, commonmeilisearch.StatusError(err, fmt.Sprintf("get vectors of meilisearch collection %q", req.Collection))
	}

	byID := make(map[string]vector.Record, len(res.Results))
	for _, hit := range res.Results {
		record, _, err := recordFromHit(hit, req.IncludeValues, true)
		if err != nil {
			return nil, err
		}
		byID[record.ID] = record
	}
	out := &vector.GetResponse{Records: make([]vector.Record, 0, len(byID))}
	for _, id := range req.IDs {
		if record, ok := byID[id]; ok {
			out.Records = append(out.Records, record)
			delete(byID, id)
		}
	}
	return out, nil
}

// Delete deletes records by ID. It is a write: the deletion task is
// acknowledged with the same mode and wait semantics as Upsert. IDs that do
// not exist are not an error.
func (m *Meilisearch) Delete(ctx context.Context, req *vector.DeleteRequest) (*vector.DeleteResponse, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, commonmeilisearch.ContextStatusError(ctx)
	}
	if req == nil || req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection is required")
	}
	client, dispatcher, err := m.ready()
	if err != nil {
		return nil, err
	}
	if err = search.ValidateIndexingOptions(ctx, req.Options, supportsQueuedAck); err != nil {
		return nil, err
	}
	if len(req.IDs) == 0 {
		return &vector.DeleteResponse{Ack: search.IndexAckCompleted}, nil
	}

	ack, err := commonmeilisearch.EnqueueWrite(ctx, dispatcher, req.Options, fmt.Sprintf("delete vectors of meilisearch collection %q", req.Collection),
		func(ctx context.Context) (*meilisearchgo.TaskInfo, error) {
			return client.Index(req.Collection).DeleteDocumentsWithContext(ctx, req.IDs, nil)
		})
	if err != nil {
		return nil, err
	}
	return &vector.DeleteResponse{Ack: ack}, nil
}

// Query runs a single nearest-neighbour query.
func (m *Meilisearch) Query(ctx context.Context, req *vector.QueryRequest) (*vector.QueryResponse, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, commonmeilisearch.ContextStatusError(ctx)
	}
	if req == nil || req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection is required")
	}
	client, _, err := m.ready()
	if err != nil {
		return nil, err
	}
	return m.query(ctx, client, req)
}

// BatchQuery evaluates each query independently against the same collection
// and returns one result per query in request order. A query that fails
// validation or is rejected by Meilisearch produces an error result; only
// request-wide failures (a closed component, a missing collection, invalid
// credentials, transport) fail the call.
func (m *Meilisearch) BatchQuery(ctx context.Context, req *vector.BatchQueryRequest) (*vector.BatchQueryResponse, error) {
	if req == nil || req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "collection is required")
	}
	client, _, err := m.ready()
	if err != nil {
		return nil, err
	}
	out := &vector.BatchQueryResponse{Results: make([]vector.BatchQueryResult, len(req.Queries))}
	if len(req.Queries) == 0 {
		return out, nil
	}

	// A missing collection or a credential problem is request-wide and is
	// detected up front rather than reported once per query.
	if _, err := client.GetIndexWithContext(ctx, req.Collection); err != nil {
		return nil, commonmeilisearch.StatusError(err, fmt.Sprintf("get meilisearch collection %q", req.Collection))
	}

	for i := range req.Queries {
		query := req.Queries[i]
		query.Collection = req.Collection
		res, err := m.query(ctx, client, &query)
		if err != nil {
			if ctx.Err() != nil {
				return nil, commonmeilisearch.ContextStatusError(ctx)
			}
			out.Results[i] = vector.BatchQueryResult{Error: err}
			continue
		}
		out.Results[i] = vector.BatchQueryResult{Response: res}
	}
	return out, nil
}

// GetComponentMetadata returns the metadata of the component.
func (m *Meilisearch) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(commonmeilisearch.MeilisearchMetadata{}), &metadataInfo, metadata.VectorType)
	return metadataInfo
}

// Close closes the component and stops its shared task-change stream.
func (m *Meilisearch) Close() error {
	m.mu.Lock()
	dispatcher := m.dispatcher
	m.closed = true
	m.mu.Unlock()
	if dispatcher != nil {
		return dispatcher.Close()
	}
	return nil
}

func (m *Meilisearch) ready() (meilisearchgo.ServiceManager, *commonmeilisearch.TaskDispatcher, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, nil, status.Error(codes.FailedPrecondition, "the meilisearch vector component is closed")
	}
	if m.client == nil {
		return nil, nil, status.Error(codes.FailedPrecondition, "the meilisearch vector component is not initialized")
	}
	return m.client, m.dispatcher, nil
}

func (m *Meilisearch) query(ctx context.Context, client meilisearchgo.ServiceManager, req *vector.QueryRequest) (*vector.QueryResponse, error) {
	if err := validateMetric(req.Metric); err != nil {
		return nil, err
	}
	hasVector := req.Vector != nil
	hasID := req.ByID != ""
	if hasVector == hasID {
		return nil, status.Error(codes.InvalidArgument, "exactly one of vector or id must be set")
	}
	if hasVector && len(req.Vector.Values) == 0 {
		return nil, status.Error(codes.InvalidArgument, "the query vector has no values")
	}

	limit := defaultTopK
	if req.TopK > 0 {
		limit = int64(req.TopK)
	}
	var filter string
	if len(req.Filter) > 0 {
		translated, err := commonmeilisearch.TranslateFilterWithPrefix(req.Filter, metadataFilterPrefix)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "translate filter: %v", err)
		}
		filter = translated
	}

	var threshold float64
	if req.ScoreThreshold != nil {
		threshold = rankingScoreThreshold(*req.ScoreThreshold)
	}

	if hasID {
		return m.querySimilar(ctx, client, req, limit, filter, threshold)
	}

	msReq := &meilisearchgo.SearchRequest{
		Limit:                 limit,
		Vector:                req.Vector.Values,
		Hybrid:                &meilisearchgo.SearchRequestHybrid{Embedder: DefaultEmbedder, SemanticRatio: 1},
		ShowRankingScore:      true,
		RetrieveVectors:       req.IncludeValues,
		RankingScoreThreshold: threshold,
	}
	if filter != "" {
		msReq.Filter = filter
	}
	res, err := client.Index(req.Collection).SearchWithContext(ctx, "", msReq)
	if err != nil {
		return nil, commonmeilisearch.StatusError(err, fmt.Sprintf("query meilisearch collection %q", req.Collection))
	}
	return queryResponseFromHits(res.Hits, req.IncludeValues, req.IncludePayload)
}

func (m *Meilisearch) querySimilar(ctx context.Context, client meilisearchgo.ServiceManager, req *vector.QueryRequest, limit int64, filter string, threshold float64) (*vector.QueryResponse, error) {
	param := &meilisearchgo.SimilarDocumentQuery{
		Id:                    req.ByID,
		Embedder:              DefaultEmbedder,
		Limit:                 limit,
		RetrieveVectors:       req.IncludeValues,
		ShowRankingScore:      true,
		Filter:                filter,
		RankingScoreThreshold: threshold,
	}
	var res meilisearchgo.SimilarDocumentResult
	if err := client.Index(req.Collection).SearchSimilarDocumentsWithContext(ctx, param, &res); err != nil {
		return nil, commonmeilisearch.StatusError(err, fmt.Sprintf("query meilisearch collection %q by id", req.Collection))
	}
	return queryResponseFromHits(res.Hits, req.IncludeValues, req.IncludePayload)
}

// validateMetric accepts the component default (unspecified) and cosine, the
// only metric Meilisearch provides for user-provided embeddings.
func validateMetric(metric vector.DistanceMetric) error {
	switch metric {
	case vector.DistanceMetricUnspecified, vector.DistanceMetricCosine:
		return nil
	default:
		return status.Error(codes.InvalidArgument, "meilisearch only supports the cosine distance metric for user-provided embeddings")
	}
}

// filterableAttributes returns the filterable attributes of a new collection.
// Record metadata is the filterable part of a record, so the MetadataField
// object is always declared filterable (Meilisearch makes the nested fields of
// a filterable object filterable), merged with any caller-provided attributes.
func filterableAttributes(md map[string]string) []string {
	out := []string{commonmeilisearch.MetadataField}
	for _, attribute := range commonmeilisearch.SplitList(md[mdFilterableAttributes]) {
		if attribute != commonmeilisearch.MetadataField {
			out = append(out, attribute)
		}
	}
	return out
}

// Meilisearch reports a normalized `_rankingScore` in [0, 1] (documented at
// https://www.meilisearch.com/docs/learn/relevancy/ranking_score) and
// `rankingScoreThreshold` uses that same normalized scale. For a pure-semantic
// query (semanticRatio = 1) the ranking score is the cosine similarity mapped
// onto [0, 1] as (1 + cos) / 2, which is how Meilisearch normalizes its
// cosine distance. The public documentation states the [0, 1] range and the
// threshold scale but does not publish the mapping itself, so the two helpers
// below isolate it: cosineFromRankingScore and rankingScoreThreshold are the
// only places that need to change if Meilisearch documents a different
// normalization.
func cosineFromRankingScore(score float64) float64 {
	return 2*score - 1
}

func rankingScoreThreshold(cosine float64) float64 {
	return math.Min(1, math.Max(0, (1+cosine)/2))
}

func queryResponseFromHits(hits meilisearchgo.Hits, includeValues, includePayload bool) (*vector.QueryResponse, error) {
	// The effective metric is always concrete in the response. Meilisearch
	// only supports cosine similarity for user-provided embeddings.
	out := &vector.QueryResponse{Matches: make([]vector.Match, 0, len(hits)), Metric: vector.DistanceMetricCosine}
	for _, hit := range hits {
		record, score, err := recordFromHit(hit, includeValues, includePayload)
		if err != nil {
			return nil, err
		}
		out.Matches = append(out.Matches, vector.Match{Record: record, Score: cosineFromRankingScore(score)})
	}
	return out, nil
}

func recordFromHit(hit meilisearchgo.Hit, includeValues, includePayload bool) (vector.Record, float64, error) {
	record := vector.Record{}
	var score float64

	for key, raw := range hit {
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return record, 0, status.Errorf(codes.Internal, "decode meilisearch hit attribute %q", key)
		}
		switch key {
		case commonmeilisearch.PrimaryKey:
			record.ID = fmt.Sprint(value)
		case commonmeilisearch.MetadataField:
			record.Metadata = commonmeilisearch.DecodeRecordMetadata(value)
		case commonmeilisearch.PayloadField:
			if !includePayload {
				continue
			}
			payload, err := commonmeilisearch.DecodePayload(value)
			if err != nil {
				return record, 0, err
			}
			record.Payload = payload
		case commonmeilisearch.RankingScoreField:
			score = commonmeilisearch.NumberAsFloat(value)
		case commonmeilisearch.VectorsField:
			if !includeValues {
				continue
			}
			record.Values = valuesFromVectors(value)
		}
	}
	return record, score, nil
}

// valuesFromVectors decodes the dense vector of the default embedder from a
// `_vectors` attribute. Meilisearch returns either the raw embedding array or
// an object carrying the embeddings and the regenerate flag, depending on the
// request.
func valuesFromVectors(value any) []float32 {
	embedders, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	raw, ok := embedders[DefaultEmbedder]
	if !ok {
		return nil
	}
	switch typed := raw.(type) {
	case []any:
		return floatSlice(typed)
	case map[string]any:
		embeddings, ok := typed["embeddings"]
		if !ok {
			return nil
		}
		nested, ok := embeddings.([]any)
		if !ok || len(nested) == 0 {
			return nil
		}
		if inner, ok := nested[0].([]any); ok {
			return floatSlice(inner)
		}
		return floatSlice(nested)
	default:
		return nil
	}
}

func floatSlice(items []any) []float32 {
	out := make([]float32, 0, len(items))
	for _, item := range items {
		out = append(out, float32(commonmeilisearch.NumberAsFloat(item)))
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
