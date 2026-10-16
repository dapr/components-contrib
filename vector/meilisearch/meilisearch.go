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
		// The task may still create the collection after the wait gave up.
		commonmeilisearch.ReconcileIndexCreation(ctx, client, task.TaskUID, req.Collection, m.logger)
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
	if err = configureCollection(ctx, client, req.Collection, settings); err != nil {
		// The index was created by this request but has no embedder. Left
		// behind, it would make every retry fail with ALREADY_EXISTS without
		// ever becoming a usable collection, so the creation is undone.
		commonmeilisearch.RollbackIndex(ctx, client, req.Collection, m.logger)
		return err
	}
	return nil
}

// configureCollection applies the settings of a newly created collection and
// waits for them to take effect.
func configureCollection(ctx context.Context, client meilisearchgo.ServiceManager, collection string, settings *meilisearchgo.Settings) error {
	msg := fmt.Sprintf("configure meilisearch collection %q", collection)
	task, err := client.Index(collection).UpdateSettingsWithContext(ctx, settings)
	if err != nil {
		return commonmeilisearch.StatusError(err, msg)
	}
	return commonmeilisearch.WaitForTask(ctx, client, task.TaskUID, msg)
}

// collectionDimensions returns the dimensions of a vector collection from the
// embedders of its index. Collections share the instance with plain indexes:
// the search component creates them, and so does Meilisearch itself on a
// document addition to a missing index. An index without a user-provided
// `default` embedder of fixed dimensions can neither store nor query the
// vectors of this component, whose records carry their own vectors, and is
// not a collection. That includes an index whose `default` embedder generates
// its vectors (OpenAI, Hugging Face, REST, Ollama).
func collectionDimensions(collection string, embedders map[string]meilisearchgo.Embedder) (int, error) {
	embedder, ok := embedders[DefaultEmbedder]
	if !ok || embedder.Source != meilisearchgo.UserProvidedEmbedderSource || embedder.Dimensions <= 0 {
		return 0, status.Errorf(codes.FailedPrecondition,
			"meilisearch index %q is not a vector collection: it has no user-provided %q embedder with fixed dimensions", collection, DefaultEmbedder)
	}
	return embedder.Dimensions, nil
}

// lookupCollection returns the dimensions of a collection, reading its
// embedders. A missing index is NOT_FOUND and an index that is not a
// collection is FAILED_PRECONDITION.
func lookupCollection(ctx context.Context, client meilisearchgo.ServiceManager, collection string) (int, error) {
	embedders, err := client.Index(collection).GetEmbeddersWithContext(ctx)
	if err != nil {
		return 0, commonmeilisearch.StatusError(err, fmt.Sprintf("get meilisearch collection %q", collection))
	}
	return collectionDimensions(collection, embedders)
}

// GetCollection returns the record count, dimensions, metric and settings of
// a collection. An index that is not configured for vectors is
// FAILED_PRECONDITION.
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

	embedderDimensions, err := collectionDimensions(req.Collection, settings.Embedders)
	if err != nil {
		return nil, err
	}
	var dimensions uint32
	if embedderDimensions > 0 {
		if uint64(embedderDimensions) > math.MaxUint32 {
			return nil, status.Errorf(codes.Internal, "meilisearch collection %q reported dimensions outside the uint32 range", req.Collection)
		}
		dimensions = uint32(embedderDimensions)
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

// ListCollections lists the collections of the store: the indexes of the
// instance that are configured for vectors.
func (m *Meilisearch) ListCollections(ctx context.Context, req *vector.ListCollectionsRequest) (*vector.ListCollectionsResponse, error) {
	client, _, err := m.ready()
	if err != nil {
		return nil, err
	}
	_ = req
	uids, err := commonmeilisearch.ListIndexUIDs(ctx, client)
	if err != nil {
		return nil, commonmeilisearch.StatusError(err, "list meilisearch collections")
	}
	// The index list does not carry the embedders, so they are read per index.
	out := &vector.ListCollectionsResponse{Collections: make([]string, 0, len(uids))}
	for _, uid := range uids {
		embedders, embeddersErr := client.Index(uid).GetEmbeddersWithContext(ctx)
		if embeddersErr != nil {
			statusErr := commonmeilisearch.StatusError(embeddersErr, fmt.Sprintf("get the embedders of meilisearch index %q", uid))
			if status.Code(statusErr) == codes.NotFound {
				// Deleted since it was listed.
				continue
			}
			return nil, statusErr
		}
		if _, notCollection := collectionDimensions(uid, embedders); notCollection == nil {
			out.Collections = append(out.Collections, uid)
		}
	}
	return out, nil
}

// DeleteCollection deletes a collection. An index that is not configured for
// vectors is FAILED_PRECONDITION: plain search indexes share the instance, and
// the vector API must not delete one it reports as not being a collection.
func (m *Meilisearch) DeleteCollection(ctx context.Context, req *vector.DeleteCollectionRequest) error {
	if req == nil || req.Collection == "" {
		return status.Error(codes.InvalidArgument, "collection is required")
	}
	client, _, err := m.ready()
	if err != nil {
		return err
	}
	if _, err = lookupCollection(ctx, client, req.Collection); err != nil {
		return err
	}
	task, err := client.DeleteIndexWithContext(ctx, req.Collection)
	if err != nil {
		return commonmeilisearch.StatusError(err, fmt.Sprintf("delete meilisearch collection %q", req.Collection))
	}
	return commonmeilisearch.WaitForTask(ctx, client, task.TaskUID, fmt.Sprintf("delete meilisearch collection %q", req.Collection))
}

// Upsert is a keyed upsert of dense vector records into an existing
// collection. A missing collection is NOT_FOUND.
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

	// Meilisearch creates a missing index on the first document addition, and
	// stores `_vectors` in an index that has no embedder. Either way the
	// records would land where no query can reach them and CreateCollection
	// could no longer repair it, and a write to a missing collection is a
	// request-wide error, so the collection is looked up before anything is
	// enqueued. That narrows but cannot close the window: Meilisearch has no
	// option to refuse a write to a missing index (`skipCreation` skips missing
	// documents, not indexes), so a collection deleted concurrently is
	// recreated without an embedder.
	dimensions, err := lookupCollection(ctx, client, req.Collection)
	if err != nil {
		return nil, err
	}

	// Item failures identified before the task is enqueued. A Meilisearch task
	// is atomic: one record it rejects fails every record of the batch. These
	// are therefore the only item failures the component can report, and what
	// Meilisearch would reject per record is checked here.
	failed := make([]search.FailedItem, 0)
	docs := make([]map[string]any, 0, len(req.Records))
	for _, record := range req.Records {
		if !commonmeilisearch.ValidDocumentID(record.ID) {
			failed = append(failed, commonmeilisearch.InvalidDocumentIDItem(record.ID))
			continue
		}
		if len(record.Values) == 0 {
			failed = append(failed, commonmeilisearch.NewFailedItem(record.ID, codes.InvalidArgument, "the record has no vector values"))
			continue
		}
		if len(record.Values) != dimensions {
			failed = append(failed, commonmeilisearch.NewFailedItem(record.ID, codes.InvalidArgument,
				"the record has %d vector values but the collection has %d dimensions", len(record.Values), dimensions))
			continue
		}
		if valuesErr := validateCosineValues(record.Values); valuesErr != nil {
			failed = append(failed, commonmeilisearch.NewFailedItem(record.ID, codes.InvalidArgument, "the record vector %s", valuesErr))
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
	// An ID Meilisearch cannot store names no record. Passing it on would fail
	// the whole read instead of omitting that record.
	ids := commonmeilisearch.ValidDocumentIDs(req.IDs)
	if len(ids) == 0 {
		return &vector.GetResponse{Records: []vector.Record{}}, nil
	}

	query := &meilisearchgo.DocumentsQuery{Ids: ids, Limit: int64(len(ids)), RetrieveVectors: req.IncludeValues}
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
	for _, id := range ids {
		if record, ok := byID[id]; ok {
			out.Records = append(out.Records, record)
			delete(byID, id)
		}
	}
	return out, nil
}

// Delete deletes records by ID. It is a write: the deletion task is
// acknowledged with the same mode and wait semantics as Upsert. IDs that do
// not exist are not an error; a missing collection is NOT_FOUND.
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
	// As for Upsert, the target is checked before anything is enqueued: a
	// plain search index sharing the instance is not a collection, and its
	// documents must not be deleted through the vector API.
	if _, err = lookupCollection(ctx, client, req.Collection); err != nil {
		return nil, err
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
// validation or is rejected by Meilisearch produces an error result; a
// request-wide failure (a closed component, a missing collection, invalid
// credentials, transport, a provider outage) fails the call, whichever query
// meets it.
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
			if requestWideQueryError(err) {
				return nil, err
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

// requestWideQueryError reports whether a failed query would fail every other
// query of its batch the same way. Only a rejection of the query itself is
// specific to it: INVALID_ARGUMENT, from the component's validation or from a
// Meilisearch `invalid_request` such as a vector of the wrong size or an
// unknown by_id. Credentials, a collection that disappeared, transport and
// provider failures are not.
func requestWideQueryError(err error) bool {
	return status.Code(err) != codes.InvalidArgument
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
	if hasVector {
		if len(req.Vector.Values) == 0 {
			return nil, status.Error(codes.InvalidArgument, "the query vector has no values")
		}
		if err := validateCosineValues(req.Vector.Values); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "the query vector %s", err)
		}
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
		// A cosine similarity lies in [-1, 1]. A cutoff outside that range is
		// rejected rather than clamped: clamping 1.5 to 1 would return exact
		// matches that the requested cutoff excludes.
		cosine := *req.ScoreThreshold
		if math.IsNaN(cosine) || cosine < -1 || cosine > 1 {
			return nil, status.Error(codes.InvalidArgument, "score_threshold must be a cosine similarity between -1 and 1")
		}
		threshold = rankingScoreThreshold(cosine)
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

// validateCosineValues checks that a vector has a cosine similarity with
// other vectors. A NaN or infinite value cannot be encoded in JSON, so
// Meilisearch never receives it. A vector whose values are all zero has no
// direction: its cosine similarity is undefined, and Meilisearch scores it as
// a perfect match instead of rejecting it.
func validateCosineValues(values []float32) error {
	zero := true
	for i, value := range values {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return fmt.Errorf("has a value that is not a finite number at index %d", i)
		}
		if value != 0 {
			zero = false
		}
	}
	if zero {
		return errors.New("has a zero norm, for which cosine similarity is undefined")
	}
	return nil
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
	return (1 + cosine) / 2
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
