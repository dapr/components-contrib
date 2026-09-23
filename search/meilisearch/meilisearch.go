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
	"reflect"
	"strconv"
	"strings"
	"sync"

	meilisearchgo "github.com/meilisearch/meilisearch-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonmeilisearch "github.com/dapr/components-contrib/common/component/meilisearch"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"
)

const (
	// defaultTopK is the page size used when a request does not set TopK.
	defaultTopK = int64(20)

	// supportsQueuedAck reports that Meilisearch offers a native durable
	// queued acknowledgement through its asynchronous task API.
	supportsQueuedAck = true

	// Index settings accepted in CreateIndex metadata.
	mdFilterableAttributes = "filterableAttributes"
	mdSortableAttributes   = "sortableAttributes"
	mdSearchableAttributes = "searchableAttributes"
)

// Meilisearch implements the Dapr Search building block with Meilisearch.
type Meilisearch struct {
	logger logger.Logger

	mu         sync.RWMutex
	client     meilisearchgo.ServiceManager
	dispatcher *commonmeilisearch.TaskDispatcher
	md         commonmeilisearch.MeilisearchMetadata
	closed     bool
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
	m.dispatcher = commonmeilisearch.NewTaskDispatcher(md, client, m.logger)
	m.closed = false
	return nil
}

// CreateIndex creates a Meilisearch index. Component-specific settings travel
// in the request metadata. The document ID is always declared sortable so the
// pagination tie-breaker is available. An existing index is ALREADY_EXISTS.
func (m *Meilisearch) CreateIndex(ctx context.Context, req *search.CreateIndexRequest) error {
	if req == nil || req.Index == "" {
		return status.Error(codes.InvalidArgument, "index is required")
	}
	client, _, err := m.ready()
	if err != nil {
		return err
	}

	task, err := client.CreateIndexWithContext(ctx, &meilisearchgo.IndexConfig{Uid: req.Index, PrimaryKey: commonmeilisearch.PrimaryKey})
	if err != nil {
		return commonmeilisearch.StatusError(err, fmt.Sprintf("create meilisearch index %q", req.Index))
	}
	// Meilisearch reports an existing index as a failed creation task with
	// the `index_already_exists` code, which maps to ALREADY_EXISTS.
	err = commonmeilisearch.WaitForTask(ctx, client, task.TaskUID, fmt.Sprintf("create meilisearch index %q", req.Index))
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return status.Errorf(codes.AlreadyExists, "meilisearch index %q already exists", req.Index)
		}
		return err
	}

	settings := settingsFromMetadata(req.Metadata)
	settingsTask, err := client.Index(req.Index).UpdateSettingsWithContext(ctx, settings)
	if err != nil {
		return commonmeilisearch.StatusError(err, fmt.Sprintf("update settings of meilisearch index %q", req.Index))
	}
	return commonmeilisearch.WaitForTask(ctx, client, settingsTask.TaskUID, fmt.Sprintf("update settings of meilisearch index %q", req.Index))
}

// GetIndex returns the document count and settings of a Meilisearch index.
func (m *Meilisearch) GetIndex(ctx context.Context, req *search.GetIndexRequest) (*search.GetIndexResponse, error) {
	if req == nil || req.Index == "" {
		return nil, status.Error(codes.InvalidArgument, "index is required")
	}
	client, _, err := m.ready()
	if err != nil {
		return nil, err
	}

	idx, err := client.GetIndexWithContext(ctx, req.Index)
	if err != nil {
		return nil, commonmeilisearch.StatusError(err, fmt.Sprintf("get meilisearch index %q", req.Index))
	}
	stats, err := idx.GetStatsWithContext(ctx)
	if err != nil {
		return nil, commonmeilisearch.StatusError(err, fmt.Sprintf("get stats of meilisearch index %q", req.Index))
	}
	settings, err := idx.GetSettingsWithContext(ctx)
	if err != nil {
		return nil, commonmeilisearch.StatusError(err, fmt.Sprintf("get settings of meilisearch index %q", req.Index))
	}
	if stats.NumberOfDocuments < 0 {
		return nil, status.Errorf(codes.Internal, "meilisearch index %q reported a negative document count", req.Index)
	}

	properties := map[string]string{"primaryKey": idx.PrimaryKey}
	addListProperty(properties, mdFilterableAttributes, settings.FilterableAttributes)
	addListProperty(properties, mdSortableAttributes, settings.SortableAttributes)
	addListProperty(properties, mdSearchableAttributes, settings.SearchableAttributes)

	return &search.GetIndexResponse{
		Index:         idx.UID,
		DocumentCount: uint64(stats.NumberOfDocuments),
		Properties:    properties,
	}, nil
}

// ListIndexes lists the Meilisearch indexes of the store.
func (m *Meilisearch) ListIndexes(ctx context.Context, req *search.ListIndexesRequest) (*search.ListIndexesResponse, error) {
	client, _, err := m.ready()
	if err != nil {
		return nil, err
	}
	_ = req
	res, err := client.ListIndexesWithContext(ctx, nil)
	if err != nil {
		return nil, commonmeilisearch.StatusError(err, "list meilisearch indexes")
	}
	out := &search.ListIndexesResponse{Indexes: make([]string, 0, len(res.Results))}
	for _, idx := range res.Results {
		out.Indexes = append(out.Indexes, idx.UID)
	}
	return out, nil
}

// DeleteIndex deletes a Meilisearch index.
func (m *Meilisearch) DeleteIndex(ctx context.Context, req *search.DeleteIndexRequest) error {
	if req == nil || req.Index == "" {
		return status.Error(codes.InvalidArgument, "index is required")
	}
	client, _, err := m.ready()
	if err != nil {
		return err
	}
	task, err := client.DeleteIndexWithContext(ctx, req.Index)
	if err != nil {
		return commonmeilisearch.StatusError(err, fmt.Sprintf("delete meilisearch index %q", req.Index))
	}
	return commonmeilisearch.WaitForTask(ctx, client, task.TaskUID, fmt.Sprintf("delete meilisearch index %q", req.Index))
}

// IndexDocuments is a keyed upsert of documents into a Meilisearch index.
func (m *Meilisearch) IndexDocuments(ctx context.Context, req *search.IndexDocumentsRequest) (*search.IndexDocumentsResponse, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, commonmeilisearch.ContextStatusError(ctx)
	}
	if req == nil || req.Index == "" {
		return nil, status.Error(codes.InvalidArgument, "index is required")
	}
	client, dispatcher, err := m.ready()
	if err != nil {
		return nil, err
	}

	ids := make([]string, len(req.Documents))
	for i, doc := range req.Documents {
		ids[i] = doc.ID
	}
	if err = search.ValidateWriteIDs(ids); err != nil {
		return nil, err
	}
	if err = search.ValidateIndexingOptions(ctx, req.Options, supportsQueuedAck); err != nil {
		return nil, err
	}

	// Item failures identified before the task is enqueued. A Meilisearch task
	// is atomic, so these are the only item failures the component can report.
	// The runtime already rejects non-object content; this is a defensive
	// check producing the same INVALID_ARGUMENT item failure.
	failed := make([]search.FailedItem, 0)
	docs := make([]map[string]any, 0, len(req.Documents))
	for _, doc := range req.Documents {
		content, contentErr := commonmeilisearch.DecodeContent(doc.Content)
		if contentErr != nil {
			failed = append(failed, search.FailedItem{ID: doc.ID, Error: status.Convert(contentErr)})
			continue
		}
		body := commonmeilisearch.CloneMap(content)
		body[commonmeilisearch.PrimaryKey] = doc.ID
		if meta := commonmeilisearch.EncodeMetadata(doc.Metadata); meta != nil {
			body[commonmeilisearch.MetadataField] = meta
		}
		docs = append(docs, body)
	}
	if len(docs) == 0 {
		return &search.IndexDocumentsResponse{FailedItems: failed, Ack: search.IndexAckCompleted}, nil
	}

	ack, err := commonmeilisearch.EnqueueWrite(ctx, dispatcher, req.Options, fmt.Sprintf("index documents into meilisearch index %q", req.Index),
		func(ctx context.Context) (*meilisearchgo.TaskInfo, error) {
			return client.Index(req.Index).AddDocumentsWithContext(ctx, docs,
				&meilisearchgo.DocumentOptions{PrimaryKey: meilisearchgo.StringPtr(commonmeilisearch.PrimaryKey)})
		})
	if err != nil {
		return nil, err
	}
	return &search.IndexDocumentsResponse{FailedItems: failed, Ack: ack}, nil
}

// GetDocuments fetches documents by ID. Found documents are returned in
// request order; documents that are not found are omitted.
func (m *Meilisearch) GetDocuments(ctx context.Context, req *search.GetDocumentsRequest) (*search.GetDocumentsResponse, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, commonmeilisearch.ContextStatusError(ctx)
	}
	if req == nil || req.Index == "" {
		return nil, status.Error(codes.InvalidArgument, "index is required")
	}
	client, _, err := m.ready()
	if err != nil {
		return nil, err
	}
	if len(req.IDs) == 0 {
		return &search.GetDocumentsResponse{Documents: []search.Document{}}, nil
	}

	query := &meilisearchgo.DocumentsQuery{Ids: req.IDs, Limit: int64(len(req.IDs))}
	if !req.IncludeContent {
		query.Fields = []string{commonmeilisearch.PrimaryKey, commonmeilisearch.MetadataField}
	}
	var res meilisearchgo.DocumentsResult
	if err := client.Index(req.Index).GetDocumentsWithContext(ctx, query, &res); err != nil {
		return nil, commonmeilisearch.StatusError(err, fmt.Sprintf("get documents of meilisearch index %q", req.Index))
	}

	byID := make(map[string]search.Document, len(res.Results))
	for _, hit := range res.Results {
		doc, _, _, err := documentFromHit(hit, req.IncludeContent)
		if err != nil {
			return nil, err
		}
		byID[doc.ID] = doc
	}
	out := &search.GetDocumentsResponse{Documents: make([]search.Document, 0, len(byID))}
	for _, id := range req.IDs {
		if doc, ok := byID[id]; ok {
			out.Documents = append(out.Documents, doc)
			delete(byID, id)
		}
	}
	return out, nil
}

// DeleteDocuments deletes documents by ID. It is a write: the deletion task
// is acknowledged with the same mode and wait semantics as IndexDocuments.
// IDs that do not exist are not an error.
func (m *Meilisearch) DeleteDocuments(ctx context.Context, req *search.DeleteDocumentsRequest) (*search.DeleteDocumentsResponse, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, commonmeilisearch.ContextStatusError(ctx)
	}
	if req == nil || req.Index == "" {
		return nil, status.Error(codes.InvalidArgument, "index is required")
	}
	client, dispatcher, err := m.ready()
	if err != nil {
		return nil, err
	}
	if err = search.ValidateIndexingOptions(ctx, req.Options, supportsQueuedAck); err != nil {
		return nil, err
	}
	if len(req.IDs) == 0 {
		return &search.DeleteDocumentsResponse{Ack: search.IndexAckCompleted}, nil
	}

	ack, err := commonmeilisearch.EnqueueWrite(ctx, dispatcher, req.Options, fmt.Sprintf("delete documents of meilisearch index %q", req.Index),
		func(ctx context.Context) (*meilisearchgo.TaskInfo, error) {
			return client.Index(req.Index).DeleteDocumentsWithContext(ctx, req.IDs, nil)
		})
	if err != nil {
		return nil, err
	}
	return &search.DeleteDocumentsResponse{Ack: ack}, nil
}

// Search queries a Meilisearch index.
func (m *Meilisearch) Search(ctx context.Context, req *search.SearchRequest) (*search.SearchResponse, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, commonmeilisearch.ContextStatusError(ctx)
	}
	if req == nil || req.Index == "" {
		return nil, status.Error(codes.InvalidArgument, "index is required")
	}
	client, _, err := m.ready()
	if err != nil {
		return nil, err
	}
	if req.Text != "" && len(req.Native) > 0 {
		return nil, status.Error(codes.InvalidArgument, "text and native are mutually exclusive")
	}

	msReq, fingerprint, err := buildSearchRequest(req)
	if err != nil {
		return nil, err
	}
	res, err := client.Index(req.Index).SearchWithContext(ctx, msReq.Query, msReq)
	if err != nil {
		return nil, commonmeilisearch.StatusError(err, fmt.Sprintf("search meilisearch index %q", req.Index))
	}
	return searchResponseFromMeilisearch(res, req.IncludeContent, fingerprint)
}

// GetComponentMetadata returns the metadata of the component.
func (m *Meilisearch) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(commonmeilisearch.MeilisearchMetadata{}), &metadataInfo, metadata.SearchType)
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
		return nil, nil, status.Error(codes.FailedPrecondition, "the meilisearch search component is closed")
	}
	if m.client == nil {
		return nil, nil, status.Error(codes.FailedPrecondition, "the meilisearch search component is not initialized")
	}
	return m.client, m.dispatcher, nil
}

// settingsFromMetadata builds the index settings applied after creation. The
// document ID is always sortable: Meilisearch only sorts on declared
// attributes and the ID is the pagination tie-breaker appended to every sort.
func settingsFromMetadata(md map[string]string) *meilisearchgo.Settings {
	sortable := commonmeilisearch.SplitList(md[mdSortableAttributes])
	if !slicesContains(sortable, commonmeilisearch.PrimaryKey) {
		sortable = append(sortable, commonmeilisearch.PrimaryKey)
	}
	settings := &meilisearchgo.Settings{SortableAttributes: sortable}
	if filterable := commonmeilisearch.SplitList(md[mdFilterableAttributes]); len(filterable) > 0 {
		settings.FilterableAttributes = filterable
	}
	if searchable := commonmeilisearch.SplitList(md[mdSearchableAttributes]); len(searchable) > 0 {
		settings.SearchableAttributes = searchable
	}
	return settings
}

func addListProperty(properties map[string]string, key string, values []string) {
	if len(values) > 0 {
		properties[key] = strings.Join(values, ",")
	}
}

// buildSearchRequest translates a Dapr search request and returns the
// fingerprint a continuation token is bound to.
func buildSearchRequest(req *search.SearchRequest) (*meilisearchgo.SearchRequest, string, error) {
	msReq := &meilisearchgo.SearchRequest{Query: req.Text, ShowRankingScore: true}
	if req.TopK > 0 {
		msReq.Limit = int64(req.TopK)
	} else {
		msReq.Limit = defaultTopK
	}
	if len(req.ReturnFields) > 0 {
		msReq.AttributesToRetrieve = withReservedFields(req.ReturnFields)
	} else if !req.IncludeContent {
		msReq.AttributesToRetrieve = []string{commonmeilisearch.PrimaryKey, commonmeilisearch.MetadataField}
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
		// Search filters address the keys of a document's content, which are
		// stored as top-level attributes.
		filter, err := commonmeilisearch.TranslateFilter(req.Filter)
		if err != nil {
			return nil, "", status.Errorf(codes.InvalidArgument, "translate filter: %v", err)
		}
		msReq.Filter = filter
	}
	if len(req.Native) > 0 {
		if err := applyNative(msReq, req.Native); err != nil {
			return nil, "", err
		}
	}

	// The token is bound to the index and every result-affecting element of
	// the request. Transport metadata is not bound, and neither is the
	// continuation token itself.
	fingerprint := commonmeilisearch.QueryFingerprint(
		"search", req.Index, req.Text, req.Native, req.Filter, msReq.Limit,
		req.ReturnFields, req.IncludeContent, req.SearchFields, sortClauses(req.Sort), req.HighlightFields,
	)

	if req.ContinuationToken != "" {
		offset, err := commonmeilisearch.DecodeContinuationToken(req.ContinuationToken, fingerprint)
		if err != nil {
			return nil, "", err
		}
		msReq.Offset = offset
	}
	return msReq, fingerprint, nil
}

// applyNative merges a provider-native query into the request. A native query
// must not embed its own pagination or a conflicting sort.
func applyNative(msReq *meilisearchgo.SearchRequest, native map[string]any) error {
	for _, reserved := range []string{"offset", "limit", "page", "hitsPerPage", "sort"} {
		if _, ok := native[reserved]; ok {
			return status.Errorf(codes.InvalidArgument, "a native query must not set %q: pagination and sorting are portable request fields", reserved)
		}
	}
	data, err := json.Marshal(native)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "encode the native meilisearch query: %v", err)
	}
	if err := json.Unmarshal(data, msReq); err != nil {
		return status.Errorf(codes.InvalidArgument, "decode the native meilisearch query: %v", err)
	}
	return nil
}

func withReservedFields(fields []string) []string {
	out := make([]string, 0, len(fields)+2)
	out = append(out, fields...)
	for _, reserved := range []string{commonmeilisearch.PrimaryKey, commonmeilisearch.MetadataField} {
		if !slicesContains(out, reserved) {
			out = append(out, reserved)
		}
	}
	return out
}

func slicesContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// sortClauses translates the portable sort clauses and appends the document ID
// as a stable tie-breaker so pagination is deterministic.
func sortClauses(clauses []search.SortClause) []string {
	if len(clauses) == 0 {
		return nil
	}
	out := make([]string, 0, len(clauses)+1)
	tieBreaker := true
	for _, clause := range clauses {
		order := "asc"
		if clause.Order == search.SortOrderDesc {
			order = "desc"
		}
		if clause.Field == commonmeilisearch.PrimaryKey {
			tieBreaker = false
		}
		out = append(out, clause.Field+":"+order)
	}
	if tieBreaker {
		out = append(out, commonmeilisearch.PrimaryKey+":asc")
	}
	return out
}

func searchResponseFromMeilisearch(res *meilisearchgo.SearchResponse, includeContent bool, fingerprint string) (*search.SearchResponse, error) {
	out := &search.SearchResponse{Hits: make([]search.Hit, 0, len(res.Hits))}

	var total int64
	switch {
	case res.TotalHits > 0:
		total = res.TotalHits
		totalHits := uint64(res.TotalHits)
		out.TotalHits = &totalHits
		out.TotalHitsRelation = search.TotalHitsRelationExact
	case res.EstimatedTotalHits > 0:
		total = res.EstimatedTotalHits
		totalHits := uint64(res.EstimatedTotalHits)
		out.TotalHits = &totalHits
		out.TotalHitsRelation = search.TotalHitsRelationEstimate
	}

	for _, hit := range res.Hits {
		doc, score, highlights, err := documentFromHit(hit, includeContent)
		if err != nil {
			return nil, err
		}
		out.Hits = append(out.Hits, search.Hit{Document: doc, Score: score, Highlights: highlights})
	}

	next := res.Offset + int64(len(res.Hits))
	if len(res.Hits) > 0 && next < total {
		out.ContinuationToken = commonmeilisearch.EncodeContinuationToken(fingerprint, next)
	}
	return out, nil
}

// documentFromHit maps a Meilisearch hit into a Dapr document, its relevance
// score and its highlights. Meilisearch `_rankingScore` is already
// higher-is-better, so it is reported unchanged.
func documentFromHit(hit meilisearchgo.Hit, includeContent bool) (search.Document, float64, map[string]string, error) {
	doc := search.Document{}
	content := map[string]any{}
	var score float64
	var highlights map[string]string

	for key, raw := range hit {
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return doc, 0, nil, status.Errorf(codes.Internal, "decode meilisearch hit attribute %q", key)
		}
		switch key {
		case commonmeilisearch.PrimaryKey:
			doc.ID = fmt.Sprint(value)
		case commonmeilisearch.MetadataField:
			doc.Metadata = commonmeilisearch.DecodeMetadata(value)
		case commonmeilisearch.RankingScoreField:
			score = commonmeilisearch.NumberAsFloat(value)
		case commonmeilisearch.FormattedField:
			highlights = highlightsFromFormatted(value)
		default:
			if includeContent && !commonmeilisearch.IsReservedField(key) {
				content[key] = value
			}
		}
	}

	if includeContent {
		encoded, err := commonmeilisearch.EncodeContent(content)
		if err != nil {
			return doc, 0, nil, err
		}
		doc.Content = encoded
	}
	return doc, score, highlights, nil
}

func highlightsFromFormatted(value any) map[string]string {
	formatted, ok := value.(map[string]any)
	if !ok || len(formatted) == 0 {
		return nil
	}
	out := make(map[string]string, len(formatted))
	for field, snippet := range formatted {
		if commonmeilisearch.IsReservedField(field) {
			continue
		}
		switch typed := snippet.(type) {
		case string:
			out[field] = typed
		case float64:
			out[field] = strconv.FormatFloat(typed, 'f', -1, 64)
		default:
			encoded, err := json.Marshal(typed)
			if err != nil {
				continue
			}
			out[field] = string(encoded)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
