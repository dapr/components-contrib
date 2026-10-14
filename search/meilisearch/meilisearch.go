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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
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

	if err = configureIndex(ctx, client, req.Index, settingsFromMetadata(req.Metadata)); err != nil {
		// The index was created by this request but never configured. Left
		// behind, it would make every retry fail with ALREADY_EXISTS without
		// applying the settings, so the creation is undone.
		commonmeilisearch.RollbackIndex(ctx, client, req.Index, m.logger)
		return err
	}
	return nil
}

// configureIndex applies the settings of a newly created index and waits for
// them to take effect. The ranking rules are the defaults of the new index,
// which differ between Meilisearch versions, with sort moved first.
func configureIndex(ctx context.Context, client meilisearchgo.ServiceManager, index string, settings *meilisearchgo.Settings) error {
	rules, err := client.Index(index).GetRankingRulesWithContext(ctx)
	if err != nil {
		return commonmeilisearch.StatusError(err, fmt.Sprintf("get ranking rules of meilisearch index %q", index))
	}
	if rules != nil {
		settings.RankingRules = sortFirst(*rules)
	}

	msg := fmt.Sprintf("update settings of meilisearch index %q", index)
	task, err := client.Index(index).UpdateSettingsWithContext(ctx, settings)
	if err != nil {
		return commonmeilisearch.StatusError(err, msg)
	}
	return commonmeilisearch.WaitForTask(ctx, client, task.TaskUID, msg)
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
	uids, err := commonmeilisearch.ListIndexUIDs(ctx, client)
	if err != nil {
		return nil, commonmeilisearch.StatusError(err, "list meilisearch indexes")
	}
	return &search.ListIndexesResponse{Indexes: uids}, nil
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

// IndexDocuments is a keyed upsert of documents into an existing Meilisearch
// index. A missing index is NOT_FOUND.
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

	// Meilisearch creates a missing index on the first document addition. Such
	// an index has none of the settings CreateIndex applies (the document ID
	// would not be sortable, so sorted and paginated searches would fail), and
	// a write to a missing index is a request-wide error, so the index is
	// looked up before anything is enqueued.
	if _, err = client.GetIndexWithContext(ctx, req.Index); err != nil {
		return nil, commonmeilisearch.StatusError(err, fmt.Sprintf("get meilisearch index %q", req.Index))
	}

	// Item failures identified before the task is enqueued. A Meilisearch task
	// is atomic: one document it rejects fails every document of the batch.
	// These are therefore the only item failures the component can report, and
	// what Meilisearch would reject per document is checked here. The runtime
	// already rejects non-object content; that check is defensive and produces
	// the same INVALID_ARGUMENT item failure.
	failed := make([]search.FailedItem, 0)
	docs := make([]map[string]any, 0, len(req.Documents))
	for _, doc := range req.Documents {
		if !commonmeilisearch.ValidDocumentID(doc.ID) {
			failed = append(failed, commonmeilisearch.InvalidDocumentIDItem(doc.ID))
			continue
		}
		content, contentErr := commonmeilisearch.DecodeContent(doc.Content)
		if contentErr != nil {
			failed = append(failed, search.FailedItem{ID: doc.ID, Error: status.Convert(contentErr)})
			continue
		}
		// A reserved attribute would be overwritten below or dropped when the
		// document is read back, so it is rejected rather than silently lost.
		if field, reserved := commonmeilisearch.ReservedContentField(content); reserved {
			failed = append(failed, commonmeilisearch.NewFailedItem(doc.ID, codes.InvalidArgument,
				"content must not use the attribute %q: %q, %q, %q and attributes starting with an underscore are reserved",
				field, commonmeilisearch.PrimaryKey, commonmeilisearch.MetadataField, commonmeilisearch.PayloadField))
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
	// An ID Meilisearch cannot store names no document. Passing it on would
	// fail the whole read instead of omitting that document.
	ids := commonmeilisearch.ValidDocumentIDs(req.IDs)
	if len(ids) == 0 {
		return &search.GetDocumentsResponse{Documents: []search.Document{}}, nil
	}

	query := &meilisearchgo.DocumentsQuery{Ids: ids, Limit: int64(len(ids))}
	if !req.IncludeContent {
		query.Fields = []string{commonmeilisearch.PrimaryKey, commonmeilisearch.MetadataField}
	}
	var res meilisearchgo.DocumentsResult
	if err := client.Index(req.Index).GetDocumentsWithContext(ctx, query, &res); err != nil {
		return nil, commonmeilisearch.StatusError(err, fmt.Sprintf("get documents of meilisearch index %q", req.Index))
	}

	byID := make(map[string]search.Document, len(res.Results))
	for _, hit := range res.Results {
		doc, _, _, err := documentFromHit(hit, req.IncludeContent, nil)
		if err != nil {
			return nil, err
		}
		byID[doc.ID] = doc
	}
	out := &search.GetDocumentsResponse{Documents: make([]search.Document, 0, len(byID))}
	for _, id := range ids {
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
	idx := client.Index(req.Index)
	settings := &indexSettings{index: idx, name: req.Index}

	// Document metadata is stored under MetadataField, which the default
	// searchableAttributes (`*`) makes searchable, and Meilisearch cannot
	// exclude one attribute from `*`. A text query that does not name the
	// attributes to search is therefore restricted to the content attributes.
	if msReq.Query != "" && len(msReq.AttributesToSearchOn) == 0 {
		attributes, restrict, attributesErr := contentSearchAttributes(ctx, settings)
		if attributesErr != nil {
			return nil, attributesErr
		}
		if restrict {
			if len(attributes) == 0 {
				// No document has a content attribute the text could match.
				return emptySearchResponse(), nil
			}
			msReq.AttributesToSearchOn = attributes
		}
	}

	res, err := idx.SearchWithContext(ctx, msReq.Query, msReq)
	if err != nil {
		return nil, commonmeilisearch.StatusError(err, fmt.Sprintf("search meilisearch index %q", req.Index))
	}

	// Meilisearch stops counting, and paginating, at the maxTotalHits setting
	// of the index, so a total that reaches it is reported as a lower bound.
	var maxTotalHits int64
	if res.EstimatedTotalHits > 0 || res.TotalHits > 0 {
		indexSettings, settingsErr := settings.get(ctx)
		if settingsErr != nil {
			return nil, settingsErr
		}
		if indexSettings.Pagination != nil {
			maxTotalHits = indexSettings.Pagination.MaxTotalHits
		}
	}
	return searchResponseFromMeilisearch(res, req.IncludeContent, formattedFields(msReq), fingerprint, maxTotalHits)
}

// indexSettings reads the settings of the searched index at most once per
// request.
type indexSettings struct {
	index    meilisearchgo.IndexManager
	name     string
	settings *meilisearchgo.Settings
}

func (s *indexSettings) get(ctx context.Context) (*meilisearchgo.Settings, error) {
	if s.settings != nil {
		return s.settings, nil
	}
	settings, err := s.index.GetSettingsWithContext(ctx)
	if err != nil {
		return nil, commonmeilisearch.StatusError(err, fmt.Sprintf("get settings of meilisearch index %q", s.name))
	}
	s.settings = settings
	return settings, nil
}

// contentSearchAttributes returns the attributes a text query searches so that
// it matches document content only, and whether the query must be restricted
// to them. An explicit searchableAttributes setting was chosen at CreateIndex
// and is left alone. With the default (`*`) every attribute of the index is
// searchable, and the content attributes are read from its field
// distribution, which lists top-level attributes. Each one is passed with the
// `.*` pattern as well, as Meilisearch only searches the nested fields of an
// attribute named in attributesToSearchOn through that pattern.
func contentSearchAttributes(ctx context.Context, settings *indexSettings) ([]string, bool, error) {
	indexSettings, err := settings.get(ctx)
	if err != nil {
		return nil, false, err
	}
	if !slicesContains(indexSettings.SearchableAttributes, "*") {
		return nil, false, nil
	}
	stats, err := settings.index.GetStatsWithContext(ctx)
	if err != nil {
		return nil, false, commonmeilisearch.StatusError(err, fmt.Sprintf("get stats of meilisearch index %q", settings.name))
	}
	attributes := make([]string, 0, 2*len(stats.FieldDistribution))
	for field := range stats.FieldDistribution {
		if !commonmeilisearch.IsReservedField(field) {
			attributes = append(attributes, field, field+".*")
		}
	}
	sort.Strings(attributes)
	return attributes, true, nil
}

func emptySearchResponse() *search.SearchResponse {
	var total uint64
	return &search.SearchResponse{Hits: []search.Hit{}, TotalHits: &total, TotalHitsRelation: search.TotalHitsRelationExact}
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
// must not embed its own pagination or a conflicting sort, and it cannot undo
// what the portable request guarantees: the portable filter still applies and
// the attributes a hit is built from are always returned.
func applyNative(msReq *meilisearchgo.SearchRequest, native map[string]any) error {
	for _, reserved := range []string{"offset", "limit", "page", "hitsPerPage", "sort"} {
		if _, ok := native[reserved]; ok {
			return status.Errorf(codes.InvalidArgument, "a native query must not set %q: pagination and sorting are portable request fields", reserved)
		}
	}
	portableFilter, _ := msReq.Filter.(string)
	_, nativeFilter := native["filter"]

	data, err := json.Marshal(native)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "encode the native meilisearch query: %v", err)
	}
	if err := json.Unmarshal(data, msReq); err != nil {
		return status.Errorf(codes.InvalidArgument, "decode the native meilisearch query: %v", err)
	}

	// A hit is mapped from its document ID, metadata and ranking score,
	// whatever the native query selects.
	msReq.ShowRankingScore = true
	if len(msReq.AttributesToRetrieve) > 0 && !slicesContains(msReq.AttributesToRetrieve, "*") {
		msReq.AttributesToRetrieve = withReservedFields(msReq.AttributesToRetrieve)
	}
	// A native filter narrows the portable one instead of replacing it: a
	// native query must never widen a filtered request.
	if portableFilter != "" && nativeFilter {
		msReq.Filter = andFilters(portableFilter, msReq.Filter)
	}
	return nil
}

// andFilters combines the portable filter with the filter of a native query.
// In the array form of a Meilisearch filter every element must match, and an
// element is either a complete filter expression or an array of alternatives.
func andFilters(portable string, native any) any {
	switch typed := native.(type) {
	case nil:
		return portable
	case string:
		if strings.TrimSpace(typed) == "" {
			return portable
		}
		return []any{portable, typed}
	case []any:
		return append([]any{portable}, typed...)
	default:
		// Not a filter Meilisearch accepts; it rejects the request.
		return []any{portable, typed}
	}
}

// formattedFields returns the attributes whose formatted value was requested:
// the highlighted ones and, for a native query, the cropped ones.
func formattedFields(msReq *meilisearchgo.SearchRequest) []string {
	if len(msReq.AttributesToCrop) == 0 {
		return msReq.AttributesToHighlight
	}
	fields := make([]string, 0, len(msReq.AttributesToHighlight)+len(msReq.AttributesToCrop))
	fields = append(fields, msReq.AttributesToHighlight...)
	for _, field := range msReq.AttributesToCrop {
		// A cropped attribute may carry its own length (`body:20`).
		name, _, _ := strings.Cut(field, ":")
		if !slicesContains(fields, name) {
			fields = append(fields, name)
		}
	}
	return fields
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

// sortFirst moves the sort ranking rule to the front of rules. Meilisearch
// applies its relevance rules before sort by default, so a text query with an
// explicit sort would come back in relevance order. With sort first the
// requested order decides and relevance breaks ties; the rule has no effect on
// a query without a sort.
func sortFirst(rules []string) []string {
	out := make([]string, 0, len(rules)+1)
	out = append(out, "sort")
	for _, rule := range rules {
		if rule != "sort" {
			out = append(out, rule)
		}
	}
	return out
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

// searchResponseFromMeilisearch maps a Meilisearch search response.
// maxTotalHits is the pagination limit of the index, or zero when unknown.
func searchResponseFromMeilisearch(res *meilisearchgo.SearchResponse, includeContent bool, highlightFields []string, fingerprint string, maxTotalHits int64) (*search.SearchResponse, error) {
	out := &search.SearchResponse{Hits: make([]search.Hit, 0, len(res.Hits))}

	// Requests are always paginated with offset and limit (a native query
	// cannot set page or hitsPerPage), for which Meilisearch reports
	// estimatedTotalHits; totalHits only comes with page-based pagination. A
	// total is therefore always reported: an estimate of zero is a real count,
	// not a missing one.
	total, relation := res.EstimatedTotalHits, search.TotalHitsRelationEstimate
	if res.TotalHits > 0 {
		total, relation = res.TotalHits, search.TotalHitsRelationExact
	}
	if total < 0 {
		return nil, status.Error(codes.Internal, "meilisearch reported a negative number of hits")
	}
	// Meilisearch neither counts nor returns hits past maxTotalHits: a total
	// that reached it only says that at least that many documents match.
	if maxTotalHits > 0 && total >= maxTotalHits {
		relation = search.TotalHitsRelationLowerBound
	}
	totalHits := uint64(total)
	out.TotalHits = &totalHits
	out.TotalHitsRelation = relation

	for _, hit := range res.Hits {
		doc, score, highlights, err := documentFromHit(hit, includeContent, highlightFields)
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
func documentFromHit(hit meilisearchgo.Hit, includeContent bool, highlightFields []string) (search.Document, float64, map[string]string, error) {
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
			highlights = highlightsFromFormatted(value, highlightFields)
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

// highlightsFromFormatted extracts the requested fields from the `_formatted`
// attribute of a hit. Meilisearch puts every retrieved attribute in it,
// highlighted or not, with nested objects kept as objects, so each requested
// field is looked up by its path and reported under that path. `*` requests
// every top-level attribute.
func highlightsFromFormatted(value any, fields []string) map[string]string {
	formatted, ok := value.(map[string]any)
	if !ok || len(formatted) == 0 || len(fields) == 0 {
		return nil
	}
	out := make(map[string]string, len(fields))
	for _, field := range fields {
		if field == "*" {
			for key, snippet := range formatted {
				if !commonmeilisearch.IsReservedField(key) {
					addHighlight(out, key, snippet)
				}
			}
			continue
		}
		if commonmeilisearch.IsReservedField(field) {
			continue
		}
		if snippet, found := lookupPath(formatted, field); found {
			addHighlight(out, field, snippet)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// lookupPath resolves a dot-separated attribute path in a formatted hit. An
// attribute whose own name contains a dot is matched before the path is split,
// and a path through an array addresses that attribute of every element.
func lookupPath(value any, path string) (any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		if leaf, ok := typed[path]; ok {
			return leaf, true
		}
		for i := range len(path) {
			if path[i] != '.' {
				continue
			}
			child, ok := typed[path[:i]]
			if !ok {
				continue
			}
			if leaf, found := lookupPath(child, path[i+1:]); found {
				return leaf, true
			}
		}
	case []any:
		leaves := make([]any, 0, len(typed))
		for _, item := range typed {
			if leaf, found := lookupPath(item, path); found {
				leaves = append(leaves, leaf)
			}
		}
		if len(leaves) > 0 {
			return leaves, true
		}
	}
	return nil, false
}

// addHighlight records the formatted value of a field. A highlighted array or
// object keeps its structure as JSON, without escaping the highlight tags.
func addHighlight(out map[string]string, field string, snippet any) {
	switch typed := snippet.(type) {
	case nil:
	case string:
		out[field] = typed
	default:
		var encoded bytes.Buffer
		encoder := json.NewEncoder(&encoded)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(typed); err != nil {
			return
		}
		out[field] = strings.TrimSuffix(encoded.String(), "\n")
	}
}
