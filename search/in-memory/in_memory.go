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

package inmemory

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/common/component/indexing"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"
)

const defaultTopK = 10

var errClosed = status.Error(codes.FailedPrecondition, "the in-memory search store is closed")

// Metadata is the in-memory search store metadata.
type Metadata struct {
	// IndexingDelay queues every write and applies it once the delay has
	// passed, so that queued acknowledgements and wait timeouts can be
	// observed. A development and test aid.
	IndexingDelay time.Duration `mapstructure:"indexingDelay"`
}

type document struct {
	search.Document
	fields map[string]any
}

type index struct {
	docs map[string]document
}

type store struct {
	lock    sync.RWMutex
	indexes map[string]*index
	md      Metadata
}

// NewInMemorySearch returns a search store that keeps every index in process
// memory. It is meant for development and tests.
func NewInMemorySearch(logger.Logger) search.Search {
	return &store{indexes: make(map[string]*index)}
}

func (s *store) Init(_ context.Context, meta search.Metadata) error {
	return kmeta.DecodeMetadata(meta.Properties, &s.md)
}

func (s *store) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(Metadata{}), &metadataInfo, metadata.SearchType)
	return
}

func (s *store) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.indexes = nil
	return nil
}

// index returns the named index. The caller holds the lock.
func (s *store) index(ctx context.Context, name string) (*index, error) {
	if err := indexing.CtxErr(ctx); err != nil {
		return nil, err
	}
	if s.indexes == nil {
		return nil, errClosed
	}
	idx, ok := s.indexes[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "index %q not found", name)
	}
	return idx, nil
}

func (s *store) CreateIndex(ctx context.Context, req *search.CreateIndexRequest) error {
	if err := indexing.CtxErr(ctx); err != nil {
		return err
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.indexes == nil {
		return errClosed
	}
	if _, ok := s.indexes[req.Index]; ok {
		return status.Errorf(codes.AlreadyExists, "index %q already exists", req.Index)
	}
	s.indexes[req.Index] = &index{docs: make(map[string]document)}
	return nil
}

func (s *store) GetIndex(ctx context.Context, req *search.GetIndexRequest) (*search.GetIndexResponse, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	idx, err := s.index(ctx, req.Index)
	if err != nil {
		return nil, err
	}
	return &search.GetIndexResponse{Index: req.Index, DocumentCount: uint64(len(idx.docs))}, nil
}

func (s *store) ListIndexes(ctx context.Context, _ *search.ListIndexesRequest) (*search.ListIndexesResponse, error) {
	if err := indexing.CtxErr(ctx); err != nil {
		return nil, err
	}
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.indexes == nil {
		return nil, errClosed
	}
	return &search.ListIndexesResponse{Indexes: slices.Sorted(maps.Keys(s.indexes))}, nil
}

func (s *store) DeleteIndex(ctx context.Context, req *search.DeleteIndexRequest) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, err := s.index(ctx, req.Index); err != nil {
		return err
	}
	delete(s.indexes, req.Index)
	return nil
}

func (s *store) IndexDocuments(ctx context.Context, req *search.IndexDocumentsRequest) (*search.IndexDocumentsResponse, error) {
	ids := make([]string, len(req.Documents))
	for i, doc := range req.Documents {
		ids[i] = doc.ID
	}
	if err := search.ValidateWriteIDs(ids); err != nil {
		return nil, err
	}
	s.lock.RLock()
	idx, err := s.index(ctx, req.Index)
	s.lock.RUnlock()
	if err != nil {
		return nil, err
	}

	var failed []search.FailedItem
	docs := make([]document, 0, len(req.Documents))
	for _, doc := range req.Documents {
		var fields map[string]any
		if vErr := search.ValidateDocumentContent(doc.Content); vErr != nil {
			failed = append(failed, search.FailedItem{ID: doc.ID, Error: status.Convert(vErr)})
			continue
		}
		if vErr := json.Unmarshal(doc.Content, &fields); vErr != nil {
			failed = append(failed, search.FailedItem{ID: doc.ID, Error: status.New(codes.InvalidArgument, vErr.Error())})
			continue
		}
		docs = append(docs, document{Document: doc, fields: fields})
	}

	ack, err := indexing.Apply(ctx, req.Options, s.md.IndexingDelay, func() {
		s.lock.Lock()
		defer s.lock.Unlock()
		// The index may have been replaced while the write was queued.
		if s.indexes[req.Index] == idx {
			for _, doc := range docs {
				idx.docs[doc.ID] = doc
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return &search.IndexDocumentsResponse{FailedItems: failed, Ack: ack}, nil
}

func (s *store) GetDocuments(ctx context.Context, req *search.GetDocumentsRequest) (*search.GetDocumentsResponse, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	idx, err := s.index(ctx, req.Index)
	if err != nil {
		return nil, err
	}
	docs := make([]search.Document, 0, len(req.IDs))
	for _, id := range req.IDs {
		if doc, ok := idx.docs[id]; ok {
			docs = append(docs, doc.view(req.IncludeContent, nil))
		}
	}
	return &search.GetDocumentsResponse{Documents: docs}, nil
}

func (s *store) DeleteDocuments(ctx context.Context, req *search.DeleteDocumentsRequest) (*search.DeleteDocumentsResponse, error) {
	s.lock.RLock()
	idx, err := s.index(ctx, req.Index)
	s.lock.RUnlock()
	if err != nil {
		return nil, err
	}
	ack, err := indexing.Apply(ctx, req.Options, s.md.IndexingDelay, func() {
		s.lock.Lock()
		defer s.lock.Unlock()
		if s.indexes[req.Index] == idx {
			for _, id := range req.IDs {
				delete(idx.docs, id)
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return &search.DeleteDocumentsResponse{Ack: ack}, nil
}

func (s *store) Search(ctx context.Context, req *search.SearchRequest) (*search.SearchResponse, error) {
	switch {
	case req.Text != "" && len(req.Native) > 0:
		return nil, status.Error(codes.InvalidArgument, "text and native are mutually exclusive")
	case len(req.Native) > 0:
		return nil, status.Error(codes.Unimplemented, "the in-memory search store does not support native queries")
	case len(req.HighlightFields) > 0:
		return nil, status.Error(codes.Unimplemented, "the in-memory search store does not support highlights")
	}
	match, err := indexing.CompileFilter(req.Filter)
	if err != nil {
		return nil, err
	}
	shape, err := queryShape(req)
	if err != nil {
		return nil, err
	}
	offset, err := parseToken(req.ContinuationToken, shape)
	if err != nil {
		return nil, err
	}
	terms := tokenize(req.Text)

	s.lock.RLock()
	defer s.lock.RUnlock()
	idx, err := s.index(ctx, req.Index)
	if err != nil {
		return nil, err
	}

	type hit struct {
		document
		score float64
	}
	hits := make([]hit, 0, len(idx.docs))
	for _, doc := range idx.docs {
		if !match(doc.fields) {
			continue
		}
		if score, ok := textScore(doc.fields, terms, req.SearchFields); ok {
			hits = append(hits, hit{document: doc, score: score})
		}
	}
	slices.SortFunc(hits, func(a, b hit) int {
		for _, clause := range req.Sort {
			c := compareField(a.fields, b.fields, clause.Field)
			if clause.Order == search.SortOrderDesc {
				c = -c
			}
			if c != 0 {
				return c
			}
		}
		return cmp.Or(cmp.Compare(b.score, a.score), strings.Compare(a.ID, b.ID))
	})

	total := uint64(len(hits))
	start := min(offset, len(hits))
	end := min(start+cmp.Or(int(req.TopK), defaultTopK), len(hits))
	resp := &search.SearchResponse{
		Hits:              make([]search.Hit, 0, end-start),
		TotalHits:         &total,
		TotalHitsRelation: search.TotalHitsRelationExact,
	}
	for _, h := range hits[start:end] {
		resp.Hits = append(resp.Hits, search.Hit{Document: h.view(req.IncludeContent, req.ReturnFields), Score: h.score})
	}
	if end < len(hits) {
		resp.ContinuationToken = strconv.Itoa(end) + "." + shape
	}
	return resp, nil
}

// compareField orders two documents on one sort field: by sortRank first,
// then by value within a rank, so that the order is total and pages are
// disjoint.
func compareField(a, b map[string]any, path string) int {
	x, foundX := indexing.Lookup(a, path)
	y, foundY := indexing.Lookup(b, path)
	if c := cmp.Compare(sortRank(x, foundX), sortRank(y, foundY)); c != 0 {
		return c
	}
	c, _ := indexing.Compare(x, y)
	return c
}

// sortRank groups the values that cannot be compared with each other:
// numbers, then strings, then booleans, then nulls and nested values, then
// missing fields.
func sortRank(v any, found bool) int {
	if !found {
		return 4
	}
	switch v.(type) {
	case string:
		return 1
	case bool:
		return 2
	}
	if _, ok := indexing.Compare(v, 0.0); ok {
		return 0
	}
	return 3
}

// view returns the document as a caller sees it: content only when asked for,
// projected onto returnFields when any are given.
func (d document) view(includeContent bool, returnFields []string) search.Document {
	out := search.Document{ID: d.ID, Metadata: d.Metadata}
	if !includeContent {
		return out
	}
	if len(returnFields) == 0 {
		out.Content = d.Content
		return out
	}
	projected := make(map[string]any, len(returnFields))
	for _, path := range returnFields {
		if coveredByPrefix(path, returnFields) {
			continue
		}
		v, ok := indexing.Lookup(d.fields, path)
		if !ok {
			continue
		}
		parts := strings.Split(path, ".")
		m := projected
		for _, part := range parts[:len(parts)-1] {
			next, ok := m[part].(map[string]any)
			if !ok {
				next = make(map[string]any)
				m[part] = next
			}
			m = next
		}
		m[parts[len(parts)-1]] = v
	}
	out.Content, _ = json.Marshal(projected)
	return out
}

// coveredByPrefix reports whether another requested path is a strict prefix
// of path, in which case that path already projects the whole object.
func coveredByPrefix(path string, paths []string) bool {
	for _, other := range paths {
		if len(other) < len(path) && strings.HasPrefix(path, other+".") {
			return true
		}
	}
	return false
}

// queryShape fingerprints everything in a query but its continuation token,
// so that a token is only accepted by the query it was issued for.
func queryShape(req *search.SearchRequest) (string, error) {
	b, err := json.Marshal([]any{req.Index, req.Text, req.Filter, req.TopK, req.ReturnFields, req.IncludeContent, req.SearchFields, req.Sort})
	if err != nil {
		return "", status.Error(codes.InvalidArgument, err.Error())
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8]), nil
}

func parseToken(token, shape string) (int, error) {
	if token == "" {
		return 0, nil
	}
	offset, hash, ok := strings.Cut(token, ".")
	n, err := strconv.Atoi(offset)
	if !ok || err != nil || n < 0 || hash != shape {
		return 0, status.Error(codes.InvalidArgument, "the continuation token is malformed or was issued for a different query")
	}
	return n, nil
}

// textScore counts the occurrences of the query terms in the searchable
// fields. A document matches only when every term occurs; no terms match
// every document.
//
// ponytail: every query re-tokenizes every document, O(documents * words). Count words per field at index time if a development index ever grows large enough to notice.
func textScore(fields map[string]any, terms, searchFields []string) (float64, bool) {
	if len(terms) == 0 {
		return 0, true
	}
	var words []string
	if len(searchFields) == 0 {
		collectWords(fields, &words)
	}
	for _, path := range searchFields {
		if v, ok := indexing.Lookup(fields, path); ok {
			collectWords(v, &words)
		}
	}
	counts := make(map[string]int, len(words))
	for _, w := range words {
		counts[w]++
	}
	score := 0
	for _, term := range terms {
		if counts[term] == 0 {
			return 0, false
		}
		score += counts[term]
	}
	return float64(score), true
}

func collectWords(v any, words *[]string) {
	switch x := v.(type) {
	case string:
		*words = append(*words, tokenize(x)...)
	case float64:
		*words = append(*words, tokenize(strconv.FormatFloat(x, 'f', -1, 64))...)
	case bool:
		*words = append(*words, strconv.FormatBool(x))
	case map[string]any:
		for _, e := range x {
			collectWords(e, words)
		}
	case []any:
		for _, e := range x {
			collectWords(e, words)
		}
	}
}

func tokenize(s string) []string {
	return strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}
