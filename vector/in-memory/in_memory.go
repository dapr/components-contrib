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
	"maps"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/common/component/indexing"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/components-contrib/vector"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"
)

const defaultTopK = 10

var errClosed = status.Error(codes.FailedPrecondition, "the in-memory vector store is closed")

// Metadata is the in-memory vector store metadata.
type Metadata struct {
	// IndexingDelay queues every write and applies it once the delay has
	// passed, so that queued acknowledgements and wait timeouts can be
	// observed. A development and test aid.
	IndexingDelay time.Duration `mapstructure:"indexingDelay"`
}

type collection struct {
	dimensions uint32
	metric     vector.DistanceMetric
	records    map[string]vector.Record
}

type store struct {
	lock        sync.RWMutex
	collections map[string]*collection
	md          Metadata
}

// NewInMemoryVector returns a vector store that keeps every collection in
// process memory and scores queries by exhaustive search. It is meant for
// development and tests.
func NewInMemoryVector(logger.Logger) vector.Vector {
	return &store{collections: make(map[string]*collection)}
}

func (s *store) Init(_ context.Context, meta vector.Metadata) error {
	return kmeta.DecodeMetadata(meta.Properties, &s.md)
}

func (s *store) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(Metadata{}), &metadataInfo, metadata.VectorType)
	return
}

func (s *store) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.collections = nil
	return nil
}

// collection returns the named collection. The caller holds the lock.
func (s *store) collection(ctx context.Context, name string) (*collection, error) {
	if err := indexing.CtxErr(ctx); err != nil {
		return nil, err
	}
	if s.collections == nil {
		return nil, errClosed
	}
	c, ok := s.collections[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "collection %q not found", name)
	}
	return c, nil
}

func (s *store) CreateCollection(ctx context.Context, req *vector.CreateCollectionRequest) error {
	if req.Dimensions == 0 {
		return status.Error(codes.InvalidArgument, "dimensions must be positive")
	}
	metric := cmp.Or(req.Metric, vector.DistanceMetricCosine)
	if err := validMetric(metric); err != nil {
		return err
	}
	if err := indexing.CtxErr(ctx); err != nil {
		return err
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.collections == nil {
		return errClosed
	}
	if _, ok := s.collections[req.Collection]; ok {
		return status.Errorf(codes.AlreadyExists, "collection %q already exists", req.Collection)
	}
	s.collections[req.Collection] = &collection{dimensions: req.Dimensions, metric: metric, records: make(map[string]vector.Record)}
	return nil
}

func (s *store) GetCollection(ctx context.Context, req *vector.GetCollectionRequest) (*vector.GetCollectionResponse, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	c, err := s.collection(ctx, req.Collection)
	if err != nil {
		return nil, err
	}
	return &vector.GetCollectionResponse{
		Collection:  req.Collection,
		RecordCount: uint64(len(c.records)),
		Dimensions:  c.dimensions,
		Metric:      c.metric,
	}, nil
}

func (s *store) ListCollections(ctx context.Context, _ *vector.ListCollectionsRequest) (*vector.ListCollectionsResponse, error) {
	if err := indexing.CtxErr(ctx); err != nil {
		return nil, err
	}
	s.lock.RLock()
	defer s.lock.RUnlock()
	return &vector.ListCollectionsResponse{Collections: slices.Sorted(maps.Keys(s.collections))}, nil
}

func (s *store) DeleteCollection(ctx context.Context, req *vector.DeleteCollectionRequest) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, err := s.collection(ctx, req.Collection); err != nil {
		return err
	}
	delete(s.collections, req.Collection)
	return nil
}

func (s *store) Upsert(ctx context.Context, req *vector.UpsertRequest) (*vector.UpsertResponse, error) {
	ids := make([]string, len(req.Records))
	for i, r := range req.Records {
		ids[i] = r.ID
	}
	if err := search.ValidateWriteIDs(ids); err != nil {
		return nil, err
	}

	s.lock.RLock()
	c, err := s.collection(ctx, req.Collection)
	var dimensions int
	if err == nil {
		dimensions = int(c.dimensions)
	}
	s.lock.RUnlock()
	if err != nil {
		return nil, err
	}

	var failed []search.FailedItem
	records := make([]vector.Record, 0, len(req.Records))
	for _, r := range req.Records {
		if len(r.Values) != dimensions {
			failed = append(failed, search.FailedItem{ID: r.ID, Error: status.Newf(codes.InvalidArgument, "the record has %d dimensions, the collection has %d", len(r.Values), dimensions)})
			continue
		}
		records = append(records, r)
	}

	ack, err := indexing.Apply(ctx, req.Options, s.md.IndexingDelay, func() {
		s.lock.Lock()
		defer s.lock.Unlock()
		if c := s.collections[req.Collection]; c != nil {
			for _, r := range records {
				c.records[r.ID] = r
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return &vector.UpsertResponse{FailedItems: failed, Ack: ack}, nil
}

func (s *store) Get(ctx context.Context, req *vector.GetRequest) (*vector.GetResponse, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	c, err := s.collection(ctx, req.Collection)
	if err != nil {
		return nil, err
	}
	records := make([]vector.Record, 0, len(req.IDs))
	for _, id := range req.IDs {
		if r, ok := c.records[id]; ok {
			records = append(records, view(r, req.IncludeValues, true))
		}
	}
	return &vector.GetResponse{Records: records}, nil
}

func (s *store) Delete(ctx context.Context, req *vector.DeleteRequest) (*vector.DeleteResponse, error) {
	s.lock.RLock()
	_, err := s.collection(ctx, req.Collection)
	s.lock.RUnlock()
	if err != nil {
		return nil, err
	}
	ack, err := indexing.Apply(ctx, req.Options, s.md.IndexingDelay, func() {
		s.lock.Lock()
		defer s.lock.Unlock()
		if c := s.collections[req.Collection]; c != nil {
			for _, id := range req.IDs {
				delete(c.records, id)
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return &vector.DeleteResponse{Ack: ack}, nil
}

func (s *store) Query(ctx context.Context, req *vector.QueryRequest) (*vector.QueryResponse, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	c, err := s.collection(ctx, req.Collection)
	if err != nil {
		return nil, err
	}
	return c.query(req)
}

func (s *store) BatchQuery(ctx context.Context, req *vector.BatchQueryRequest) (*vector.BatchQueryResponse, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	c, err := s.collection(ctx, req.Collection)
	if err != nil {
		return nil, err
	}
	results := make([]vector.BatchQueryResult, len(req.Queries))
	for i := range req.Queries {
		resp, err := c.query(&req.Queries[i])
		results[i] = vector.BatchQueryResult{Response: resp, Error: err}
	}
	return &vector.BatchQueryResponse{Results: results}, nil
}

func (c *collection) query(req *vector.QueryRequest) (*vector.QueryResponse, error) {
	if (req.Vector != nil) == (req.ByID != "") {
		return nil, status.Error(codes.InvalidArgument, "exactly one of vector and by_id must be set")
	}
	metric := cmp.Or(req.Metric, c.metric)
	if err := validMetric(metric); err != nil {
		return nil, err
	}
	match, err := indexing.CompileFilter(req.Filter)
	if err != nil {
		return nil, err
	}

	var values []float32
	if req.ByID != "" {
		r, ok := c.records[req.ByID]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "record %q not found", req.ByID)
		}
		values = r.Values
	} else {
		values = req.Vector.Values
	}
	if len(values) != int(c.dimensions) {
		return nil, status.Errorf(codes.InvalidArgument, "the query vector has %d dimensions, the collection has %d", len(values), c.dimensions)
	}

	matches := make([]vector.Match, 0, len(c.records))
	for _, r := range c.records {
		if !match(r.Metadata) {
			continue
		}
		s := score(metric, values, r.Values)
		if req.ScoreThreshold != nil && !within(metric, s, *req.ScoreThreshold) {
			continue
		}
		matches = append(matches, vector.Match{Record: view(r, req.IncludeValues, req.IncludePayload), Score: s})
	}
	slices.SortFunc(matches, func(a, b vector.Match) int {
		c := cmp.Compare(a.Score, b.Score)
		if metric.HigherIsBetter() {
			c = -c
		}
		return cmp.Or(c, strings.Compare(a.Record.ID, b.Record.ID))
	})
	topK := cmp.Or(int(req.TopK), defaultTopK)
	return &vector.QueryResponse{Matches: matches[:min(topK, len(matches))], Metric: metric}, nil
}

func view(r vector.Record, includeValues, includePayload bool) vector.Record {
	out := vector.Record{ID: r.ID, Metadata: r.Metadata}
	if includeValues {
		out.Values = r.Values
	}
	if includePayload {
		out.Payload = r.Payload
	}
	return out
}

func validMetric(metric vector.DistanceMetric) error {
	if metric < vector.DistanceMetricCosine || metric > vector.DistanceMetricEuclidean {
		return status.Errorf(codes.InvalidArgument, "unknown distance metric %d", metric)
	}
	return nil
}

func score(metric vector.DistanceMetric, a, b []float32) float64 {
	var dot, normA, normB, dist float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		normA += x * x
		normB += y * y
		dist += (x - y) * (x - y)
	}
	switch metric {
	case vector.DistanceMetricDotProduct:
		return dot
	case vector.DistanceMetricEuclidean:
		return math.Sqrt(dist)
	default:
		if normA == 0 || normB == 0 {
			return 0
		}
		return dot / math.Sqrt(normA*normB)
	}
}

// within applies an inclusive score threshold in the metric's direction.
func within(metric vector.DistanceMetric, score, threshold float64) bool {
	if metric.HigherIsBetter() {
		return score >= threshold
	}
	return score <= threshold
}
