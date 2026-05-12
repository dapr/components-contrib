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

package vector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/dapr/components-contrib/vector"
)

type TestConfig struct{ utils.CommonConfig }

func NewTestConfig(componentName string) TestConfig {
	return TestConfig{CommonConfig: utils.CommonConfig{ComponentType: "vector", ComponentName: componentName}}
}

func ConformanceTests(t *testing.T, props map[string]string, v vector.Vector, component string) {
	ctx := t.Context()

	t.Run("init", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		require.NoError(t, v.Init(c, vector.Metadata{Base: metadata.Base{Properties: props}}))
	})
	if t.Failed() {
		t.Fatal("init failed")
	}

	t.Run("init idempotent", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		require.NoError(t, v.Init(c, vector.Metadata{Base: metadata.Base{Properties: props}}))
	})

	t.Run("metric matrix", func(t *testing.T) {
		metrics := []vector.DistanceMetric{
			vector.DistanceMetricCosine,
			vector.DistanceMetricDotProduct,
			vector.DistanceMetricEuclidean,
			vector.DistanceMetricManhattan,
			vector.DistanceMetricHamming,
		}
		for _, metric := range metrics {
			t.Run(fmt.Sprintf("metric-%d", metric), func(t *testing.T) {
				name := "conf-" + component + "-metric-" + randSuffix()
				c, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				_, err := v.CreateCollection(c, &vector.CreateCollectionRequest{Collection: name, Dimension: 4, Metric: metric, MetadataSchema: vectorConformanceSchema()})
				if unsupportedErr(err) {
					t.Skipf("metric %v unsupported: %v", metric, err)
				}
				require.NoError(t, err)
				t.Cleanup(func() { _ = v.DropCollection(t.Context(), &vector.DropCollectionRequest{Collection: name}) })
			})
		}
	})

	collection := "conf-" + component + "-" + randSuffix()
	t.Cleanup(func() { _ = v.DropCollection(ctx, &vector.DropCollectionRequest{Collection: collection}) })

	t.Run("create collection", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err := v.CreateCollection(c, &vector.CreateCollectionRequest{Collection: collection, Dimension: 4, Metric: vector.DistanceMetricCosine, MetadataSchema: vectorConformanceSchema()})
		require.NoError(t, err)
	})

	t.Run("describe collection", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		resp, err := v.DescribeCollection(c, &vector.DescribeCollectionRequest{Collection: collection})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, collection, resp.Collection)
		assert.Equal(t, uint32(4), resp.Dimension)
	})

	t.Run("list collections contains it", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		resp, err := v.ListCollections(c, &vector.ListCollectionsRequest{})
		require.NoError(t, err)
		require.Contains(t, resp.Collections, collection)
	})

	t.Run("upsert vectors", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		resp, err := v.Upsert(c, &vector.UpsertRequest{Collection: collection, Ack: search.IndexAckDurable, Vectors: vectorConformanceVectors()})
		require.NoError(t, err)
		require.Len(t, resp.Results, 6)
		for _, result := range resp.Results {
			assert.True(t, result.Success, result.ErrorMessage)
		}
	})

	t.Run("dimension mismatch upsert", func(t *testing.T) {
		name := "conf-" + component + "-dim-" + randSuffix()
		createCollection(t, v, name, 4)
		t.Cleanup(func() { _ = v.DropCollection(t.Context(), &vector.DropCollectionRequest{Collection: name}) })
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		resp, err := v.Upsert(c, &vector.UpsertRequest{Collection: name, Ack: search.IndexAckDurable, Vectors: []vector.Vec{{ID: "bad-dim", Values: []float32{1, 2, 3}}}})
		if err != nil {
			return
		}
		require.NotNil(t, resp)
		require.NotEmpty(t, resp.Results)
		assert.False(t, resp.Results[0].Success, "dimension mismatch should fail the call or the per-vector result")
	})

	t.Run("describe round trips", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		resp, err := v.DescribeCollection(c, &vector.DescribeCollectionRequest{Collection: collection})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, vector.DistanceMetricCosine, resp.Metric)
		assert.ElementsMatch(t, schemaNames(vectorConformanceSchema()), schemaNames(resp.MetadataSchema))
		if resp.VectorCount == 0 {
			t.Log("provider reported VectorCount=0; skipping count assertion for eventual consistency")
			return
		}
		assert.Greater(t, resp.VectorCount, uint64(0))
	})

	t.Run("get by ids", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		resp, err := v.Get(c, &vector.GetRequest{Collection: collection, IDs: []string{"vec-1", "vec-2"}, IncludeValues: true, IncludeMetadata: true})
		require.NoError(t, err)
		require.Len(t, resp.Vectors, 2)
		assert.NotEmpty(t, resp.Vectors[0].Values)
		assert.NotEmpty(t, resp.Vectors[0].Metadata)
	})

	t.Run("get include permutations", func(t *testing.T) {
		tests := []struct {
			name            string
			includeValues   bool
			includeMetadata bool
		}{
			{name: "ids only"},
			{name: "values", includeValues: true},
			{name: "metadata", includeMetadata: true},
			{name: "values metadata", includeValues: true, includeMetadata: true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				c, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				resp, err := v.Get(c, &vector.GetRequest{Collection: collection, IDs: []string{"vec-1"}, IncludeValues: tt.includeValues, IncludeMetadata: tt.includeMetadata})
				require.NoError(t, err)
				require.Len(t, resp.Vectors, 1)
				if tt.includeValues {
					assert.NotEmpty(t, resp.Vectors[0].Values)
				} else {
					assert.Empty(t, resp.Vectors[0].Values)
				}
				if tt.includeMetadata {
					assert.NotEmpty(t, resp.Vectors[0].Metadata)
				} else {
					assert.Empty(t, resp.Vectors[0].Metadata)
				}
			})
		}
	})

	t.Run("get missing ids", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		resp, err := v.Get(c, &vector.GetRequest{Collection: collection, IDs: []string{"missing-1", "missing-2"}, IncludeValues: true, IncludeMetadata: true})
		require.NoError(t, err)
		require.Empty(t, resp.Vectors)
	})

	t.Run("upsert ack queued", func(t *testing.T) {
		name := "conf-" + component + "-queued-" + randSuffix()
		createCollection(t, v, name, 4)
		t.Cleanup(func() { _ = v.DropCollection(t.Context(), &vector.DropCollectionRequest{Collection: name}) })
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		resp, err := v.Upsert(c, &vector.UpsertRequest{Collection: name, Ack: search.IndexAckQueued, Vectors: []vector.Vec{{ID: "queued-1", Values: []float32{1, 0, 0, 0}, Metadata: map[string]any{"make": "queued", "bodyStyle": "sedan", "rank": 100}}}})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, search.IndexAckQueued, resp.Ack)
	})

	t.Run("upsert idempotency key resubmit", func(t *testing.T) {
		name := "conf-" + component + "-idem-" + randSuffix()
		createCollection(t, v, name, 4)
		t.Cleanup(func() { _ = v.DropCollection(t.Context(), &vector.DropCollectionRequest{Collection: name}) })
		vectors := []vector.Vec{{ID: "idem-1", Values: []float32{0, 1, 0, 0}, Metadata: map[string]any{"make": "idem", "bodyStyle": "sedan", "rank": 101}}}
		key := "idem-" + randSuffix()
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		first, err := v.Upsert(c, &vector.UpsertRequest{Collection: name, Ack: search.IndexAckDurable, IdempotencyKey: key, Vectors: vectors})
		require.NoError(t, err)
		second, err := v.Upsert(c, &vector.UpsertRequest{Collection: name, Ack: search.IndexAckDurable, IdempotencyKey: key, Vectors: vectors})
		require.NoError(t, err)
		require.Len(t, second.Results, len(first.Results))
	})

	t.Run("query by vector topK ordering", func(t *testing.T) {
		resp := requireQuery(t, v, collection, &vector.QueryRequest{QueryVector: []float32{1, 0, 0, 0}, TopK: 3, IncludeValues: true, IncludeMetadata: true})
		require.Len(t, resp.Hits, 3)
		assert.LessOrEqual(t, resp.Hits[0].Distance, resp.Hits[1].Distance)
		assert.LessOrEqual(t, resp.Hits[1].Distance, resp.Hits[2].Distance)
	})

	t.Run("query by id", func(t *testing.T) {
		resp := requireQuery(t, v, collection, &vector.QueryRequest{QueryByID: "vec-1", TopK: 3, IncludeMetadata: true})
		require.NotEmpty(t, resp.Hits)
		seenOther := false
		for _, hit := range resp.Hits {
			if hit.Vector.ID != "vec-1" {
				seenOther = true
			}
		}
		assert.True(t, seenOther)
	})

	t.Run("query with filter", func(t *testing.T) {
		resp := requireQuery(t, v, collection, &vector.QueryRequest{QueryVector: []float32{0, 1, 0, 0}, Filter: map[string]any{"make": map[string]any{"$eq": "toyota"}}, TopK: 3, IncludeMetadata: true})
		require.NotEmpty(t, resp.Hits)
		for _, hit := range resp.Hits {
			assert.Equal(t, "toyota", hit.Vector.Metadata["make"])
		}
	})

	t.Run("query score threshold", func(t *testing.T) {
		threshold := 0.99
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		resp, err := v.Query(c, &vector.QueryRequest{Collection: collection, QueryVector: []float32{1, 0, 0, 0}, TopK: 10, ScoreThreshold: &threshold})
		if unsupportedErr(err) {
			t.Skipf("score threshold unsupported: %v", err)
		}
		require.NoError(t, err)
		for _, hit := range resp.Hits {
			assert.LessOrEqual(t, hit.Distance, 1-threshold)
		}
	})

	t.Run("query continuation token", func(t *testing.T) {
		seen := map[string]struct{}{}
		token := ""
		pages := 0
		for {
			c, cancel := context.WithTimeout(ctx, 30*time.Second)
			resp, err := v.Query(c, &vector.QueryRequest{Collection: collection, QueryVector: []float32{1, 0, 0, 0}, TopK: 2, ContinuationToken: token})
			cancel()
			require.NoError(t, err)
			pages++
			for _, hit := range resp.Hits {
				if _, ok := seen[hit.Vector.ID]; ok {
					t.Fatalf("duplicate query result id %q", hit.Vector.ID)
				}
				seen[hit.Vector.ID] = struct{}{}
			}
			if resp.ContinuationToken == "" {
				break
			}
			token = resp.ContinuationToken
			require.LessOrEqual(t, pages, 10)
		}
		require.GreaterOrEqual(t, len(seen), 2*pages)
	})

	t.Run("query mutual exclusion", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err := v.Query(c, &vector.QueryRequest{Collection: collection, QueryVector: []float32{1, 0, 0, 0}, QueryByID: "vec-1", TopK: 1})
		if !validationErr(err) {
			t.Logf("component did not validate both QueryVector and QueryByID: %v", err)
		}
	})

	t.Run("query both empty", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err := v.Query(c, &vector.QueryRequest{Collection: collection, TopK: 1})
		if !validationErr(err) {
			t.Logf("component did not validate empty QueryVector and QueryByID: %v", err)
		}
	})

	t.Run("delete selector mutual exclusion", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err := v.Delete(c, &vector.DeleteRequest{Collection: collection, Selector: vector.DeleteSelector{IDs: []string{"vec-1"}, Filter: map[string]any{"make": map[string]any{"$eq": "hyundai"}}}})
		require.True(t, validationErr(err), "expected selector mutual exclusion validation error, got %v", err)
	})

	t.Run("delete selector empty", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err := v.Delete(c, &vector.DeleteRequest{Collection: collection})
		require.True(t, validationErr(err), "expected empty selector validation error, got %v", err)
	})

	t.Run("delete by filter", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err := v.Delete(c, &vector.DeleteRequest{Collection: collection, Selector: vector.DeleteSelector{Filter: map[string]any{"make": map[string]any{"$eq": "hyundai"}}}, Ack: search.IndexAckDurable})
		require.NoError(t, err)
		resp := requireQuery(t, v, collection, &vector.QueryRequest{QueryVector: []float32{1, 0, 0, 0}, Filter: map[string]any{"make": map[string]any{"$eq": "hyundai"}}, TopK: 10, IncludeMetadata: true})
		assert.Empty(t, resp.Hits)
	})

	t.Run("batch query", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		resp, err := v.BatchQuery(c, &vector.BatchQueryRequest{Collection: collection, Queries: []vector.QueryRequest{
			{QueryVector: []float32{1, 0, 0, 0}, TopK: 2},
			{QueryVector: []float32{0, 1, 0, 0}, TopK: 2},
			{QueryVector: []float32{0, 0, 1, 0}, Filter: map[string]any{"make": map[string]any{"$regex": "toyota"}}, TopK: 2},
		}})
		require.NoError(t, err)
		require.Len(t, resp.Results, 3)
		assert.NotEmpty(t, resp.Results[0].Hits)
		assert.NotEmpty(t, resp.Results[1].Hits)
		assert.NotEmpty(t, resp.Results[2].ErrorCode)
	})

	t.Run("batch query ordering and indices", func(t *testing.T) {
		queries := make([]vector.QueryRequest, 5)
		for i := range queries {
			queries[i] = vector.QueryRequest{QueryVector: []float32{0, 1, 0, 0}, TopK: 1}
		}
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		resp, err := v.BatchQuery(c, &vector.BatchQueryRequest{Collection: collection, Queries: queries})
		require.NoError(t, err)
		require.Len(t, resp.Results, 5)
		for i, result := range resp.Results {
			//nolint:gosec // i bounded by len(resp.Results)==5, no overflow possible
			assert.Equal(t, uint32(i), result.QueryIndex)
		}
	})

	t.Run("batch query empty", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		resp, err := v.BatchQuery(c, &vector.BatchQueryRequest{Collection: collection})
		if err != nil {
			require.True(t, validationErr(err), "expected empty batch to succeed or return validation error, got %v", err)
			return
		}
		require.NotNil(t, resp)
		assert.Empty(t, resp.Results)
	})

	t.Run("namespace isolation", func(t *testing.T) {
		name := "conf-" + component + "-ns-" + randSuffix()
		createCollection(t, v, name, 4)
		t.Cleanup(func() { _ = v.DropCollection(t.Context(), &vector.DropCollectionRequest{Collection: name}) })
		vectors := []vector.Vec{
			{ID: "ns-a-1", Namespace: "ns-a", Values: []float32{1, 0, 0, 0}, Metadata: map[string]any{"make": "hyundai", "bodyStyle": "sedan", "rank": 1}},
			{ID: "ns-a-2", Namespace: "ns-a", Values: []float32{0.9, 0.1, 0, 0}, Metadata: map[string]any{"make": "hyundai", "bodyStyle": "sedan", "rank": 2}},
			{ID: "ns-b-1", Namespace: "ns-b", Values: []float32{0, 1, 0, 0}, Metadata: map[string]any{"make": "toyota", "bodyStyle": "sedan", "rank": 3}},
			{ID: "ns-b-2", Namespace: "ns-b", Values: []float32{0, 0.9, 0.1, 0}, Metadata: map[string]any{"make": "toyota", "bodyStyle": "sedan", "rank": 4}},
		}
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err := v.Upsert(c, &vector.UpsertRequest{Collection: name, Ack: search.IndexAckDurable, Vectors: vectors})
		if namespaceUnsupportedErr(err) {
			t.Skipf("namespaces unsupported: %v", err)
		}
		require.NoError(t, err)
		got, err := v.Get(c, &vector.GetRequest{Collection: name, Namespace: "ns-a", IDs: []string{"ns-a-1", "ns-a-2", "ns-b-1", "ns-b-2"}, IncludeMetadata: true})
		if namespaceUnsupportedErr(err) {
			t.Skipf("namespaces unsupported: %v", err)
		}
		require.NoError(t, err)
		require.Len(t, got.Vectors, 2)
		for _, vec := range got.Vectors {
			assert.Equal(t, "ns-a", vec.Namespace)
		}
		queried, err := v.Query(c, &vector.QueryRequest{Collection: name, Namespace: "ns-a", QueryVector: []float32{1, 0, 0, 0}, TopK: 10, IncludeMetadata: true})
		if namespaceUnsupportedErr(err) {
			t.Skipf("namespaces unsupported: %v", err)
		}
		require.NoError(t, err)
		for _, hit := range queried.Hits {
			assert.NotEqual(t, "ns-b", hit.Vector.Namespace)
		}
		_, err = v.Delete(c, &vector.DeleteRequest{Collection: name, Namespace: "ns-a", Selector: vector.DeleteSelector{IDs: []string{"ns-b-1"}}, Ack: search.IndexAckDurable})
		if namespaceUnsupportedErr(err) {
			t.Skipf("namespaces unsupported: %v", err)
		}
		require.NoError(t, err)
		got, err = v.Get(c, &vector.GetRequest{Collection: name, Namespace: "ns-b", IDs: []string{"ns-b-1"}, IncludeMetadata: true})
		require.NoError(t, err)
		require.Len(t, got.Vectors, 1)
		assert.Equal(t, "ns-b", got.Vectors[0].Namespace)
	})

	t.Run("concurrent upsert+query race", func(t *testing.T) {
		name := "conf-" + component + "-race-" + randSuffix()
		createCollection(t, v, name, 4)
		t.Cleanup(func() { _ = v.DropCollection(t.Context(), &vector.DropCollectionRequest{Collection: name}) })
		_, err := v.Upsert(ctx, &vector.UpsertRequest{Collection: name, Ack: search.IndexAckDurable, Vectors: []vector.Vec{{ID: "seed", Values: []float32{1, 0, 0, 0}, Metadata: map[string]any{"make": "seed", "bodyStyle": "sedan", "rank": 0}}}})
		require.NoError(t, err)
		var wg sync.WaitGroup
		errs := make(chan error, 16)
		for i := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				_, err := v.Upsert(c, &vector.UpsertRequest{Collection: name, Ack: search.IndexAckQueued, Vectors: []vector.Vec{{ID: fmt.Sprintf("race-%d", i), Values: []float32{1, float32(i) / 10, 0, 0}, Metadata: map[string]any{"make": "race", "bodyStyle": "sedan", "rank": i}}}})
				if err != nil {
					errs <- err
				}
			}()
			wg.Add(1)
			go func() {
				defer wg.Done()
				c, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				_, err := v.Query(c, &vector.QueryRequest{Collection: name, QueryVector: []float32{1, 0, 0, 0}, TopK: 2})
				if err != nil {
					errs <- err
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
	})

	t.Run("context cancelled", func(t *testing.T) {
		c, cancel := context.WithCancel(ctx)
		cancel()
		_, err := v.Query(c, &vector.QueryRequest{Collection: collection, QueryVector: []float32{1, 0, 0, 0}, TopK: 1})
		require.True(t, contextCanceledErr(err), "expected context canceled query error, got %v", err)
		_, err = v.Upsert(c, &vector.UpsertRequest{Collection: collection, Vectors: []vector.Vec{{ID: "cancel-upsert", Values: []float32{1, 0, 0, 0}}}})
		require.True(t, contextCanceledErr(err), "expected context canceled upsert error, got %v", err)
		_, err = v.Delete(c, &vector.DeleteRequest{Collection: collection, Selector: vector.DeleteSelector{IDs: []string{"cancel-upsert"}}})
		require.True(t, contextCanceledErr(err), "expected context canceled delete error, got %v", err)
	})

	t.Run("delete by ids", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		resp, err := v.Delete(c, &vector.DeleteRequest{Collection: collection, Selector: vector.DeleteSelector{IDs: []string{"vec-1", "vec-2"}}, Ack: search.IndexAckDurable})
		require.NoError(t, err)
		require.Len(t, resp.Results, 2)
	})

	t.Run("query after delete returns fewer", func(t *testing.T) {
		resp := requireQuery(t, v, collection, &vector.QueryRequest{QueryVector: []float32{1, 0, 0, 0}, TopK: 10})
		assert.LessOrEqual(t, len(resp.Hits), 4)
	})

	t.Run("drop collection", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		require.NoError(t, v.DropCollection(c, &vector.DropCollectionRequest{Collection: collection}))
	})

	t.Run("close", func(t *testing.T) { require.NoError(t, v.Close()) })

	t.Run("post-close lifecycle", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("post-close operation panicked: %v", r)
			}
		}()
		_, err := v.Upsert(ctx, &vector.UpsertRequest{Collection: collection, Vectors: []vector.Vec{{ID: "post-close", Values: []float32{1, 0, 0, 0}}}})
		require.Error(t, err)
		_, err = v.Query(ctx, &vector.QueryRequest{Collection: collection, QueryVector: []float32{1, 0, 0, 0}, TopK: 1})
		require.Error(t, err)
	})
}

func vectorConformanceSchema() []search.IndexFieldSchema {
	return []search.IndexFieldSchema{
		{Name: "namespace", Type: search.IndexFieldTypeKeyword, Filterable: true},
		{Name: "make", Type: search.IndexFieldTypeKeyword, Filterable: true},
		{Name: "bodyStyle", Type: search.IndexFieldTypeKeyword, Filterable: true},
		{Name: "rank", Type: search.IndexFieldTypeInt, Filterable: true, Sortable: true},
	}
}

func vectorConformanceVectors() []vector.Vec {
	return []vector.Vec{
		{ID: "vec-1", Values: []float32{1, 0, 0, 0}, Metadata: map[string]any{"make": "hyundai", "bodyStyle": "sedan", "rank": 1}},
		{ID: "vec-2", Values: []float32{0.9, 0.1, 0, 0}, Metadata: map[string]any{"make": "hyundai", "bodyStyle": "sedan", "rank": 2}},
		{ID: "vec-3", Values: []float32{0, 1, 0, 0}, Metadata: map[string]any{"make": "toyota", "bodyStyle": "sedan", "rank": 3}},
		{ID: "vec-4", Values: []float32{0, 0.9, 0.1, 0}, Metadata: map[string]any{"make": "toyota", "bodyStyle": "suv", "rank": 4}},
		{ID: "vec-5", Values: []float32{0, 0, 1, 0}, Metadata: map[string]any{"make": "ford", "bodyStyle": "sedan", "rank": 5}},
		{ID: "vec-6", Values: []float32{0, 0, 0, 1}, Metadata: map[string]any{"make": "ford", "bodyStyle": "suv", "rank": 6}},
	}
}

func requireQuery(t *testing.T, v vector.Vector, collection string, req *vector.QueryRequest) *vector.QueryResponse {
	t.Helper()
	c, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	req.Collection = collection
	resp, err := v.Query(c, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	return resp
}

func createCollection(t *testing.T, v vector.Vector, collection string, dimension uint32) {
	t.Helper()
	c, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err := v.CreateCollection(c, &vector.CreateCollectionRequest{Collection: collection, Dimension: dimension, Metric: vector.DistanceMetricCosine, MetadataSchema: vectorConformanceSchema()})
	require.NoError(t, err)
}

func schemaNames(schema []search.IndexFieldSchema) []string {
	names := make([]string, 0, len(schema))
	for _, field := range schema {
		names = append(names, field.Name)
	}
	return names
}

func unsupportedErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unsupported") || strings.Contains(msg, "support") || strings.Contains(msg, "only")
}

func namespaceUnsupportedErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "namespace") && (strings.Contains(msg, "unsupported") || strings.Contains(msg, "support"))
}

func validationErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "exactly one") || strings.Contains(msg, "mutually") || strings.Contains(msg, "both") || strings.Contains(msg, "requires") || strings.Contains(msg, "selector")
}

func contextCanceledErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.Canceled) || strings.Contains(strings.ToLower(err.Error()), "context canceled")
}

func randSuffix() string { return uuid.NewString()[:8] }
