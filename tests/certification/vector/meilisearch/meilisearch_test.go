//go:build certtests
// +build certtests

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

package meilisearch_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	commonmeilisearch "github.com/dapr/components-contrib/common/component/meilisearch"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/components-contrib/vector"
	vectormeilisearch "github.com/dapr/components-contrib/vector/meilisearch"
	"github.com/dapr/kit/logger"
)

func TestMeilisearchVectorCertification(t *testing.T) {
	host := os.Getenv("MEILISEARCH_HOST")
	if host == "" {
		t.Skip("MEILISEARCH_HOST is required for Meilisearch certification tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	component := vectormeilisearch.NewMeilisearch(logger.NewLogger("cert"))
	require.NoError(t, component.Init(ctx, vector.Metadata{Base: metadata.Base{Properties: map[string]string{"host": host, "apiKey": os.Getenv("MEILISEARCH_API_KEY")}}}))

	collection := "cert-vector-" + uuid.NewString()[:8]
	t.Cleanup(func() {
		_ = component.DropCollection(context.Background(), &vector.DropCollectionRequest{Collection: collection})
	})

	t.Run("auth_failure", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("auth failure path panicked: %v", r)
			}
		}()

		bad := vectormeilisearch.NewMeilisearch(logger.NewLogger("cert.bad"))
		badCtx, badCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer badCancel()
		err := bad.Init(badCtx, vector.Metadata{Base: metadata.Base{Properties: map[string]string{"host": host, "apiKey": "wrong-key"}}})
		if err == nil {
			_, err = bad.CreateCollection(badCtx, &vector.CreateCollectionRequest{Collection: "cert-vector-auth-" + uuid.NewString()[:8], Dimension: 4, Metric: vector.DistanceMetricCosine})
		}
		require.Error(t, err)
	})

	t.Run("component_metadata_contract", func(t *testing.T) {
		metadataProvider, ok := component.(interface{ GetComponentMetadata() metadata.MetadataMap })
		require.True(t, ok)
		assertMeilisearchMetadataContract(t, metadataProvider.GetComponentMetadata())
	})

	t.Run("unicode_content", func(t *testing.T) {
		unicodeCollection := "cert-vector-unicode-" + uuid.NewString()[:8]
		t.Cleanup(func() {
			_ = component.DropCollection(context.Background(), &vector.DropCollectionRequest{Collection: unicodeCollection})
		})
		_, err := component.CreateCollection(ctx, &vector.CreateCollectionRequest{Collection: unicodeCollection, Dimension: 4, Metric: vector.DistanceMetricCosine})
		require.NoError(t, err)

		want := map[string]any{"title_ko": "현대 소나타 🚗", "title_en": "Hyundai Sonata sedan 🚗", "title_th": "ฮุนได โซนาต้า รถเก๋ง 🚗", "emoji": "🚗✨"}
		vec := vector.Vec{ID: "unicode-vec-1", Values: []float32{1, 0, 0, 0}, Namespace: "unicode-ns", Metadata: want}
		upsertResp, err := component.Upsert(ctx, &vector.UpsertRequest{Collection: unicodeCollection, Ack: search.IndexAckDurable, Vectors: []vector.Vec{vec}})
		require.NoError(t, err)
		requireVectorResultsSucceeded(t, upsertResp.Results)

		getResp, err := component.Get(ctx, &vector.GetRequest{Collection: unicodeCollection, IDs: []string{vec.ID}, IncludeMetadata: true, IncludeValues: true})
		require.NoError(t, err)
		require.Len(t, getResp.Vectors, 1)
		require.Equal(t, vec.ID, getResp.Vectors[0].ID)
		require.Equal(t, vec.Namespace, getResp.Vectors[0].Namespace)
		require.Equal(t, want, getResp.Vectors[0].Metadata)
	})

	t.Run("high_dimensional", func(t *testing.T) {
		highCollection := "cert-vector-high-" + uuid.NewString()[:8]
		t.Cleanup(func() {
			_ = component.DropCollection(context.Background(), &vector.DropCollectionRequest{Collection: highCollection})
		})
		_, err := component.CreateCollection(ctx, &vector.CreateCollectionRequest{Collection: highCollection, Dimension: 1024, Metric: vector.DistanceMetricCosine, MetadataSchema: vectorCertificationSchema()})
		require.NoError(t, err)

		vectors := makeRandomVectors(50, 1024, "hyundai")
		upsertResp, err := component.Upsert(ctx, &vector.UpsertRequest{Collection: highCollection, Ack: search.IndexAckDurable, Vectors: vectors})
		require.NoError(t, err)
		requireVectorResultsSucceeded(t, upsertResp.Results)

		queryResp, err := component.Query(ctx, &vector.QueryRequest{Collection: highCollection, QueryVector: vectors[0].Values, TopK: 10, IncludeMetadata: true})
		require.NoError(t, err)
		require.Len(t, queryResp.Hits, 10)
	})

	t.Run("large_payload_batch", func(t *testing.T) {
		largeCollection := "cert-vector-large-" + uuid.NewString()[:8]
		t.Cleanup(func() {
			_ = component.DropCollection(context.Background(), &vector.DropCollectionRequest{Collection: largeCollection})
		})
		size := upsertLargestVectorBatch(t, ctx, component, largeCollection, 5000)
		if size < 5000 {
			t.Logf("meilisearch accepted %d vectors in the largest successful single-call batch below 5000", size)
		}
	})

	t.Run("soak_no_leak", func(t *testing.T) {
		soakCollection := "cert-vector-soak-" + uuid.NewString()[:8]
		t.Cleanup(func() {
			_ = component.DropCollection(context.Background(), &vector.DropCollectionRequest{Collection: soakCollection})
		})
		_, err := component.CreateCollection(ctx, &vector.CreateCollectionRequest{Collection: soakCollection, Dimension: 4, Metric: vector.DistanceMetricCosine})
		require.NoError(t, err)

		runtime.GC()
		baseline := runtime.NumGoroutine()
		for i := range 50 {
			vectors := make([]vector.Vec, 5)
			ids := make([]string, 5)
			for j := range vectors {
				id := fmt.Sprintf("soak-%03d-%03d", i, j)
				ids[j] = id
				vectors[j] = vector.Vec{ID: id, Values: []float32{float32(j), 1, 0, 0}, Metadata: map[string]any{"bodyStyle": "sedan", "ordinal": i*5 + j}}
			}
			upsertResp, err := component.Upsert(ctx, &vector.UpsertRequest{Collection: soakCollection, Ack: search.IndexAckDurable, Vectors: vectors})
			require.NoError(t, err)
			requireVectorResultsSucceeded(t, upsertResp.Results)
			deleteResp, err := component.Delete(ctx, &vector.DeleteRequest{Collection: soakCollection, Selector: vector.DeleteSelector{IDs: ids}, Ack: search.IndexAckDurable})
			require.NoError(t, err)
			requireVectorResultsSucceeded(t, deleteResp.Results)
		}
		runtime.GC()
		time.Sleep(200 * time.Millisecond)
		after := runtime.NumGoroutine()
		delta := after - baseline
		t.Logf("goroutines before=%d after=%d delta=%d", baseline, after, delta)
		require.LessOrEqual(t, delta, 20)
	})

	t.Run("create collection", func(t *testing.T) {
		_, err := component.CreateCollection(ctx, &vector.CreateCollectionRequest{Collection: collection, Dimension: 8, Metric: vector.DistanceMetricCosine, MetadataSchema: vectorCertificationSchema()})
		require.NoError(t, err)
	})

	t.Run("list", func(t *testing.T) {
		resp, err := component.ListCollections(ctx, &vector.ListCollectionsRequest{})
		require.NoError(t, err)
		require.Contains(t, resp.Collections, collection)
	})

	t.Run("describe", func(t *testing.T) {
		resp, err := component.DescribeCollection(ctx, &vector.DescribeCollectionRequest{Collection: collection})
		require.NoError(t, err)
		require.Equal(t, collection, resp.Collection)
		require.Equal(t, uint32(8), resp.Dimension)
	})

	t.Run("upsert durable", func(t *testing.T) {
		vectors := vectorCertificationVectors()
		resp, err := component.Upsert(ctx, &vector.UpsertRequest{Collection: collection, Ack: search.IndexAckDurable, Vectors: vectors})
		require.NoError(t, err)
		require.Len(t, resp.Results, len(vectors))
		for _, result := range resp.Results {
			require.True(t, result.Success, result.ErrorMessage)
		}
	})

	t.Run("get include combinations", func(t *testing.T) {
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
				resp, err := component.Get(ctx, &vector.GetRequest{Collection: collection, IDs: []string{"vec-000", "vec-001", "vec-002"}, IncludeValues: tt.includeValues, IncludeMetadata: tt.includeMetadata})
				require.NoError(t, err)
				require.Len(t, resp.Vectors, 3)
				if tt.includeValues {
					require.NotEmpty(t, resp.Vectors[0].Values)
				}
				if tt.includeMetadata {
					require.NotEmpty(t, resp.Vectors[0].Metadata)
				}
			})
		}
	})

	t.Run("query by vector topK filter", func(t *testing.T) {
		resp, err := component.Query(ctx, &vector.QueryRequest{Collection: collection, Namespace: "ns-a", QueryVector: certVectorValues(0), Filter: map[string]any{"make": map[string]any{"$eq": "hyundai"}}, TopK: 10, IncludeValues: true, IncludeMetadata: true})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Hits)
		require.LessOrEqual(t, len(resp.Hits), 10)
		for _, hit := range resp.Hits {
			require.Equal(t, "ns-a", hit.Vector.Namespace)
			require.Equal(t, "hyundai", hit.Vector.Metadata["make"])
		}
	})

	t.Run("query by id", func(t *testing.T) {
		resp, err := component.Query(ctx, &vector.QueryRequest{Collection: collection, QueryByID: "vec-000", TopK: 5, IncludeMetadata: true})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Hits)
	})

	t.Run("batch query with per query failure", func(t *testing.T) {
		resp, err := component.BatchQuery(ctx, &vector.BatchQueryRequest{Collection: collection, Queries: []vector.QueryRequest{
			{QueryVector: certVectorValues(0), TopK: 3},
			{QueryVector: certVectorValues(1), TopK: 3},
			{QueryVector: certVectorValues(2), TopK: 3},
			{QueryVector: certVectorValues(3), TopK: 3},
			{QueryVector: []float32{1, 2}, TopK: 3},
		}})
		require.NoError(t, err)
		require.Len(t, resp.Results, 5)
		for i := 0; i < 4; i++ {
			require.NotEmpty(t, resp.Results[i].Hits)
		}
		require.NotEmpty(t, resp.Results[4].ErrorCode)
		require.NotEmpty(t, resp.Results[4].ErrorMessage)
	})

	t.Run("delete by ids", func(t *testing.T) {
		ids := []string{"vec-000", "vec-002", "vec-004", "vec-006", "vec-008"}
		resp, err := component.Delete(ctx, &vector.DeleteRequest{Collection: collection, Selector: vector.DeleteSelector{IDs: ids}, Ack: search.IndexAckDurable})
		require.NoError(t, err)
		require.Len(t, resp.Results, len(ids))
	})

	t.Run("query returns fewer after delete", func(t *testing.T) {
		resp, err := component.Query(ctx, &vector.QueryRequest{Collection: collection, Namespace: "ns-a", QueryVector: certVectorValues(0), TopK: 50, IncludeMetadata: true})
		require.NoError(t, err)
		require.LessOrEqual(t, len(resp.Hits), 20)
	})

	t.Run("drop collection", func(t *testing.T) {
		require.NoError(t, component.DropCollection(ctx, &vector.DropCollectionRequest{Collection: collection}))
	})

	t.Run("close", func(t *testing.T) { require.NoError(t, component.Close()) })
}

func upsertLargestVectorBatch(t *testing.T, ctx context.Context, component vector.Vector, collection string, target int) int {
	t.Helper()

	for size := target; size >= 1; size /= 2 {
		_ = component.DropCollection(context.Background(), &vector.DropCollectionRequest{Collection: collection})
		_, err := component.CreateCollection(ctx, &vector.CreateCollectionRequest{Collection: collection, Dimension: 4, Metric: vector.DistanceMetricCosine})
		require.NoError(t, err)
		vectors := makeLargeVectors(size)
		resp, err := component.Upsert(ctx, &vector.UpsertRequest{Collection: collection, Ack: search.IndexAckDurable, Vectors: vectors})
		if err == nil && !hasFailedVectorResult(resp.Results) {
			return size
		}
		t.Logf("single-call vector batch size %d rejected: err=%v results=%#v", size, err, vectorResultSample(resp))
	}
	t.Fatal("no successful vector batch size found")
	return 0
}

func makeLargeVectors(size int) []vector.Vec {
	vectors := make([]vector.Vec, size)
	for i := range vectors {
		vectors[i] = vector.Vec{ID: fmt.Sprintf("large-vec-%05d", i), Values: []float32{float32(i % 7), float32(i % 11), float32(i % 13), 1}, Metadata: map[string]any{"bodyStyle": "sedan", "ordinal": i}}
	}
	return vectors
}

func makeRandomVectors(count, dimension int, prefix string) []vector.Vec {
	rng := rand.New(rand.NewSource(42))
	vectors := make([]vector.Vec, count)
	for i := range vectors {
		values := make([]float32, dimension)
		for j := range values {
			values[j] = rng.Float32()
		}
		vectors[i] = vector.Vec{ID: fmt.Sprintf("%s-vec-%03d", prefix, i), Values: values, Metadata: map[string]any{"make": prefix, "ordinal": i}}
	}
	return vectors
}

func vectorResultSample(resp *vector.UpsertResponse) []search.OperationResult {
	if resp == nil || len(resp.Results) == 0 {
		return nil
	}
	if len(resp.Results) < 3 {
		return resp.Results
	}
	return resp.Results[:3]
}

func requireVectorResultsSucceeded(t *testing.T, results []search.OperationResult) {
	t.Helper()
	require.NotEmpty(t, results)
	for _, result := range results {
		require.True(t, result.Success, result.ErrorMessage)
	}
}

func hasFailedVectorResult(results []search.OperationResult) bool {
	if len(results) == 0 {
		return true
	}
	for _, result := range results {
		if !result.Success {
			return true
		}
	}
	return false
}

func assertMeilisearchMetadataContract(t *testing.T, got metadata.MetadataMap) {
	t.Helper()

	actual := make(map[string]struct{}, len(got))
	for key := range got {
		actual[strings.ToLower(key)] = struct{}{}
	}

	mdType := reflect.TypeOf(commonmeilisearch.MeilisearchMetadata{})
	var missing []string
	for i := range mdType.NumField() {
		field := mdType.Field(i)
		if !field.IsExported() || field.Tag.Get("mapstructure") == "-" || strings.EqualFold(field.Tag.Get("mdignore"), "true") {
			continue
		}
		key := field.Name
		if tag := strings.Split(field.Tag.Get("mapstructure"), ",")[0]; tag != "" {
			key = tag
		}
		if _, ok := actual[strings.ToLower(key)]; !ok {
			missing = append(missing, key)
		}
	}
	require.Empty(t, missing, "component metadata missing fields: %s", strings.Join(missing, ", "))
}

func vectorCertificationSchema() []search.IndexFieldSchema {
	return []search.IndexFieldSchema{
		{Name: "namespace", Type: search.IndexFieldTypeKeyword, Filterable: true},
		{Name: "make", Type: search.IndexFieldTypeKeyword, Filterable: true},
		{Name: "bodyStyle", Type: search.IndexFieldTypeKeyword, Filterable: true},
		{Name: "ordinal", Type: search.IndexFieldTypeInt, Filterable: true, Sortable: true},
	}
}

func vectorCertificationVectors() []vector.Vec {
	vectors := make([]vector.Vec, 50)
	for i := range vectors {
		namespace := "ns-b"
		make := "toyota"
		if i%2 == 0 {
			namespace = "ns-a"
			make = "hyundai"
		}
		bodyStyles := []string{"sedan", "suv", "truck", "hatchback", "coupe"}
		vectors[i] = vector.Vec{
			ID:        fmt.Sprintf("vec-%03d", i),
			Values:    certVectorValues(i),
			Namespace: namespace,
			Metadata:  map[string]any{"make": make, "bodyStyle": bodyStyles[i%5], "ordinal": i},
		}
	}
	return vectors
}

func certVectorValues(seed int) []float32 {
	values := make([]float32, 8)
	values[seed%8] = 1
	values[(seed+1)%8] = float32(seed%5) / 10
	return values
}
