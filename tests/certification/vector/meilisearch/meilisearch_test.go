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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonmeilisearch "github.com/dapr/components-contrib/common/component/meilisearch"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/components-contrib/vector"
	vectormeilisearch "github.com/dapr/components-contrib/vector/meilisearch"
	"github.com/dapr/kit/logger"
)

const (
	certWaitTimeout = 2 * time.Minute
	// scoreEpsilon absorbs the float32 round-tripping every provider performs
	// on stored vectors when comparing scores against a threshold.
	scoreEpsilon = 1e-5
)

// certCollectionMetadata carries the component-specific collection settings.
// Record metadata is stored under the reserved daprMetadata attribute, which
// must be filterable for the portable filter DSL to apply to it.
func certCollectionMetadata() map[string]string {
	return map[string]string{
		"filterableAttributes": "daprMetadata",
	}
}

// certCreateRequest builds a cosine collection with typed dimensions.
func certCreateRequest(name string, dimensions uint32) *vector.CreateCollectionRequest {
	return &vector.CreateCollectionRequest{
		Collection: name,
		Metadata:   certCollectionMetadata(),
		Dimensions: dimensions,
		Metric:     vector.DistanceMetricCosine,
	}
}

func certWaitOptions() search.IndexingOptions {
	return search.IndexingOptions{
		Mode:          search.IndexingModeWaitForCompletion,
		WaitTimeout:   certWaitTimeout,
		OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest,
	}
}

//nolint:gocyclo,maintidx // A certification suite is intentionally a long linear list of scenarios.
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
		_ = component.DeleteCollection(context.Background(), &vector.DeleteCollectionRequest{Collection: collection})
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
			err = bad.CreateCollection(badCtx, certCreateRequest("cert-vector-auth-"+uuid.NewString()[:8], 4))
			requireStatusCode(t, err, codes.Unauthenticated, codes.PermissionDenied, codes.Internal)
			return
		}
		require.Error(t, err)
	})

	t.Run("component_metadata_contract", func(t *testing.T) {
		metadataProvider, ok := component.(interface{ GetComponentMetadata() metadata.MetadataMap })
		require.True(t, ok)
		assertMeilisearchMetadataContract(t, metadataProvider.GetComponentMetadata())
	})

	t.Run("create_collection_requires_dimensions", func(t *testing.T) {
		// The runtime validates dimensions > 0 before invoking the component;
		// the Meilisearch component applies the same rule defensively because
		// a user-provided embedder cannot be configured without a size.
		name := "cert-vector-nodim-" + uuid.NewString()[:8]
		t.Cleanup(func() {
			_ = component.DeleteCollection(context.Background(), &vector.DeleteCollectionRequest{Collection: name})
		})
		err := component.CreateCollection(ctx, &vector.CreateCollectionRequest{Collection: name, Metadata: certCollectionMetadata(), Metric: vector.DistanceMetricCosine})
		requireStatusCode(t, err, codes.InvalidArgument)
		_, err = component.GetCollection(ctx, &vector.GetCollectionRequest{Collection: name})
		requireStatusCode(t, err, codes.NotFound)
	})

	t.Run("non_cosine_metric_is_rejected", func(t *testing.T) {
		for _, metric := range []vector.DistanceMetric{vector.DistanceMetricDotProduct, vector.DistanceMetricEuclidean} {
			name := "cert-vector-metric-" + uuid.NewString()[:8]
			t.Cleanup(func() {
				_ = component.DeleteCollection(context.Background(), &vector.DeleteCollectionRequest{Collection: name})
			})
			err := component.CreateCollection(ctx, &vector.CreateCollectionRequest{Collection: name, Metadata: certCollectionMetadata(), Dimensions: 4, Metric: metric})
			requireStatusCode(t, err, codes.InvalidArgument)
			// Nothing was created.
			_, err = component.GetCollection(ctx, &vector.GetCollectionRequest{Collection: name})
			requireStatusCode(t, err, codes.NotFound)
		}
	})

	t.Run("unspecified_metric_defaults_to_cosine", func(t *testing.T) {
		name := "cert-vector-default-" + uuid.NewString()[:8]
		t.Cleanup(func() {
			_ = component.DeleteCollection(context.Background(), &vector.DeleteCollectionRequest{Collection: name})
		})
		require.NoError(t, component.CreateCollection(ctx, &vector.CreateCollectionRequest{Collection: name, Metadata: certCollectionMetadata(), Dimensions: 4}))
		got, err := component.GetCollection(ctx, &vector.GetCollectionRequest{Collection: name})
		require.NoError(t, err)
		require.Equal(t, vector.DistanceMetricCosine, got.Metric, "the effective metric is concrete and is the component default")
		require.Equal(t, uint32(4), got.Dimensions)
	})

	t.Run("unicode_payload_and_metadata", func(t *testing.T) {
		unicodeCollection := "cert-vector-unicode-" + uuid.NewString()[:8]
		t.Cleanup(func() {
			_ = component.DeleteCollection(context.Background(), &vector.DeleteCollectionRequest{Collection: unicodeCollection})
		})
		require.NoError(t, component.CreateCollection(ctx, certCreateRequest(unicodeCollection, 4)))

		payload := []byte(`{"title_ko":"오만과 편견 📚","title_th":"ความภาคภูมิใจและความอยุติธรรม 📚"}`)
		// Structured metadata keeps its JSON types: strings (including
		// non-ASCII), numbers, booleans and nested objects.
		wantMetadata := map[string]any{
			"author":    "pride-and-prejudice",
			"emoji":     "📚✨",
			"year":      2024.0,
			"electric":  true,
			"publisher": map[string]any{"city": "서울", "rating": 4.5},
		}
		record := vector.Record{ID: "unicode-vec-1", Values: []float32{1, 0, 0, 0}, Payload: payload, Metadata: wantMetadata}
		upsertResp, err := component.Upsert(ctx, &vector.UpsertRequest{Collection: unicodeCollection, Records: []vector.Record{record}, Options: certWaitOptions()})
		require.NoError(t, err)
		require.Empty(t, upsertResp.FailedItems)
		require.Equal(t, search.IndexAckCompleted, upsertResp.Ack)

		// Payload bytes and metadata round-trip exactly.
		getResp, err := component.Get(ctx, &vector.GetRequest{Collection: unicodeCollection, IDs: []string{record.ID}, IncludeValues: true})
		require.NoError(t, err)
		require.Len(t, getResp.Records, 1)
		require.Equal(t, record.ID, getResp.Records[0].ID)
		require.Equal(t, payload, getResp.Records[0].Payload)
		require.Equal(t, wantMetadata, getResp.Records[0].Metadata)
		require.Len(t, getResp.Records[0].Values, 4)

		// Nested metadata paths are filterable with dotted notation.
		queryResp, err := component.Query(ctx, &vector.QueryRequest{Collection: unicodeCollection, Vector: &vector.Record{Values: []float32{1, 0, 0, 0}}, Filter: map[string]any{"publisher.city": "서울", "electric": true}, TopK: 5})
		require.NoError(t, err)
		require.Len(t, queryResp.Matches, 1)
		require.Equal(t, record.ID, queryResp.Matches[0].Record.ID)
	})

	t.Run("keyed_upsert_validation", func(t *testing.T) {
		validationCollection := "cert-vector-keys-" + uuid.NewString()[:8]
		t.Cleanup(func() {
			_ = component.DeleteCollection(context.Background(), &vector.DeleteCollectionRequest{Collection: validationCollection})
		})
		require.NoError(t, component.CreateCollection(ctx, certCreateRequest(validationCollection, 4)))

		_, err := component.Upsert(ctx, &vector.UpsertRequest{Collection: validationCollection, Records: []vector.Record{{ID: "", Values: []float32{1, 0, 0, 0}}}, Options: certWaitOptions()})
		requireStatusCode(t, err, codes.InvalidArgument)

		_, err = component.Upsert(ctx, &vector.UpsertRequest{Collection: validationCollection, Records: []vector.Record{
			{ID: "dupe", Values: []float32{1, 0, 0, 0}},
			{ID: "dupe", Values: []float32{0, 1, 0, 0}},
		}, Options: certWaitOptions()})
		requireStatusCode(t, err, codes.InvalidArgument)
	})

	t.Run("failed_items_are_identified_before_enqueue", func(t *testing.T) {
		failCollection := "cert-vector-failed-" + uuid.NewString()[:8]
		t.Cleanup(func() {
			_ = component.DeleteCollection(context.Background(), &vector.DeleteCollectionRequest{Collection: failCollection})
		})
		require.NoError(t, component.CreateCollection(ctx, certCreateRequest(failCollection, 4)))

		resp, err := component.Upsert(ctx, &vector.UpsertRequest{Collection: failCollection, Records: []vector.Record{
			{ID: "good-1", Values: []float32{1, 0, 0, 0}},
			{ID: "bad-dim", Values: []float32{1, 0, 0}},
			{ID: "no-values"},
		}, Options: certWaitOptions()})
		if err != nil {
			requireStatusCode(t, err, codes.InvalidArgument)
			return
		}
		require.Equal(t, search.IndexAckCompleted, resp.Ack)
		failedIDs := make([]string, 0, len(resp.FailedItems))
		for _, item := range resp.FailedItems {
			failedIDs = append(failedIDs, item.ID)
			require.NotNil(t, item.Error)
			require.Equal(t, codes.InvalidArgument, item.Error.Code())
		}
		require.ElementsMatch(t, []string{"bad-dim", "no-values"}, failedIDs)

		got, err := component.Get(ctx, &vector.GetRequest{Collection: failCollection, IDs: []string{"good-1", "bad-dim", "no-values"}})
		require.NoError(t, err)
		require.Len(t, got.Records, 1)
		require.Equal(t, "good-1", got.Records[0].ID)
	})

	t.Run("indexing_modes", func(t *testing.T) {
		modeCollection := "cert-vector-modes-" + uuid.NewString()[:8]
		t.Cleanup(func() {
			_ = component.DeleteCollection(context.Background(), &vector.DeleteCollectionRequest{Collection: modeCollection})
		})
		require.NoError(t, component.CreateCollection(ctx, certCreateRequest(modeCollection, 4)))

		record := func(id string) []vector.Record {
			return []vector.Record{{ID: id, Values: []float32{1, 0, 0, 0}, Metadata: map[string]any{"author": "pride-and-prejudice", "price": 1.0, "inStock": true}}}
		}

		queued, err := component.Upsert(ctx, &vector.UpsertRequest{Collection: modeCollection, Records: record("mode-acceptance"), Options: search.IndexingOptions{Mode: search.IndexingModeReturnOnAcceptance}})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckQueued, queued.Ack)

		// INDEXING_MODE_UNSPECIFIED is identical to RETURN_ON_ACCEPTANCE.
		unspecified, err := component.Upsert(ctx, &vector.UpsertRequest{Collection: modeCollection, Records: record("mode-unspecified")})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckQueued, unspecified.Ack)

		completed, err := component.Upsert(ctx, &vector.UpsertRequest{Collection: modeCollection, Records: record("mode-completed"), Options: certWaitOptions()})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckCompleted, completed.Ack)

		async, err := component.Upsert(ctx, &vector.UpsertRequest{Collection: modeCollection, Records: record("mode-async"), Options: search.IndexingOptions{
			Mode:          search.IndexingModeWaitForCompletion,
			WaitTimeout:   certWaitTimeout,
			OnWaitTimeout: search.IndexingWaitTimeoutActionContinueAsync,
		}})
		require.NoError(t, err)
		require.Contains(t, []search.IndexAck{search.IndexAckQueued, search.IndexAckCompleted}, async.Ack)

		shortCtx, shortCancel := context.WithTimeout(ctx, 30*time.Second)
		defer shortCancel()
		_, err = component.Upsert(shortCtx, &vector.UpsertRequest{Collection: modeCollection, Records: record("mode-short"), Options: search.IndexingOptions{
			Mode:          search.IndexingModeWaitForCompletion,
			WaitTimeout:   time.Nanosecond,
			OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest,
		}})
		if err != nil {
			requireStatusCode(t, err, codes.DeadlineExceeded)
		}

		_, err = component.Upsert(ctx, &vector.UpsertRequest{Collection: modeCollection, Records: record("mode-invalid"), Options: search.IndexingOptions{
			Mode:          search.IndexingModeWaitForCompletion,
			OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest,
		}})
		requireStatusCode(t, err, codes.InvalidArgument)
	})

	t.Run("delete_modes", func(t *testing.T) {
		deleteCollection := "cert-vector-delete-" + uuid.NewString()[:8]
		t.Cleanup(func() {
			_ = component.DeleteCollection(context.Background(), &vector.DeleteCollectionRequest{Collection: deleteCollection})
		})
		require.NoError(t, component.CreateCollection(ctx, certCreateRequest(deleteCollection, 4)))

		ids := []string{"del-acceptance", "del-unspecified", "del-completed", "del-async"}
		records := make([]vector.Record, 0, len(ids))
		for i, id := range ids {
			records = append(records, vector.Record{ID: id, Values: []float32{float32(i + 1), 1, 0, 0}, Metadata: map[string]any{"author": "pride-and-prejudice"}})
		}
		seeded, err := component.Upsert(ctx, &vector.UpsertRequest{Collection: deleteCollection, Records: records, Options: certWaitOptions()})
		require.NoError(t, err)
		require.Empty(t, seeded.FailedItems)

		// Deletes are writes: they take the same IndexingOptions and report
		// the same acknowledgement boundaries as Upsert.
		queued, err := component.Delete(ctx, &vector.DeleteRequest{Collection: deleteCollection, IDs: []string{"del-acceptance"}, Options: search.IndexingOptions{Mode: search.IndexingModeReturnOnAcceptance}})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckQueued, queued.Ack)

		unspecified, err := component.Delete(ctx, &vector.DeleteRequest{Collection: deleteCollection, IDs: []string{"del-unspecified"}})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckQueued, unspecified.Ack)

		completed, err := component.Delete(ctx, &vector.DeleteRequest{Collection: deleteCollection, IDs: []string{"del-completed"}, Options: certWaitOptions()})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckCompleted, completed.Ack)

		async, err := component.Delete(ctx, &vector.DeleteRequest{Collection: deleteCollection, IDs: []string{"del-async"}, Options: search.IndexingOptions{
			Mode:          search.IndexingModeWaitForCompletion,
			WaitTimeout:   certWaitTimeout,
			OnWaitTimeout: search.IndexingWaitTimeoutActionContinueAsync,
		}})
		require.NoError(t, err)
		require.Contains(t, []search.IndexAck{search.IndexAckQueued, search.IndexAckCompleted}, async.Ack)

		// Wait options are validated for deletes too.
		_, err = component.Delete(ctx, &vector.DeleteRequest{Collection: deleteCollection, IDs: []string{"del-invalid"}, Options: search.IndexingOptions{
			Mode:        search.IndexingModeReturnOnAcceptance,
			WaitTimeout: time.Second,
		}})
		requireStatusCode(t, err, codes.InvalidArgument)

		// Missing ids are not failures; a settled delete of the whole set
		// confirms everything is gone.
		settled, err := component.Delete(ctx, &vector.DeleteRequest{Collection: deleteCollection, IDs: append(ids, "never-existed"), Options: certWaitOptions()})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckCompleted, settled.Ack)
		got, err := component.Get(ctx, &vector.GetRequest{Collection: deleteCollection, IDs: ids})
		require.NoError(t, err)
		require.Empty(t, got.Records)
	})

	t.Run("high_dimensional", func(t *testing.T) {
		highCollection := "cert-vector-high-" + uuid.NewString()[:8]
		t.Cleanup(func() {
			_ = component.DeleteCollection(context.Background(), &vector.DeleteCollectionRequest{Collection: highCollection})
		})
		require.NoError(t, component.CreateCollection(ctx, certCreateRequest(highCollection, 1024)))

		records := makeRandomRecords(50, 1024, "pride-and-prejudice")
		upsertResp, err := component.Upsert(ctx, &vector.UpsertRequest{Collection: highCollection, Records: records, Options: certWaitOptions()})
		require.NoError(t, err)
		require.Empty(t, upsertResp.FailedItems)

		got, err := component.GetCollection(ctx, &vector.GetCollectionRequest{Collection: highCollection})
		require.NoError(t, err)
		require.Equal(t, uint32(1024), got.Dimensions)

		queryResp, err := component.Query(ctx, &vector.QueryRequest{Collection: highCollection, Vector: &vector.Record{Values: records[0].Values}, TopK: 10})
		require.NoError(t, err)
		require.Len(t, queryResp.Matches, 10)
		require.Equal(t, vector.DistanceMetricCosine, queryResp.Metric)
	})

	t.Run("large_payload_batch", func(t *testing.T) {
		largeCollection := "cert-vector-large-" + uuid.NewString()[:8]
		t.Cleanup(func() {
			_ = component.DeleteCollection(context.Background(), &vector.DeleteCollectionRequest{Collection: largeCollection})
		})
		size := upsertLargestVectorBatch(t, ctx, component, largeCollection, 5000)
		if size < 5000 {
			t.Logf("meilisearch accepted %d vectors in the largest successful single-call batch below 5000", size)
		}
	})

	t.Run("soak_no_leak", func(t *testing.T) {
		soakCollection := "cert-vector-soak-" + uuid.NewString()[:8]
		t.Cleanup(func() {
			_ = component.DeleteCollection(context.Background(), &vector.DeleteCollectionRequest{Collection: soakCollection})
		})
		require.NoError(t, component.CreateCollection(ctx, certCreateRequest(soakCollection, 4)))

		runtime.GC()
		baseline := runtime.NumGoroutine()
		for i := range 50 {
			records := make([]vector.Record, 5)
			ids := make([]string, 5)
			for j := range records {
				id := fmt.Sprintf("soak-%03d-%03d", i, j)
				ids[j] = id
				records[j] = vector.Record{ID: id, Values: []float32{float32(j), 1, 0, 0}, Metadata: map[string]any{"genre": "fiction", "ordinal": float64(i*5 + j)}}
			}
			upsertResp, err := component.Upsert(ctx, &vector.UpsertRequest{Collection: soakCollection, Records: records, Options: certWaitOptions()})
			require.NoError(t, err)
			require.Empty(t, upsertResp.FailedItems)
			deleteResp, err := component.Delete(ctx, &vector.DeleteRequest{Collection: soakCollection, IDs: ids, Options: certWaitOptions()})
			require.NoError(t, err)
			require.Equal(t, search.IndexAckCompleted, deleteResp.Ack)
		}
		runtime.GC()
		time.Sleep(200 * time.Millisecond)
		after := runtime.NumGoroutine()
		delta := after - baseline
		t.Logf("goroutines before=%d after=%d delta=%d", baseline, after, delta)
		require.LessOrEqual(t, delta, 20)
	})

	t.Run("create collection", func(t *testing.T) {
		require.NoError(t, component.CreateCollection(ctx, certCreateRequest(collection, 8)))
	})

	t.Run("create existing collection is ALREADY_EXISTS", func(t *testing.T) {
		err := component.CreateCollection(ctx, certCreateRequest(collection, 8))
		requireStatusCode(t, err, codes.AlreadyExists)

		// Settings are not reconciled: different dimensions on the same name
		// are still ALREADY_EXISTS and the original collection is unchanged.
		err = component.CreateCollection(ctx, certCreateRequest(collection, 16))
		requireStatusCode(t, err, codes.AlreadyExists)
		got, err := component.GetCollection(ctx, &vector.GetCollectionRequest{Collection: collection})
		require.NoError(t, err)
		require.Equal(t, uint32(8), got.Dimensions)
	})

	t.Run("list", func(t *testing.T) {
		resp, err := component.ListCollections(ctx, &vector.ListCollectionsRequest{})
		require.NoError(t, err)
		require.Contains(t, resp.Collections, collection)
	})

	t.Run("get collection", func(t *testing.T) {
		resp, err := component.GetCollection(ctx, &vector.GetCollectionRequest{Collection: collection})
		require.NoError(t, err)
		require.Equal(t, collection, resp.Collection)
		require.Equal(t, uint32(8), resp.Dimensions)
		require.Equal(t, vector.DistanceMetricCosine, resp.Metric)
	})

	t.Run("get collection missing", func(t *testing.T) {
		_, err := component.GetCollection(ctx, &vector.GetCollectionRequest{Collection: "cert-vector-missing-" + uuid.NewString()[:8]})
		requireStatusCode(t, err, codes.NotFound)
	})

	t.Run("delete collection missing", func(t *testing.T) {
		err := component.DeleteCollection(ctx, &vector.DeleteCollectionRequest{Collection: "cert-vector-missing-" + uuid.NewString()[:8]})
		requireStatusCode(t, err, codes.NotFound)
	})

	t.Run("upsert", func(t *testing.T) {
		resp, err := component.Upsert(ctx, &vector.UpsertRequest{Collection: collection, Records: vectorCertificationRecords(), Options: certWaitOptions()})
		require.NoError(t, err)
		require.Empty(t, resp.FailedItems)
		require.Equal(t, search.IndexAckCompleted, resp.Ack)
	})

	t.Run("get include combinations", func(t *testing.T) {
		tests := []struct {
			name          string
			includeValues bool
		}{
			{name: "ids only"},
			{name: "values", includeValues: true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp, err := component.Get(ctx, &vector.GetRequest{Collection: collection, IDs: []string{"vec-000", "vec-001", "vec-002"}, IncludeValues: tt.includeValues})
				require.NoError(t, err)
				require.Len(t, resp.Records, 3)
				for _, record := range resp.Records {
					if tt.includeValues {
						require.Len(t, record.Values, 8)
						continue
					}
					require.Empty(t, record.Values)
				}
			})
		}
	})

	t.Run("get returns records in request order and omits missing ids", func(t *testing.T) {
		resp, err := component.Get(ctx, &vector.GetRequest{Collection: collection, IDs: []string{"vec-002", "missing-vec", "vec-000"}})
		require.NoError(t, err)
		require.Len(t, resp.Records, 2)
		require.Equal(t, "vec-002", resp.Records[0].ID)
		require.Equal(t, "vec-000", resp.Records[1].ID)
	})

	t.Run("query reports the effective metric", func(t *testing.T) {
		resp, err := component.Query(ctx, &vector.QueryRequest{Collection: collection, Vector: &vector.Record{Values: certVectorValues(0)}, TopK: 5, IncludePayload: true})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Matches)
		// The collection metric is reported even though the request used
		// DISTANCE_METRIC_UNSPECIFIED.
		require.Equal(t, vector.DistanceMetricCosine, resp.Metric)
		for i := 1; i < len(resp.Matches); i++ {
			require.GreaterOrEqual(t, resp.Matches[i-1].Score, resp.Matches[i].Score, "cosine similarity is higher-is-better")
		}
		for _, match := range resp.Matches {
			// Cosine similarity lives in [-1, 1]; the reported score is the
			// unnormalized metric value, not the provider ranking score.
			require.GreaterOrEqual(t, match.Score, -1.0-scoreEpsilon)
			require.LessOrEqual(t, match.Score, 1.0+scoreEpsilon)
		}
	})

	t.Run("query with an unsupported metric", func(t *testing.T) {
		for _, metric := range []vector.DistanceMetric{vector.DistanceMetricDotProduct, vector.DistanceMetricEuclidean} {
			_, err := component.Query(ctx, &vector.QueryRequest{Collection: collection, Vector: &vector.Record{Values: certVectorValues(0)}, TopK: 5, Metric: metric})
			requireStatusCode(t, err, codes.InvalidArgument)
		}
	})

	t.Run("query requires exactly one of vector and by_id", func(t *testing.T) {
		_, err := component.Query(ctx, &vector.QueryRequest{Collection: collection, TopK: 5})
		requireStatusCode(t, err, codes.InvalidArgument)

		_, err = component.Query(ctx, &vector.QueryRequest{Collection: collection, Vector: &vector.Record{Values: certVectorValues(0)}, ByID: "vec-000", TopK: 5})
		requireStatusCode(t, err, codes.InvalidArgument)
	})

	t.Run("score_threshold is inclusive", func(t *testing.T) {
		baseline, err := component.Query(ctx, &vector.QueryRequest{Collection: collection, Vector: &vector.Record{Values: certVectorValues(0)}, TopK: 3, Metric: vector.DistanceMetricCosine})
		require.NoError(t, err)
		require.Len(t, baseline.Matches, 3)
		boundary := baseline.Matches[len(baseline.Matches)-1]

		threshold := boundary.Score
		filtered, err := component.Query(ctx, &vector.QueryRequest{Collection: collection, Vector: &vector.Record{Values: certVectorValues(0)}, TopK: 20, Metric: vector.DistanceMetricCosine, ScoreThreshold: &threshold})
		require.NoError(t, err)
		require.Equal(t, vector.DistanceMetricCosine, filtered.Metric)

		var retained bool
		for _, match := range filtered.Matches {
			if match.Record.ID == boundary.Record.ID {
				retained = true
			}
			require.GreaterOrEqual(t, match.Score, threshold-scoreEpsilon)
		}
		require.True(t, retained, "score_threshold is inclusive, so the boundary match is retained")
	})

	t.Run("query with filter", func(t *testing.T) {
		// Every record has: author (pride-and-prejudice for even ordinals, frankenstein for odd),
		// genre (one of five), price (ordinal * 1000) and inStock
		// (ordinal % 3 == 0). Filters address the structured metadata with
		// its JSON types.
		tests := []struct {
			name     string
			filter   map[string]any
			check    func(t *testing.T, md map[string]any)
			wantCode codes.Code
		}{
			{name: "eq string", filter: map[string]any{"author": map[string]any{"$eq": "pride-and-prejudice"}}, check: func(t *testing.T, md map[string]any) { assert.Equal(t, "pride-and-prejudice", md["author"]) }},
			{name: "bare eq shorthand", filter: map[string]any{"author": "frankenstein"}, check: func(t *testing.T, md map[string]any) { assert.Equal(t, "frankenstein", md["author"]) }},
			{name: "ne string", filter: map[string]any{"author": map[string]any{"$ne": "pride-and-prejudice"}}, check: func(t *testing.T, md map[string]any) { assert.Equal(t, "frankenstein", md["author"]) }},
			{name: "gt number", filter: map[string]any{"price": map[string]any{"$gt": 40000.0}}, check: func(t *testing.T, md map[string]any) { assert.Greater(t, md["price"], 40000.0) }},
			{name: "gte number", filter: map[string]any{"price": map[string]any{"$gte": 49000.0}}, check: func(t *testing.T, md map[string]any) { assert.GreaterOrEqual(t, md["price"], 49000.0) }},
			{name: "lt number", filter: map[string]any{"price": map[string]any{"$lt": 3000.0}}, check: func(t *testing.T, md map[string]any) { assert.Less(t, md["price"], 3000.0) }},
			{name: "lte number", filter: map[string]any{"price": map[string]any{"$lte": 0.0}}, check: func(t *testing.T, md map[string]any) { assert.LessOrEqual(t, md["price"], 0.0) }},
			{name: "numeric range", filter: map[string]any{"price": map[string]any{"$gte": 10000.0, "$lt": 20000.0}}, check: func(t *testing.T, md map[string]any) {
				assert.GreaterOrEqual(t, md["price"], 10000.0)
				assert.Less(t, md["price"], 20000.0)
			}},
			{name: "in strings", filter: map[string]any{"genre": map[string]any{"$in": []any{"fiction", "poetry"}}}, check: func(t *testing.T, md map[string]any) { assert.Contains(t, []any{"fiction", "poetry"}, md["genre"]) }},
			{name: "nin strings", filter: map[string]any{"genre": map[string]any{"$nin": []any{"fiction", "poetry", "drama", "essay"}}}, check: func(t *testing.T, md map[string]any) { assert.Equal(t, "novel", md["genre"]) }},
			{name: "in numbers", filter: map[string]any{"price": map[string]any{"$in": []any{1000.0, 2000.0}}}, check: func(t *testing.T, md map[string]any) { assert.Contains(t, []any{1000.0, 2000.0}, md["price"]) }},
			{name: "bool eq", filter: map[string]any{"inStock": map[string]any{"$eq": true}}, check: func(t *testing.T, md map[string]any) { assert.Equal(t, true, md["inStock"]) }},
			{name: "bare bool shorthand", filter: map[string]any{"inStock": false}, check: func(t *testing.T, md map[string]any) { assert.Equal(t, false, md["inStock"]) }},
			{name: "exists", filter: map[string]any{"promo": map[string]any{"$exists": true}}, check: func(t *testing.T, md map[string]any) { assert.Contains(t, md, "promo") }},
			{name: "and", filter: map[string]any{"$and": []any{map[string]any{"author": "pride-and-prejudice"}, map[string]any{"genre": "fiction"}}}, check: func(t *testing.T, md map[string]any) {
				assert.Equal(t, "pride-and-prejudice", md["author"])
				assert.Equal(t, "fiction", md["genre"])
			}},
			{name: "or", filter: map[string]any{"$or": []any{map[string]any{"price": map[string]any{"$lt": 1000.0}}, map[string]any{"price": map[string]any{"$gt": 48000.0}}}}, check: func(t *testing.T, md map[string]any) {
				price := md["price"].(float64) //nolint:forcetypeassert // metadata is generated by this test
				assert.True(t, price < 1000 || price > 48000, "price %v outside both branches", price)
			}},
			{name: "not", filter: map[string]any{"$not": map[string]any{"author": "pride-and-prejudice"}}, check: func(t *testing.T, md map[string]any) { assert.Equal(t, "frankenstein", md["author"]) }},
			{name: "nested logical", filter: map[string]any{"$and": []any{
				map[string]any{"$or": []any{map[string]any{"genre": "novel"}, map[string]any{"genre": "fiction"}}},
				map[string]any{"$not": map[string]any{"inStock": true}},
			}}, check: func(t *testing.T, md map[string]any) {
				assert.Contains(t, []any{"novel", "fiction"}, md["genre"])
				assert.Equal(t, false, md["inStock"])
			}},
			{name: "regex unsupported", filter: map[string]any{"author": map[string]any{"$regex": "hyun.*"}}, wantCode: codes.InvalidArgument},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// A uniform query vector is similar to every record, so the
				// filter alone determines the result set.
				resp, err := component.Query(ctx, &vector.QueryRequest{Collection: collection, Vector: &vector.Record{Values: certUniformValues()}, Filter: tt.filter, TopK: 50, IncludeValues: true, IncludePayload: true})
				if tt.wantCode != codes.OK {
					requireStatusCode(t, err, tt.wantCode)
					return
				}
				require.NoError(t, err)
				require.NotEmpty(t, resp.Matches)
				for _, match := range resp.Matches {
					tt.check(t, match.Record.Metadata)
				}
			})
		}
	})

	t.Run("query by id", func(t *testing.T) {
		resp, err := component.Query(ctx, &vector.QueryRequest{Collection: collection, ByID: "vec-000", TopK: 5})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Matches)
		require.Equal(t, vector.DistanceMetricCosine, resp.Metric)
	})

	t.Run("batch query preserves request order", func(t *testing.T) {
		queries := []vector.QueryRequest{
			{Vector: &vector.Record{Values: certVectorValues(0)}, TopK: 1},
			{Vector: &vector.Record{Values: certVectorValues(1)}, TopK: 1},
			{Vector: &vector.Record{Values: certVectorValues(2)}, TopK: 1},
			{Vector: &vector.Record{Values: certVectorValues(3)}, TopK: 3},
		}
		resp, err := component.BatchQuery(ctx, &vector.BatchQueryRequest{Collection: collection, Queries: queries})
		require.NoError(t, err)
		require.Len(t, resp.Results, len(queries))
		for i, result := range resp.Results {
			require.NoError(t, result.Error)
			require.NotNil(t, result.Response)
			require.Equal(t, vector.DistanceMetricCosine, result.Response.Metric)
			require.LessOrEqual(t, len(result.Response.Matches), int(queries[i].TopK))
		}
		require.Equal(t, "vec-000", resp.Results[0].Response.Matches[0].Record.ID)
	})

	t.Run("a failing batch query is a per-query error", func(t *testing.T) {
		tests := []struct {
			name     string
			query    vector.QueryRequest
			wantCode []codes.Code
		}{
			{name: "neither vector nor by_id", query: vector.QueryRequest{TopK: 3}, wantCode: []codes.Code{codes.InvalidArgument}},
			{name: "both vector and by_id", query: vector.QueryRequest{Vector: &vector.Record{Values: certVectorValues(0)}, ByID: "vec-000", TopK: 3}, wantCode: []codes.Code{codes.InvalidArgument}},
			{name: "unsupported metric", query: vector.QueryRequest{Vector: &vector.Record{Values: certVectorValues(0)}, TopK: 3, Metric: vector.DistanceMetricEuclidean}, wantCode: []codes.Code{codes.InvalidArgument}},
			{name: "unsupported filter operator", query: vector.QueryRequest{Vector: &vector.Record{Values: certVectorValues(0)}, TopK: 3, Filter: map[string]any{"author": map[string]any{"$regex": "x"}}}, wantCode: []codes.Code{codes.InvalidArgument}},
			{name: "dimension mismatch rejected by the provider", query: vector.QueryRequest{Vector: &vector.Record{Values: []float32{1, 2}}, TopK: 3}, wantCode: []codes.Code{codes.InvalidArgument, codes.FailedPrecondition, codes.Internal}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp, err := component.BatchQuery(ctx, &vector.BatchQueryRequest{Collection: collection, Queries: []vector.QueryRequest{
					{Vector: &vector.Record{Values: certVectorValues(0)}, TopK: 3},
					tt.query,
					{Vector: &vector.Record{Values: certVectorValues(1)}, TopK: 3},
				}})
				require.NoError(t, err, "a failing query must not fail the RPC")
				require.Len(t, resp.Results, 3)

				failed := resp.Results[1]
				require.Error(t, failed.Error)
				assert.Nil(t, failed.Response, "exactly one of Response or Error is set")
				st, ok := status.FromError(failed.Error)
				require.True(t, ok, "per-query errors must be gRPC status errors, got %T: %v", failed.Error, failed.Error)
				assert.Contains(t, tt.wantCode, st.Code(), "unexpected per-query status code %s: %v", st.Code(), failed.Error)

				for _, i := range []int{0, 2} {
					require.NoError(t, resp.Results[i].Error, "result %d must succeed", i)
					require.NotNil(t, resp.Results[i].Response)
					require.NotEmpty(t, resp.Results[i].Response.Matches)
				}
				require.Equal(t, "vec-000", resp.Results[0].Response.Matches[0].Record.ID)
			})
		}
	})

	t.Run("batch query against a missing collection fails the RPC", func(t *testing.T) {
		_, err := component.BatchQuery(ctx, &vector.BatchQueryRequest{Collection: "cert-vector-missing-" + uuid.NewString()[:8], Queries: []vector.QueryRequest{
			{Vector: &vector.Record{Values: certVectorValues(0)}, TopK: 3},
		}})
		requireStatusCode(t, err, codes.NotFound)
	})

	t.Run("delete by ids", func(t *testing.T) {
		ids := []string{"vec-000", "vec-002", "vec-004", "vec-006", "vec-008"}
		resp, err := component.Delete(ctx, &vector.DeleteRequest{Collection: collection, IDs: ids, Options: certWaitOptions()})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckCompleted, resp.Ack)

		got, err := component.Get(ctx, &vector.GetRequest{Collection: collection, IDs: ids})
		require.NoError(t, err)
		require.Empty(t, got.Records)
	})

	t.Run("delete is idempotent", func(t *testing.T) {
		resp, err := component.Delete(ctx, &vector.DeleteRequest{Collection: collection, IDs: []string{"vec-000", "never-existed"}, Options: certWaitOptions()})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckCompleted, resp.Ack)
	})

	t.Run("query returns fewer after delete", func(t *testing.T) {
		resp, err := component.Query(ctx, &vector.QueryRequest{Collection: collection, Vector: &vector.Record{Values: certVectorValues(0)}, TopK: 50})
		require.NoError(t, err)
		require.LessOrEqual(t, len(resp.Matches), 45)
	})

	t.Run("delete collection", func(t *testing.T) {
		require.NoError(t, component.DeleteCollection(ctx, &vector.DeleteCollectionRequest{Collection: collection}))
		_, err := component.GetCollection(ctx, &vector.GetCollectionRequest{Collection: collection})
		requireStatusCode(t, err, codes.NotFound)
	})

	t.Run("close", func(t *testing.T) { require.NoError(t, component.Close()) })
}

func upsertLargestVectorBatch(t *testing.T, ctx context.Context, component vector.Vector, collection string, target int) int {
	t.Helper()

	for size := target; size >= 1; size /= 2 {
		_ = component.DeleteCollection(context.Background(), &vector.DeleteCollectionRequest{Collection: collection})
		require.NoError(t, component.CreateCollection(ctx, certCreateRequest(collection, 4)))
		records := makeLargeRecords(size)
		resp, err := component.Upsert(ctx, &vector.UpsertRequest{Collection: collection, Records: records, Options: certWaitOptions()})
		if err == nil && len(resp.FailedItems) == 0 {
			return size
		}
		t.Logf("single-call vector batch size %d rejected: err=%v failedItems=%d", size, err, failedItemCount(resp))
	}
	t.Fatal("no successful vector batch size found")
	return 0
}

func failedItemCount(resp *vector.UpsertResponse) int {
	if resp == nil {
		return 0
	}
	return len(resp.FailedItems)
}

func makeLargeRecords(size int) []vector.Record {
	records := make([]vector.Record, size)
	for i := range records {
		records[i] = vector.Record{
			ID:       fmt.Sprintf("large-vec-%05d", i),
			Values:   []float32{float32(i % 7), float32(i % 11), float32(i % 13), 1},
			Payload:  []byte(fmt.Sprintf(`{"ordinal":%d}`, i)),
			Metadata: map[string]any{"genre": "novel", "ordinal": float64(i)},
		}
	}
	return records
}

func makeRandomRecords(count, dimensions int, prefix string) []vector.Record {
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic test data
	records := make([]vector.Record, count)
	for i := range records {
		values := make([]float32, dimensions)
		for j := range values {
			values[j] = rng.Float32()
		}
		records[i] = vector.Record{ID: fmt.Sprintf("%s-vec-%03d", prefix, i), Values: values, Metadata: map[string]any{"author": prefix}}
	}
	return records
}

func vectorCertificationRecords() []vector.Record {
	records := make([]vector.Record, 50)
	genres := []string{"novel", "fiction", "poetry", "drama", "essay"}
	for i := range records {
		author := "frankenstein"
		if i%2 == 0 {
			author = "pride-and-prejudice"
		}
		md := map[string]any{
			"author":  author,
			"genre":   genres[i%5],
			"price":   float64(i * 1000),
			"inStock": i%3 == 0,
		}
		if i%10 == 0 {
			md["promo"] = "spring"
		}
		records[i] = vector.Record{
			ID:       fmt.Sprintf("vec-%03d", i),
			Values:   certVectorValues(i),
			Payload:  []byte(fmt.Sprintf(`{"ordinal":%d}`, i)),
			Metadata: md,
		}
	}
	return records
}

func certUniformValues() []float32 {
	values := make([]float32, 8)
	for i := range values {
		values[i] = 1
	}
	return values
}

func certVectorValues(seed int) []float32 {
	values := make([]float32, 8)
	values[seed%8] = 1
	values[(seed+1)%8] = float32(seed%5) / 10
	return values
}

// requireStatusCode asserts that a component returned a gRPC status error with
// one of the expected canonical codes.
func requireStatusCode(t *testing.T, err error, want ...codes.Code) {
	t.Helper()
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok, "component errors must be gRPC status errors, got %T: %v", err, err)
	require.NotEqual(t, codes.OK, st.Code())
	assert.Contains(t, want, st.Code(), "unexpected status code %s: %v", st.Code(), err)
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
