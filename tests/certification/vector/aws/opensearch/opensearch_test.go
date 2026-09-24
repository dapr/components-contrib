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

package opensearch_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	common "github.com/dapr/components-contrib/common/component/aws/opensearch"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/components-contrib/vector"
	vectoropensearch "github.com/dapr/components-contrib/vector/aws/opensearch"
	"github.com/dapr/kit/logger"
)

type fixture struct {
	component vector.Vector
	client    *common.Client
	ctx       context.Context
	name      string
	props     map[string]string
}

func newFixture(t *testing.T, dimensions uint32, metric vector.DistanceMetric) *fixture {
	t.Helper()
	endpoint := os.Getenv("OPENSEARCH_ENDPOINT")
	if endpoint == "" {
		t.Skip("OPENSEARCH_ENDPOINT is required; use a real OpenSearch instance with the k-NN plugin")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	t.Cleanup(cancel)
	props := map[string]string{
		"endpoint": endpoint, "region": "us-east-1", "accessKey": "test", "secretKey": "test", "timeout": "3m",
	}
	log := logger.NewLogger("opensearch-vector-certification")
	component := vectoropensearch.NewOpenSearch(log)
	require.NoError(t, component.Init(ctx, vector.Metadata{Base: metadata.Base{Properties: props}}))
	t.Cleanup(func() { assert.NoError(t, component.Close()) })
	client, err := common.NewClient(ctx, props, log)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, client.Close()) })
	f := &fixture{component: component, client: client, ctx: ctx, name: "cert-vector-" + uuid.NewString(), props: props}
	// A dedicated raw client permits cleanup even when a test closes the component
	// or deliberately replaces its mapping metadata with another component kind.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		path, pathErr := common.IndexPath(f.name)
		require.NoError(t, pathErr)
		deleteErr := client.Do(cleanupCtx, http.MethodDelete, path, nil, nil)
		if status.Code(deleteErr) != codes.NotFound {
			assert.NoError(t, deleteErr)
		}
	})
	require.NoError(t, component.CreateCollection(ctx, &vector.CreateCollectionRequest{
		Collection: f.name, Dimensions: dimensions, Metric: metric,
	}))
	return f
}

func (f *fixture) upsert(t *testing.T, records ...vector.Record) {
	t.Helper()
	response, err := f.component.Upsert(f.ctx, &vector.UpsertRequest{Collection: f.name, Records: records})
	require.NoError(t, err)
	require.Empty(t, response.FailedItems)
	require.Equal(t, search.IndexAckCompleted, response.Ack)
}

func (f *fixture) query(t *testing.T, request vector.QueryRequest) *vector.QueryResponse {
	t.Helper()
	request.Collection = f.name
	response, err := f.component.Query(f.ctx, &request)
	require.NoError(t, err)
	return response
}

func expectedScore(a, b []float32, metric vector.DistanceMetric) float64 {
	var dot, normA, normB, squaredDistance float64
	for i, av := range a {
		x, y := float64(av), float64(b[i])
		dot += x * y
		normA += x * x
		normB += y * y
		squaredDistance += (x - y) * (x - y)
	}
	switch metric {
	case vector.DistanceMetricCosine:
		return dot / math.Sqrt(normA*normB)
	case vector.DistanceMetricDotProduct:
		return dot
	default:
		return math.Sqrt(squaredDistance)
	}
}

func assertScores(t *testing.T, response *vector.QueryResponse, queryValues []float32, records []vector.Record) {
	t.Helper()
	require.Len(t, response.Matches, len(records))
	expected := make(map[string]vector.Record, len(records))
	for _, record := range records {
		expected[record.ID] = record
	}
	for i, match := range response.Matches {
		record, exists := expected[match.Record.ID]
		require.True(t, exists, "unexpected or duplicate match %q", match.Record.ID)
		assert.InDelta(t, expectedScore(queryValues, record.Values, response.Metric), match.Score, 2e-5, "record %q metric %v", record.ID, response.Metric)
		delete(expected, match.Record.ID)
		if i > 0 {
			if response.Metric.HigherIsBetter() {
				assert.GreaterOrEqual(t, response.Matches[i-1].Score, match.Score)
			} else {
				assert.LessOrEqual(t, response.Matches[i-1].Score, match.Score)
			}
		}
	}
}

func TestExactMetricsAndOverrides(t *testing.T) {
	records := []vector.Record{
		{ID: "same", Values: []float32{2, 0, 0}},
		{ID: "longer", Values: []float32{7, 0, 0}},
		{ID: "opposite", Values: []float32{-3, 0, 0}},
		{ID: "orthogonal", Values: []float32{0, 4, 0}},
		{ID: "oblique", Values: []float32{1, 2, -2}},
		{ID: "negative", Values: []float32{-1, 1, 2}},
	}
	queryValues := []float32{2, 0, 0}
	for _, collectionMetric := range []vector.DistanceMetric{vector.DistanceMetricCosine, vector.DistanceMetricDotProduct, vector.DistanceMetricEuclidean} {
		t.Run(fmt.Sprintf("collection_metric_%d", collectionMetric), func(t *testing.T) {
			f := newFixture(t, 3, collectionMetric)
			f.upsert(t, records...)
			for _, queryMetric := range []vector.DistanceMetric{vector.DistanceMetricUnspecified, vector.DistanceMetricCosine, vector.DistanceMetricDotProduct, vector.DistanceMetricEuclidean} {
				t.Run(fmt.Sprintf("query_metric_%d", queryMetric), func(t *testing.T) {
					response := f.query(t, vector.QueryRequest{Vector: &vector.Record{Values: queryValues}, Metric: queryMetric, TopK: 20})
					effective := queryMetric
					if effective == vector.DistanceMetricUnspecified {
						effective = collectionMetric
					}
					assert.Equal(t, effective, response.Metric)
					assertScores(t, response, queryValues, records)
					threshold := response.Matches[3].Score
					filtered := f.query(t, vector.QueryRequest{Vector: &vector.Record{Values: queryValues}, Metric: queryMetric, TopK: 20, ScoreThreshold: &threshold})
					var expectedIDs []string
					for _, match := range response.Matches {
						if (effective.HigherIsBetter() && match.Score >= threshold) || (!effective.HigherIsBetter() && match.Score <= threshold) {
							expectedIDs = append(expectedIDs, match.Record.ID)
						}
					}
					var actualIDs []string
					for _, match := range filtered.Matches {
						actualIDs = append(actualIDs, match.Record.ID)
					}
					assert.ElementsMatch(t, expectedIDs, actualIDs, "inclusive boundary retains every qualifying match")
				})
			}
			description, err := f.component.GetCollection(f.ctx, &vector.GetCollectionRequest{Collection: f.name})
			require.NoError(t, err)
			assert.Equal(t, collectionMetric, description.Metric, "query overrides never mutate collection settings")
		})
	}
}

func TestByIDDuplicatesFilteringAndBatch(t *testing.T) {
	f := newFixture(t, 2, vector.DistanceMetricCosine)
	records := []vector.Record{
		{ID: "self", Values: []float32{1, 0}, Metadata: map[string]any{"group": "keep"}},
		{ID: "duplicate", Values: []float32{1, 0}, Metadata: map[string]any{"group": "keep"}},
		{ID: "opposite", Values: []float32{-1, 0}, Metadata: map[string]any{"group": "keep"}},
		{ID: "orthogonal", Values: []float32{0, 1}, Metadata: map[string]any{"group": "drop"}},
	}
	f.upsert(t, records...)
	response := f.query(t, vector.QueryRequest{ByID: "self", TopK: 10})
	require.Len(t, response.Matches, 3)
	assert.Equal(t, "duplicate", response.Matches[0].Record.ID, "exclude only the ID, not identical vectors")
	assert.Equal(t, "opposite", response.Matches[2].Record.ID)
	assert.InDelta(t, -1, response.Matches[2].Score, 1e-6, "zero OpenSearch score must remain a valid hit")
	filtered := f.query(t, vector.QueryRequest{ByID: "self", TopK: 1, Filter: map[string]any{"group": "drop"}})
	require.Len(t, filtered.Matches, 1)
	assert.Equal(t, "orthogonal", filtered.Matches[0].Record.ID, "metadata filter is applied before top-k")
	minusOne, zero := -1.0, 0.0
	batch, err := f.component.BatchQuery(f.ctx, &vector.BatchQueryRequest{
		Collection: f.name,
		Queries: []vector.QueryRequest{
			{Collection: "ignored", ByID: "self", ScoreThreshold: &minusOne, TopK: 10},
			{Vector: &vector.Record{Values: []float32{1, 0}}, Metric: vector.DistanceMetricEuclidean, ScoreThreshold: &zero, TopK: 10},
			{Vector: &vector.Record{Values: []float32{1, 0}}, Metric: vector.DistanceMetricDotProduct, TopK: 10},
			{Vector: &vector.Record{Values: []float32{1}}},
			{ByID: "missing"},
			{Vector: &vector.Record{Values: []float32{1, 0}}, Filter: map[string]any{"group": map[string]any{"$regex": ".*"}}},
		},
	})
	require.NoError(t, err)
	require.Len(t, batch.Results, 6)
	for i := range 3 {
		require.NoError(t, batch.Results[i].Error)
		require.NotNil(t, batch.Results[i].Response)
	}
	assert.Len(t, batch.Results[0].Response.Matches, 3)
	assert.Len(t, batch.Results[1].Response.Matches, 2)
	assert.Equal(t, vector.DistanceMetricDotProduct, batch.Results[2].Response.Metric)
	for i, code := range []codes.Code{codes.InvalidArgument, codes.NotFound, codes.InvalidArgument} {
		assert.Equal(t, code, status.Code(batch.Results[i+3].Error))
		assert.Nil(t, batch.Results[i+3].Response)
	}
}

func TestDimensionBoundaries(t *testing.T) {
	for _, dimension := range []uint32{1, 1024, 16000} {
		t.Run(strconv.FormatUint(uint64(dimension), 10), func(t *testing.T) {
			f := newFixture(t, dimension, vector.DistanceMetricCosine)
			values := make([]float32, dimension)
			values[0] = 1
			f.upsert(t, vector.Record{ID: "valid", Values: values})
			response := f.query(t, vector.QueryRequest{Vector: &vector.Record{Values: values}, IncludeValues: true})
			require.Len(t, response.Matches, 1)
			assert.Equal(t, values, response.Matches[0].Record.Values)
			for _, invalid := range [][]float32{nil, make([]float32, dimension-1), make([]float32, dimension+1)} {
				_, err := f.component.Upsert(f.ctx, &vector.UpsertRequest{Collection: f.name, Records: []vector.Record{{ID: "invalid", Values: invalid}}})
				assert.Equal(t, codes.InvalidArgument, status.Code(err))
				_, err = f.component.Query(f.ctx, &vector.QueryRequest{Collection: f.name, Vector: &vector.Record{Values: invalid}})
				assert.Equal(t, codes.InvalidArgument, status.Code(err))
			}
			_, err := f.component.Query(f.ctx, &vector.QueryRequest{Collection: f.name, ByID: "invalid"})
			assert.Equal(t, codes.NotFound, status.Code(err))
		})
	}
	f := newFixture(t, 2, vector.DistanceMetricCosine)
	require.NoError(t, f.component.DeleteCollection(f.ctx, &vector.DeleteCollectionRequest{Collection: f.name}))
	err := f.component.CreateCollection(f.ctx, &vector.CreateCollectionRequest{Collection: f.name, Dimensions: 0})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = f.component.GetCollection(f.ctx, &vector.GetCollectionRequest{Collection: f.name})
	assert.Equal(t, codes.NotFound, status.Code(err), "invalid dimensions do not leave a collection")
}

func TestZeroVectors(t *testing.T) {
	for _, metric := range []vector.DistanceMetric{vector.DistanceMetricCosine, vector.DistanceMetricDotProduct, vector.DistanceMetricEuclidean} {
		t.Run(fmt.Sprintf("metric_%d", metric), func(t *testing.T) {
			f := newFixture(t, 2, metric)
			zero := vector.Record{ID: "zero", Values: []float32{0, 0}, Metadata: map[string]any{"nonzero": false}}
			nonzero := vector.Record{ID: "nonzero", Values: []float32{3, 4}, Metadata: map[string]any{"nonzero": true}}
			f.upsert(t, nonzero)
			response, err := f.component.Upsert(f.ctx, &vector.UpsertRequest{Collection: f.name, Records: []vector.Record{zero}})
			if metric == vector.DistanceMetricCosine {
				assert.Equal(t, codes.InvalidArgument, status.Code(err))
				assert.Nil(t, response)
			} else {
				require.NoError(t, err)
				require.Empty(t, response.FailedItems)
				results := f.query(t, vector.QueryRequest{Vector: &zero, TopK: 10})
				assertScores(t, results, zero.Values, []vector.Record{zero, nonzero})
				_, err = f.component.Query(f.ctx, &vector.QueryRequest{Collection: f.name, Vector: &nonzero, Metric: vector.DistanceMetricCosine})
				assert.Equal(t, codes.InvalidArgument, status.Code(err), "cosine of a stored zero vector is undefined, not a fabricated similarity")
				filtered := f.query(t, vector.QueryRequest{Vector: &nonzero, Metric: vector.DistanceMetricCosine, Filter: map[string]any{"nonzero": true}})
				require.Len(t, filtered.Matches, 1)
				assert.InDelta(t, 1, filtered.Matches[0].Score, 1e-6)
			}
			_, err = f.component.Query(f.ctx, &vector.QueryRequest{Collection: f.name, Vector: &zero, Metric: vector.DistanceMetricCosine})
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			_, err = f.component.Query(f.ctx, &vector.QueryRequest{Collection: f.name, ByID: "zero", Metric: vector.DistanceMetricCosine})
			if metric == vector.DistanceMetricCosine {
				assert.Equal(t, codes.NotFound, status.Code(err))
			} else {
				assert.Equal(t, codes.InvalidArgument, status.Code(err))
			}
		})
	}
}

//nolint:gosmopolitan // Verify multilingual IDs and metadata survive actual provider serialization.
func TestLargeBulkRoundTripAndPersistence(t *testing.T) {
	f := newFixture(t, 1024, vector.DistanceMetricCosine)
	const count = 128
	records := make([]vector.Record, count)
	binary := bytes.Repeat([]byte{0, 255, 128, 1, 17}, 2000)
	longString := strings.Repeat("日本語 café 🚀 ", 80)
	for i := range records {
		values := make([]float32, 1024)
		values[i] = 1
		records[i] = vector.Record{
			ID: "文書/🚀-" + strconv.Itoa(i), Values: values, Payload: binary,
			Metadata: map[string]any{
				"ordinal": float64(i), "date": "2026-09-24", "long": longString,
				"nested": map[string]any{"city": "Zürich", "active": true},
				"tags":   []any{"résumé", "東京"},
			},
		}
	}
	f.upsert(t, records...)
	ids := make([]string, 0, count+2)
	for i := count - 1; i >= 0; i-- {
		ids = append(ids, records[i].ID)
	}
	ids = append(ids, "missing", records[0].ID)
	get, err := f.component.Get(f.ctx, &vector.GetRequest{Collection: f.name, IDs: ids, IncludeValues: true})
	require.NoError(t, err)
	require.Len(t, get.Records, count+1)
	for i := range count {
		assert.Equal(t, records[count-i-1], get.Records[i])
	}
	assert.Equal(t, records[0], get.Records[count], "duplicate gets preserve request order")
	filtered := f.query(t, vector.QueryRequest{
		Vector: &records[17], TopK: 5, IncludeValues: true, IncludePayload: true,
		Filter: map[string]any{"$and": []any{
			map[string]any{"long": longString}, map[string]any{"date": "2026-09-24"},
			map[string]any{"nested.city": "Zürich"}, map[string]any{"nested.active": true},
			map[string]any{"ordinal": map[string]any{"$gte": 17, "$lte": 17}},
		}},
	})
	require.Len(t, filtered.Matches, 1)
	assert.Equal(t, records[17], filtered.Matches[0].Record)
	require.NoError(t, f.component.Close())
	second := vectoropensearch.NewOpenSearch(logger.NewLogger("vector-certification-reopen"))
	initCtx, cancel := context.WithCancel(f.ctx)
	require.NoError(t, second.Init(initCtx, vector.Metadata{Base: metadata.Base{Properties: f.props}}))
	cancel()
	t.Cleanup(func() { assert.NoError(t, second.Close()) })
	reopened, err := second.Get(f.ctx, &vector.GetRequest{Collection: f.name, IDs: []string{records[17].ID}, IncludeValues: true})
	require.NoError(t, err, "Init context cancellation and closing a different client cannot destroy persistent data")
	assert.Equal(t, []vector.Record{records[17]}, reopened.Records)
	info, err := second.GetCollection(f.ctx, &vector.GetCollectionRequest{Collection: f.name})
	require.NoError(t, err)
	assert.Equal(t, uint64(count), info.RecordCount)
}

func TestMappingConflictPartialFailureAndRecovery(t *testing.T) {
	f := newFixture(t, 2, vector.DistanceMetricCosine)
	f.upsert(t, vector.Record{ID: "seed", Values: []float32{1, 0}, Metadata: map[string]any{"count": 1}})
	good := vector.Record{ID: "good", Values: []float32{0, 1}, Metadata: map[string]any{"count": 2}}
	bad := vector.Record{ID: "bad", Values: []float32{1, 1}, Metadata: map[string]any{"count": "not a number"}}
	response, err := f.component.Upsert(f.ctx, &vector.UpsertRequest{Collection: f.name, Records: []vector.Record{good, bad}})
	require.NoError(t, err)
	assert.Equal(t, search.IndexAckCompleted, response.Ack)
	require.Len(t, response.FailedItems, 1)
	assert.Equal(t, bad.ID, response.FailedItems[0].ID)
	assert.Equal(t, codes.InvalidArgument, response.FailedItems[0].Error.Code())
	get, err := f.component.Get(f.ctx, &vector.GetRequest{Collection: f.name, IDs: []string{good.ID, bad.ID}, IncludeValues: true})
	require.NoError(t, err)
	require.Len(t, get.Records, 1)
	assert.Equal(t, good.ID, get.Records[0].ID)
	bad.Metadata["count"] = 3
	f.upsert(t, bad)
	get, err = f.component.Get(f.ctx, &vector.GetRequest{Collection: f.name, IDs: []string{bad.ID}, IncludeValues: true})
	require.NoError(t, err)
	require.Len(t, get.Records, 1)
	assert.Equal(t, float64(3), get.Records[0].Metadata["count"])
	deleted, err := f.component.Delete(f.ctx, &vector.DeleteRequest{Collection: f.name, IDs: []string{good.ID, "missing", good.ID}})
	require.NoError(t, err)
	assert.Equal(t, search.IndexAckCompleted, deleted.Ack)
}

func TestCollectionKindBarrier(t *testing.T) {
	f := newFixture(t, 2, vector.DistanceMetricCosine)
	f.upsert(t, vector.Record{ID: "keep", Values: []float32{1, 0}})
	path, err := common.IndexPath(f.name)
	require.NoError(t, err)
	require.NoError(t, f.client.Do(f.ctx, http.MethodPut, path+"/_mapping", map[string]any{
		"_meta": map[string]any{"dapr_kind": "search", "dimensions": 2, "metric": 1},
	}, nil))
	_, err = f.component.GetCollection(f.ctx, &vector.GetCollectionRequest{Collection: f.name})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = f.component.Upsert(f.ctx, &vector.UpsertRequest{Collection: f.name, Records: []vector.Record{{ID: "new", Values: []float32{1, 0}}}})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = f.component.Get(f.ctx, &vector.GetRequest{Collection: f.name, IDs: []string{"keep"}})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = f.component.Delete(f.ctx, &vector.DeleteRequest{Collection: f.name, IDs: []string{"keep"}})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = f.component.Query(f.ctx, &vector.QueryRequest{Collection: f.name, ByID: "keep"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = f.component.BatchQuery(f.ctx, &vector.BatchQueryRequest{Collection: f.name})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Equal(t, codes.FailedPrecondition, status.Code(f.component.DeleteCollection(f.ctx, &vector.DeleteCollectionRequest{Collection: f.name})))
	list, err := f.component.ListCollections(f.ctx, nil)
	require.NoError(t, err)
	assert.NotContains(t, list.Collections, f.name)
	docs, err := f.client.GetDocuments(f.ctx, f.name, []string{"keep", "new"})
	require.NoError(t, err)
	require.Len(t, docs, 1)
	assert.Equal(t, "keep", docs[0].ID, "wrong-kind operations never modify existing data")
}

func TestCancellationAndClose(t *testing.T) {
	f := newFixture(t, 2, vector.DistanceMetricCosine)
	f.upsert(t, vector.Record{ID: "keep", Values: []float32{1, 0}})
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	_, err := f.component.Upsert(ctx, &vector.UpsertRequest{Collection: f.name, Records: []vector.Record{{ID: "cancelled", Values: []float32{0, 1}}}})
	assert.Equal(t, codes.Canceled, status.Code(err))
	_, err = f.component.Query(ctx, &vector.QueryRequest{Collection: f.name, ByID: "keep"})
	assert.Equal(t, codes.Canceled, status.Code(err))
	_, err = f.component.BatchQuery(ctx, &vector.BatchQueryRequest{Collection: f.name, Queries: []vector.QueryRequest{{ByID: "keep"}}})
	assert.Equal(t, codes.Canceled, status.Code(err))
	_, err = f.component.Delete(ctx, &vector.DeleteRequest{Collection: f.name, IDs: []string{"keep"}})
	assert.Equal(t, codes.Canceled, status.Code(err))
	get, err := f.component.Get(f.ctx, &vector.GetRequest{Collection: f.name, IDs: []string{"keep", "cancelled"}})
	require.NoError(t, err)
	require.Len(t, get.Records, 1)
	assert.Equal(t, "keep", get.Records[0].ID)
	require.NoError(t, f.component.Close())
	require.NoError(t, f.component.Close())
	_, err = f.component.Get(f.ctx, &vector.GetRequest{Collection: f.name, IDs: []string{"keep"}})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = f.component.Query(f.ctx, &vector.QueryRequest{Collection: f.name, ByID: "keep"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = f.component.ListCollections(f.ctx, nil)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestCorruptNativeSource(t *testing.T) {
	f := newFixture(t, 2, vector.DistanceMetricCosine)
	f.upsert(t, vector.Record{ID: "corrupt", Values: []float32{1, 0}})
	path, err := common.IndexPath(f.name)
	require.NoError(t, err)
	// Binary fields are not indexed and accept arrays; this is valid provider
	// data but violates the component's single opaque []byte payload contract.
	require.NoError(t, f.client.Do(f.ctx, http.MethodPut, path+"/_doc/corrupt?refresh=wait_for", map[string]any{
		"values": []float32{1, 0}, "payload": []string{"YQ==", "Yg=="},
	}, nil))
	_, err = f.component.Get(f.ctx, &vector.GetRequest{Collection: f.name, IDs: []string{"corrupt"}})
	assert.Equal(t, codes.Internal, status.Code(err))
	_, err = f.component.Query(f.ctx, &vector.QueryRequest{Collection: f.name, Vector: &vector.Record{Values: []float32{1, 0}}, IncludePayload: true})
	assert.Equal(t, codes.Internal, status.Code(err))
	f.upsert(t, vector.Record{ID: "corrupt", Values: []float32{1, 0}, Payload: []byte("repaired")})
	get, err := f.component.Get(f.ctx, &vector.GetRequest{Collection: f.name, IDs: []string{"corrupt"}})
	require.NoError(t, err)
	require.Len(t, get.Records, 1)
	assert.Equal(t, []byte("repaired"), get.Records[0].Payload)
}
