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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/search"
)

func TestDistanceMetricEnumValuesStable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		metric DistanceMetric
		want   int32
	}{
		{name: "unspecified", metric: DistanceMetricUnspecified, want: 0},
		{name: "cosine", metric: DistanceMetricCosine, want: 1},
		{name: "dot product", metric: DistanceMetricDotProduct, want: 2},
		{name: "euclidean", metric: DistanceMetricEuclidean, want: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, int32(tt.metric))
		})
	}
}

func TestDistanceMetricHigherIsBetter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		metric DistanceMetric
		want   bool
	}{
		{name: "unspecified defers to the collection metric", metric: DistanceMetricUnspecified, want: true},
		{name: "cosine is higher is better", metric: DistanceMetricCosine, want: true},
		{name: "dot product is higher is better", metric: DistanceMetricDotProduct, want: true},
		{name: "euclidean is lower is better", metric: DistanceMetricEuclidean, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.metric.HigherIsBetter())
		})
	}
}

func TestZeroValueResponsesAreUsable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		check func(t *testing.T)
	}{
		{
			name: "get collection response",
			check: func(t *testing.T) {
				resp := GetCollectionResponse{}
				assert.Empty(t, resp.Collection)
				assert.Zero(t, resp.RecordCount)
				assert.Empty(t, resp.Properties["missing"])
			},
		},
		{
			name: "list collections response",
			check: func(t *testing.T) {
				resp := ListCollectionsResponse{}
				assert.Empty(t, resp.Collections)
			},
		},
		{
			name: "upsert response",
			check: func(t *testing.T) {
				resp := UpsertResponse{}
				assert.Empty(t, resp.FailedItems)
				assert.Equal(t, search.IndexAckUnspecified, resp.Ack)
			},
		},
		{
			name: "get response",
			check: func(t *testing.T) {
				resp := GetResponse{}
				assert.Empty(t, resp.Records)
			},
		},
		{
			name: "query response",
			check: func(t *testing.T) {
				resp := QueryResponse{}
				assert.Empty(t, resp.Matches)
				assert.Equal(t, DistanceMetricUnspecified, resp.Metric)
			},
		},
		{
			name: "batch query response",
			check: func(t *testing.T) {
				resp := BatchQueryResponse{}
				assert.Empty(t, resp.Results)
			},
		},
		{
			name: "batch query result",
			check: func(t *testing.T) {
				result := BatchQueryResult{}
				assert.Nil(t, result.Response)
				require.NoError(t, result.Error)
			},
		},
		{
			name: "delete response",
			check: func(t *testing.T) {
				resp := DeleteResponse{}
				assert.Equal(t, search.IndexAckUnspecified, resp.Ack)
			},
		},
		{
			name: "record",
			check: func(t *testing.T) {
				record := Record{}
				assert.Empty(t, record.ID)
				assert.Empty(t, record.Values)
				assert.Empty(t, record.Payload)
				assert.Nil(t, record.Metadata["missing"])
			},
		},
		{
			name: "query request",
			check: func(t *testing.T) {
				req := QueryRequest{}
				assert.Nil(t, req.Vector)
				assert.Empty(t, req.ByID)
				assert.Equal(t, DistanceMetricUnspecified, req.Metric)
				assert.Nil(t, req.ScoreThreshold)
			},
		},
		{
			name: "create collection request",
			check: func(t *testing.T) {
				req := CreateCollectionRequest{}
				assert.Zero(t, req.Dimensions)
				assert.Equal(t, DistanceMetricUnspecified, req.Metric)
				assert.Empty(t, req.Metadata["missing"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.NotPanics(t, func() { tt.check(t) })
		})
	}
}

func TestRecordCarriesDenseValuesPayloadAndStructuredMetadata(t *testing.T) {
	t.Parallel()

	record := Record{
		ID:      "vec-1",
		Values:  []float32{1, 2, 3},
		Payload: []byte(`{"title":"a"}`),
		Metadata: map[string]any{
			"tenant": "acme",
			"year":   float64(2024),
			"tags":   []any{"a", "b"},
			"dealer": map[string]any{"city": "seoul"},
		},
	}

	assert.Equal(t, []float32{1, 2, 3}, record.Values)
	assert.JSONEq(t, `{"title":"a"}`, string(record.Payload))
	assert.Equal(t, "acme", record.Metadata["tenant"])
	assert.InDelta(t, 2024, record.Metadata["year"], 0)
	assert.Equal(t, []any{"a", "b"}, record.Metadata["tags"])
	assert.Equal(t, map[string]any{"city": "seoul"}, record.Metadata["dealer"])
}

func TestBatchQueryResultIsEitherAResponseOrAnError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		result BatchQueryResult
	}{
		{name: "response", result: BatchQueryResult{Response: &QueryResponse{Metric: DistanceMetricCosine}}},
		{name: "error", result: BatchQueryResult{Error: status.Error(codes.InvalidArgument, "exactly one of vector or id must be set")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			hasResponse := tt.result.Response != nil
			hasError := tt.result.Error != nil
			assert.NotEqual(t, hasResponse, hasError, "exactly one of Response or Error is set")
			if hasError {
				assert.NotEqual(t, codes.OK, status.Code(tt.result.Error))
			}
		})
	}
}

func TestDeleteRequestReusesTheSearchWriteTypes(t *testing.T) {
	t.Parallel()

	req := DeleteRequest{Collection: "cars", IDs: []string{"vec-1"}, Options: search.IndexingOptions{Mode: search.IndexingModeReturnOnAcceptance}}
	resp := DeleteResponse{Ack: search.IndexAckQueued}

	assert.Equal(t, search.IndexingModeReturnOnAcceptance, req.Options.Mode)
	assert.Equal(t, search.IndexAckQueued, resp.Ack)
}

func TestUpsertResponseSharesTheSearchWriteTypes(t *testing.T) {
	t.Parallel()

	resp := UpsertResponse{
		FailedItems: []search.FailedItem{{ID: "vec-1", Error: status.New(codes.InvalidArgument, "the record has no vector values")}},
		Ack:         search.IndexAckCompleted,
	}

	require.Len(t, resp.FailedItems, 1)
	assert.Equal(t, "vec-1", resp.FailedItems[0].ID)
	assert.Equal(t, codes.InvalidArgument, resp.FailedItems[0].Error.Code())
	assert.Equal(t, search.IndexAckCompleted, resp.Ack)
}
