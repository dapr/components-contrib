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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/search"
)

func TestDistanceMetricWireValues(t *testing.T) {
	t.Parallel()

	assert.Equal(t, int32(0), int32(DistanceMetricUnspecified))
	assert.Equal(t, int32(1), int32(DistanceMetricCosine))
	assert.Equal(t, int32(2), int32(DistanceMetricDotProduct))
	assert.Equal(t, int32(3), int32(DistanceMetricEuclidean))
	assert.Equal(t, int32(4), int32(DistanceMetricManhattan))
	assert.Equal(t, int32(5), int32(DistanceMetricHamming))
}

func TestZeroValueResponses(t *testing.T) {
	t.Parallel()

	create := CreateCollectionResponse{}
	assert.Equal(t, CreateCollectionResponse{}, create)

	describe := DescribeCollectionResponse{}
	assert.Empty(t, describe.Collection)
	assert.Zero(t, describe.Dimension)
	assert.Equal(t, DistanceMetricUnspecified, describe.Metric)
	assert.Nil(t, describe.MetadataSchema)
	assert.Zero(t, describe.VectorCount)
	assert.Nil(t, describe.Properties)

	list := ListCollectionsResponse{}
	assert.Nil(t, list.Collections)

	upsert := UpsertResponse{}
	assert.Nil(t, upsert.Results)
	assert.Equal(t, search.IndexAckUnspecified, upsert.Ack)

	get := GetResponse{}
	assert.Nil(t, get.Vectors)

	deleteResponse := DeleteResponse{}
	assert.Nil(t, deleteResponse.Results)
	assert.Zero(t, deleteResponse.DeletedCount)
	assert.Equal(t, search.IndexAckUnspecified, deleteResponse.Ack)

	query := QueryResponse{}
	assert.Nil(t, query.Hits)
	assert.Empty(t, query.ContinuationToken)

	batch := BatchQueryResponse{}
	assert.Nil(t, batch.Results)
}

func TestZeroValueVecAndHit(t *testing.T) {
	t.Parallel()

	vec := Vec{}
	assert.Empty(t, vec.ID)
	assert.Nil(t, vec.Values)
	assert.Nil(t, vec.Metadata)
	assert.Empty(t, vec.Namespace)

	hit := Hit{}
	assert.Equal(t, Vec{}, hit.Vector)
	assert.Zero(t, hit.Distance)
}

func TestZeroValueBatchQueryResult(t *testing.T) {
	t.Parallel()

	result := BatchQueryResult{}
	assert.Zero(t, result.QueryIndex)
	assert.Nil(t, result.Hits)
	assert.Empty(t, result.ErrorCode)
	assert.Empty(t, result.ErrorMessage)
}

func TestDeleteSelectorDocumentedInvariant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   DeleteSelector
		wantErr bool
	}{
		{
			name:  "only ids",
			input: DeleteSelector{IDs: []string{"vec-1"}},
		},
		{
			name:  "only filter",
			input: DeleteSelector{Filter: map[string]any{"tenant": "a"}},
		},
		{
			name:    "both ids and filter",
			input:   DeleteSelector{IDs: []string{"vec-1"}, Filter: map[string]any{"tenant": "a"}},
			wantErr: true,
		},
		{
			name:    "neither ids nor filter",
			input:   DeleteSelector{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateDeleteSelectorForTest(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestQueryRequestDocumentedInvariant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   QueryRequest
		wantErr bool
	}{
		{
			name:  "only query vector",
			input: QueryRequest{QueryVector: []float32{1, 2, 3}},
		},
		{
			name:  "only query by id",
			input: QueryRequest{QueryByID: "vec-1"},
		},
		{
			name:    "both query vector and query by id",
			input:   QueryRequest{QueryVector: []float32{1, 2, 3}, QueryByID: "vec-1"},
			wantErr: true,
		},
		{
			name:    "neither query vector nor query by id",
			input:   QueryRequest{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateQueryRequestForTest(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestScoreThresholdDistinguishesNilFromZero(t *testing.T) {
	t.Parallel()

	unset := QueryRequest{}
	assert.Nil(t, unset.ScoreThreshold)

	zero := 0.0
	setToZero := QueryRequest{ScoreThreshold: &zero}
	require.NotNil(t, setToZero.ScoreThreshold)
	assert.Equal(t, 0.0, *setToZero.ScoreThreshold)
}

// validateDeleteSelectorForTest mirrors the documented contract invariant. The
// production package intentionally leaves enforcement to component implementations.
func validateDeleteSelectorForTest(selector DeleteSelector) error {
	hasIDs := len(selector.IDs) > 0
	hasFilter := len(selector.Filter) > 0
	if hasIDs == hasFilter {
		return errors.New("exactly one of IDs or Filter must be set")
	}
	return nil
}

// validateQueryRequestForTest mirrors the documented contract invariant. The
// production package intentionally leaves enforcement to component implementations.
func validateQueryRequestForTest(req QueryRequest) error {
	hasQueryVector := len(req.QueryVector) > 0
	hasQueryByID := req.QueryByID != ""
	if hasQueryVector == hasQueryByID {
		return errors.New("exactly one of QueryVector or QueryByID must be set")
	}
	return nil
}
