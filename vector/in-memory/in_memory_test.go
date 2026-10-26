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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/components-contrib/vector"
	"github.com/dapr/kit/logger"
)

func TestQueuedUpsertIntoReplacedCollection(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	v := NewInMemoryVector(logger.NewLogger("test"))
	require.NoError(t, v.Init(ctx, vector.Metadata{Base: metadata.Base{Properties: map[string]string{"indexingDelay": "100ms"}}}))
	require.NoError(t, v.CreateCollection(ctx, &vector.CreateCollectionRequest{Collection: "c", Dimensions: 2}))

	resp, err := v.Upsert(ctx, &vector.UpsertRequest{Collection: "c", Records: []vector.Record{{ID: "a", Values: []float32{1, 0}}}})
	require.NoError(t, err)
	assert.Equal(t, search.IndexAckQueued, resp.Ack)

	require.NoError(t, v.DeleteCollection(ctx, &vector.DeleteCollectionRequest{Collection: "c"}))
	require.NoError(t, v.CreateCollection(ctx, &vector.CreateCollectionRequest{Collection: "c", Dimensions: 4}))
	time.Sleep(300 * time.Millisecond)

	// The queued two-dimensional record is dropped rather than stored in the
	// four-dimensional collection.
	q, err := v.Query(ctx, &vector.QueryRequest{Collection: "c", Vector: &vector.Record{Values: []float32{1, 0, 0, 0}}, TopK: 1})
	require.NoError(t, err)
	assert.Empty(t, q.Matches)
}

func TestNonFiniteAndZeroVectorsAreRejected(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	v := NewInMemoryVector(logger.NewLogger("test"))
	require.NoError(t, v.Init(ctx, vector.Metadata{}))
	require.NoError(t, v.CreateCollection(ctx, &vector.CreateCollectionRequest{Collection: "c", Dimensions: 2}))

	resp, err := v.Upsert(ctx, &vector.UpsertRequest{Collection: "c", Records: []vector.Record{
		{ID: "ok", Values: []float32{1, 0}},
		{ID: "nan", Values: []float32{float32(math.NaN()), 0}},
		{ID: "zero", Values: []float32{0, 0}},
	}})
	require.NoError(t, err)
	assert.Len(t, resp.FailedItems, 2)

	_, err = v.Query(ctx, &vector.QueryRequest{Collection: "c", Vector: &vector.Record{Values: []float32{float32(math.Inf(1)), 0}}, TopK: 1})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
