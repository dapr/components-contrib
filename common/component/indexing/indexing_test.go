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

package indexing

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/search"
)

func TestCompileFilter(t *testing.T) {
	t.Parallel()

	doc := map[string]any{
		"category":  "sedan",
		"price":     25.0,
		"inStock":   false,
		"publisher": map[string]any{"city": "london"},
	}

	tests := []struct {
		name    string
		filter  map[string]any
		want    bool
		wantErr bool
	}{
		{name: "empty filter", filter: map[string]any{}, want: true},
		{name: "bare value", filter: map[string]any{"category": "sedan"}, want: true},
		{name: "$ne on a missing field", filter: map[string]any{"notes": map[string]any{"$ne": "x"}}, want: true},
		{name: "number range", filter: map[string]any{"price": map[string]any{"$gte": 20, "$lt": 30.0}}, want: true},
		{name: "mixed types never match", filter: map[string]any{"price": map[string]any{"$gt": "10"}}, want: false},
		{name: "$in", filter: map[string]any{"category": map[string]any{"$in": []any{"suv", "sedan"}}}, want: true},
		{name: "$nin on a missing field", filter: map[string]any{"notes": map[string]any{"$nin": []any{"x"}}}, want: true},
		{name: "$exists false", filter: map[string]any{"notes": map[string]any{"$exists": false}}, want: true},
		{name: "bare bool", filter: map[string]any{"inStock": true}, want: false},
		{name: "nested path", filter: map[string]any{"publisher.city": "london"}, want: true},
		{name: "$or $and $not", filter: map[string]any{"$or": []any{
			map[string]any{"category": "suv"},
			map[string]any{"$and": []any{map[string]any{"price": 25.0}, map[string]any{"$not": map[string]any{"inStock": true}}}},
		}}, want: true},
		{name: "$regex is rejected", filter: map[string]any{"category": map[string]any{"$regex": "se.*"}}, wantErr: true},
		{name: "unknown top-level operator", filter: map[string]any{"$near": map[string]any{"a": 1}}, wantErr: true},
		{name: "$in without an array", filter: map[string]any{"category": map[string]any{"$in": "sedan"}}, wantErr: true},
		{name: "empty $or", filter: map[string]any{"$or": []any{}}, wantErr: true},
		{name: "empty field condition", filter: map[string]any{"category": map[string]any{}}, wantErr: true},
		// An invalid branch is rejected even when an earlier branch already
		// decides the result.
		{name: "invalid branch behind a match", filter: map[string]any{"$or": []any{map[string]any{"category": "sedan"}, map[string]any{"x": map[string]any{"$like": 1}}}}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			match, err := CompileFilter(tt.filter)
			if tt.wantErr {
				assert.Equal(t, codes.InvalidArgument, status.Code(err))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, match(doc))
		})
	}
}

func TestApplyWithDelay(t *testing.T) {
	t.Parallel()

	wait := func(action search.IndexingWaitTimeoutAction) search.IndexingOptions {
		return search.IndexingOptions{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: 10 * time.Millisecond, OnWaitTimeout: action}
	}

	t.Run("fail request reports deadline exceeded and the write still lands", func(t *testing.T) {
		t.Parallel()
		var applied atomic.Bool
		_, err := Apply(t.Context(), wait(search.IndexingWaitTimeoutActionFailRequest), 200*time.Millisecond, func() { applied.Store(true) })
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
		assert.False(t, applied.Load())
		assert.Eventually(t, applied.Load, 5*time.Second, 10*time.Millisecond)
	})

	t.Run("continue async reports queued", func(t *testing.T) {
		t.Parallel()
		var applied atomic.Bool
		ack, err := Apply(t.Context(), wait(search.IndexingWaitTimeoutActionContinueAsync), 200*time.Millisecond, func() { applied.Store(true) })
		require.NoError(t, err)
		assert.Equal(t, search.IndexAckQueued, ack)
		assert.Eventually(t, applied.Load, 5*time.Second, 10*time.Millisecond)
	})

	t.Run("a long enough wait completes", func(t *testing.T) {
		t.Parallel()
		opts := wait(search.IndexingWaitTimeoutActionFailRequest)
		opts.WaitTimeout = 5 * time.Second
		ack, err := Apply(t.Context(), opts, 10*time.Millisecond, func() {})
		require.NoError(t, err)
		assert.Equal(t, search.IndexAckCompleted, ack)
	})

	t.Run("a cancelled context is reported as cancelled", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := Apply(ctx, search.IndexingOptions{}, 0, func() {})
		assert.Equal(t, codes.Canceled, status.Code(err))
	})
}
