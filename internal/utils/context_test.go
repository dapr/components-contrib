/*
Copyright 2023 The Dapr Authors
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

package utils

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_ContextPool(t *testing.T) {
	t.Run("a pool with no context will always be done", func(t *testing.T) {
		t.Parallel()
		pool := NewContextPool()
		select {
		case <-pool.Done():
		case <-time.After(time.Second):
			t.Error("expected context pool to be cancelled")
		}
	})

	t.Run("a cancelled context given to pool, should have pool cancelled", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		pool := NewContextPool(ctx)
		select {
		case <-pool.Done():
		case <-time.After(time.Second):
			t.Error("expected context pool to be cancelled")
		}
	})

	t.Run("a cancelled context given to pool, given a new context, should still have pool cancelled", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		pool := NewContextPool(ctx)
		pool.Add(context.Background())
		select {
		case <-pool.Done():
		case <-time.After(time.Second):
			t.Error("expected context pool to be cancelled")
		}
	})

	t.Run("pool with multiple contexts should return once all contexts have been cancelled", func(t *testing.T) {
		t.Parallel()
		var ctx [50]context.Context
		var cancel [50]context.CancelFunc

		ctx[0], cancel[0] = context.WithCancel(context.Background())
		pool := NewContextPool(ctx[0])

		for i := 1; i < 50; i++ {
			ctx[i], cancel[i] = context.WithCancel(context.Background())
			pool.Add(ctx[i])
		}

		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(ctx), func(i, j int) {
			ctx[i], ctx[j] = ctx[j], ctx[i]
			cancel[i], cancel[j] = cancel[j], cancel[i]
		})

		for i := 0; i < 50; i++ {
			select {
			case <-pool.Done():
				t.Error("expected context to not be cancelled")
			case <-time.After(time.Millisecond):
			}
			cancel[i]()
		}

		select {
		case <-pool.Done():
		case <-time.After(time.Second):
			t.Error("expected context pool to be cancelled")
		}
	})

	t.Run("pool size will not increase if the given contexts have been cancelled", func(t *testing.T) {
		t.Parallel()

		ctx1, cancel1 := context.WithCancel(context.Background())
		ctx2, cancel2 := context.WithCancel(context.Background())
		pool := NewContextPool(ctx1, ctx2)
		assert.Equal(t, 2, pool.Size())

		cancel1()
		cancel2()
		select {
		case <-pool.Done():
		case <-time.After(time.Second):
			t.Error("expected context pool to be cancelled")
		}
		pool.Add(context.Background())
		assert.Equal(t, 2, pool.Size())
	})

	t.Run("pool size will not increase if the pool has been closed", func(t *testing.T) {
		t.Parallel()

		ctx1, cancel1 := context.WithCancel(context.Background())
		ctx2, cancel2 := context.WithCancel(context.Background())
		pool := NewContextPool(ctx1, ctx2)
		assert.Equal(t, 2, pool.Size())
		pool.Close()
		pool.Add(context.Background())
		assert.Equal(t, 2, pool.Size())
		cancel1()
		cancel2()
		select {
		case <-pool.Done():
		case <-time.After(time.Second):
			t.Error("expected context pool to be cancelled")
		}
	})
}
