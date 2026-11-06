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

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/ptr"
)

func TestCollapseOps(t *testing.T) {
	firstWrite := state.SetStateOption{Concurrency: state.FirstWrite}

	t.Run("one op per key is unchanged", func(t *testing.T) {
		a := state.SetRequest{Key: "a", Value: "v", ETag: ptr.Of("1")}
		b := state.DeleteRequest{Key: "b"}
		got, err := CollapseOps([]state.TransactionalStateOperation{a, b})
		require.NoError(t, err)
		assert.Equal(t, []KeyOp{{Op: a, Guard: a}, {Op: b, Guard: b}}, got)
	})

	t.Run("last op is applied with the first op as guard", func(t *testing.T) {
		for name, tc := range map[string]struct {
			ops   []state.TransactionalStateOperation
			op    int
			guard int
		}{
			"set then set": {
				ops: []state.TransactionalStateOperation{
					state.SetRequest{Key: "a", Value: "v1", ETag: ptr.Of("1")},
					state.SetRequest{Key: "a", Value: "v2"},
				},
				op: 1, guard: 0,
			},
			"first-write set then delete": {
				ops: []state.TransactionalStateOperation{
					state.SetRequest{Key: "a", Value: "v1", Options: firstWrite},
					state.DeleteRequest{Key: "a"},
				},
				op: 1, guard: 0,
			},
			"delete then first-write set": {
				ops: []state.TransactionalStateOperation{
					state.DeleteRequest{Key: "a", ETag: ptr.Of("1")},
					state.SetRequest{Key: "a", Value: "v1", Options: firstWrite},
				},
				op: 1, guard: 0,
			},
			"set, delete, first-write set": {
				ops: []state.TransactionalStateOperation{
					state.SetRequest{Key: "a", Value: "v1"},
					state.DeleteRequest{Key: "a"},
					state.SetRequest{Key: "a", Value: "v2", Options: firstWrite},
				},
				op: 2, guard: 0,
			},
		} {
			t.Run(name, func(t *testing.T) {
				got, err := CollapseOps(tc.ops)
				require.NoError(t, err)
				assert.Equal(t, []KeyOp{{Op: tc.ops[tc.op], Guard: tc.ops[tc.guard]}}, got)
			})
		}
	})

	t.Run("ordered by each key's last op", func(t *testing.T) {
		ops := []state.TransactionalStateOperation{
			state.SetRequest{Key: "a", Value: "v1"},
			state.SetRequest{Key: "b", Value: "v1"},
			state.SetRequest{Key: "a", Value: "v2"},
		}
		got, err := CollapseOps(ops)
		require.NoError(t, err)
		assert.Equal(t, []KeyOp{{Op: ops[1], Guard: ops[1]}, {Op: ops[2], Guard: ops[0]}}, got)
	})

	t.Run("later guarded op is a mismatch", func(t *testing.T) {
		for name, ops := range map[string][]state.TransactionalStateOperation{
			"set then etag set": {
				state.SetRequest{Key: "a", Value: "v"},
				state.SetRequest{Key: "a", Value: "w", ETag: ptr.Of("1")},
			},
			"set then first-write set": {
				state.SetRequest{Key: "a", Value: "v"},
				state.SetRequest{Key: "a", Value: "w", Options: firstWrite},
			},
			"set then etag delete": {
				state.SetRequest{Key: "a", Value: "v", ETag: ptr.Of("1")},
				state.DeleteRequest{Key: "a", ETag: ptr.Of("1")},
			},
			"delete then etag set": {
				state.DeleteRequest{Key: "a"},
				state.SetRequest{Key: "a", Value: "w", ETag: ptr.Of("1")},
			},
		} {
			t.Run(name, func(t *testing.T) {
				_, err := CollapseOps(ops)
				var etagErr *state.ETagError
				require.ErrorAs(t, err, &etagErr)
				assert.Equal(t, state.ETagMismatch, etagErr.Kind())
			})
		}
	})
}
