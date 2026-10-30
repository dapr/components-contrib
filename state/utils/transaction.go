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
	"fmt"

	"github.com/dapr/components-contrib/state"
)

// KeyOp applies the last op on a key, guarded by the first op's etag or first-write option.
type KeyOp struct {
	Op    state.TransactionalStateOperation
	Guard state.TransactionalStateOperation
}

// CollapseOps returns one KeyOp per key, ordered by each key's last op.
// A later op can't be checked against the stored value, so one with an etag, or a first-write set after a set, is an ETagMismatch.
func CollapseOps(ops []state.TransactionalStateOperation) ([]KeyOp, error) {
	first := make(map[string]int, len(ops))
	last := make(map[string]int, len(ops))
	for i, o := range ops {
		key := o.GetKey()
		prev, seen := last[key]
		last[key] = i
		if !seen {
			first[key] = i
			continue
		}

		var conflict bool
		switch req := o.(type) {
		case state.SetRequest:
			_, prevSet := ops[prev].(state.SetRequest)
			conflict = req.HasETag() || (prevSet && req.Options.Concurrency == state.FirstWrite)
		case state.DeleteRequest:
			conflict = req.HasETag()
		}
		if conflict {
			return nil, state.NewETagError(state.ETagMismatch, fmt.Errorf("key %s was already written earlier in the transaction", key))
		}
	}

	res := make([]KeyOp, 0, len(last))
	for i, o := range ops {
		key := o.GetKey()
		if last[key] == i {
			res = append(res, KeyOp{Op: o, Guard: ops[first[key]]})
		}
	}
	return res, nil
}
