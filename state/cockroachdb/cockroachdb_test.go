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

package cockroachdb

import (
	"testing"

	"github.com/stretchr/testify/assert"

	postgresql "github.com/dapr/components-contrib/common/component/postgresql/v1"
	"github.com/dapr/components-contrib/state"
)

func TestSetQueryUpsertIncrementsExistingETag(t *testing.T) {
	opts := postgresql.SetQueryOptions{TableName: "state", ExpireDateValue: "NULL"}

	for _, concurrency := range []string{state.LastWrite, state.FirstWrite} {
		t.Run(concurrency, func(t *testing.T) {
			query := setQuery(&state.SetRequest{
				Key:     "key",
				Options: state.SetStateOption{Concurrency: concurrency},
			}, opts)

			assert.Contains(t, query, "ON CONFLICT (key) DO UPDATE SET")
			assert.Contains(t, query, "etag = t.etag + 1,")
			assert.NotContains(t, query, "EXCLUDED.etag")
		})
	}
}
