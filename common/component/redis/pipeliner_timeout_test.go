/*
Copyright 2021 The Dapr Authors
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

package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
)

// TestPipelinerDoQueuesOnceWithWriteTimeout is the regression test for the
// double-queue bug: Pipeliner.Do used to queue the command TWICE when
// writeTimeout was set (the writeTimeout branch was missing a return, so the
// unbounded call below it also ran). Every pipelined command — including the
// state store's transactional CAS EVALs — then executed twice per Exec,
// inflating versions and breaking ETag/first-write semantics for any
// component configured with writeTimeout (Dapr Workflow actor state being the
// visible casualty: "ERR user_script:14: failed to set key").
func TestPipelinerDoQueuesOnceWithWriteTimeout(t *testing.T) {
	ctx := context.Background()

	run := func(t *testing.T, s *Settings) {
		t.Helper()
		mr := miniredis.RunT(t)
		s.Host = mr.Addr()
		s.RedisType = NodeType

		clients := map[string]func(*Settings) (RedisClient, error){
			"v8": newV8Client,
			"v9": newV9Client,
		}
		for name, newClient := range clients {
			t.Run(name, func(t *testing.T) {
				c, err := newClient(s)
				require.NoError(t, err)
				defer c.Close()

				mr.FlushAll()
				pipe := c.TxPipeline()
				pipe.Do(ctx, "INCR", "counter")
				require.NoError(t, pipe.Exec(ctx))

				got, err := c.Get(ctx, "counter")
				require.NoError(t, err)
				require.Equal(t, "1", got,
					"a single pipelined INCR must execute exactly once (twice means Do double-queued)")
			})
		}
	}

	t.Run("writeTimeout set (the arming condition)", func(t *testing.T) {
		run(t, &Settings{WriteTimeout: Duration(3 * time.Second)})
	})
	t.Run("writeTimeout unset", func(t *testing.T) {
		run(t, &Settings{})
	})
}
