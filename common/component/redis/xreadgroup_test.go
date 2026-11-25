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

package redis

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestXReadGroupBlockOutlastsReadTimeout checks that an idle blocking read returns an empty
// result once BLOCK elapses, instead of failing because the read timeout expired first.
func TestXReadGroupBlockOutlastsReadTimeout(t *testing.T) {
	clients := []struct {
		name string
		new  func(*Settings) (RedisClient, error)
	}{{"v8", newV8Client}, {"v9", newV9Client}}

	for _, c := range clients {
		t.Run(c.name, func(t *testing.T) {
			srv := miniredis.RunT(t)
			client, err := c.new(&Settings{Host: srv.Addr(), ReadTimeout: Duration(100 * time.Millisecond)})
			require.NoError(t, err)
			t.Cleanup(func() { client.Close() })

			require.NoError(t, client.XGroupCreateMkStream(t.Context(), "stream", "group", "0"))

			const block = 300 * time.Millisecond
			start := time.Now()
			res, err := client.XReadGroupResult(t.Context(), "group", "consumer", []string{"stream", ">"}, 1, block)
			elapsed := time.Since(start)

			require.Error(t, err)
			assert.True(t, client.IsNilValueError(err), "expected an empty read, got %v", err)
			assert.Empty(t, res)
			assert.GreaterOrEqual(t, elapsed, block)
		})
	}
}
