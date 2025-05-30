//go:build conftests
// +build conftests

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

package clickhouse

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/clickhouse"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/kit/logger"
)

const (  
	componentName = "clickhouse-store"
)

func TestClickHouseStateStore(t *testing.T) {
	flow.New(t, "Test ClickHouse state store certification").  
		Step(dockercompose.Run("docker-compose.yml")).  
		Step("verify clickhouse store operations", testClickHouseStateStore()).  
		Run()
}

func testClickHouseStateStore() flow.Runnable {
	return func(ctx flow.Context) error {
		// Create a new ClickHouse state store instance
		store := clickhouse.NewClickHouseStateStore(logger.NewLogger("clickhouse-store-test"))
		
		// Initialize the state store
		metadata := state.Metadata{}
		metadata.Properties = map[string]string{
			"clickhouseURL": "tcp://localhost:9000",
			"databaseName": "dapr_test",
			"tableName":    "state_test",
			"username":     "default",
			"password":     "",
		}
		
		err := store.Init(ctx, metadata)
		require.NoError(ctx.T, err)
		
		// Set a value
		setReq := &state.SetRequest{
			Key:   "test-key",
			Value: []byte("test-value"),
		}
		err = store.Set(ctx, setReq)
		require.NoError(ctx.T, err)
		
		// Get the value
		getReq := &state.GetRequest{
			Key: "test-key",
		}
		getResp, err := store.Get(ctx, getReq)
		require.NoError(ctx.T, err)
		assert.Equal(ctx.T, "test-value", string(getResp.Data))
		
		// Delete the value
		delReq := &state.DeleteRequest{
			Key: "test-key",
		}
		err = store.Delete(ctx, delReq)
		require.NoError(ctx.T, err)
		
		// Verify the value is deleted
		getResp, err = store.Get(ctx, getReq)
		require.NoError(ctx.T, err)
		assert.Nil(ctx.T, getResp.Data)
		
		// Test TTL
		ttlSetReq := &state.SetRequest{
			Key:   "test-ttl-key",
			Value: []byte("test-ttl-value"),
			Metadata: map[string]string{
				"ttlInSeconds": "1", // 1 second TTL
			},
		}
		err = store.Set(ctx, ttlSetReq)
		require.NoError(ctx.T, err)
		
		// Wait for TTL to expire (2 seconds to be safe)
		fmt.Println("Waiting for TTL to expire...")
		flow.Sleep(2 * flow.Second)
		
		// Verify the value is expired
		ttlGetReq := &state.GetRequest{
			Key: "test-ttl-key",
		}
		ttlGetResp, err := store.Get(ctx, ttlGetReq)
		require.NoError(ctx.T, err)
		assert.Nil(ctx.T, ttlGetResp.Data)
		
		// Test ETag
		etagSetReq := &state.SetRequest{
			Key:   "test-etag-key",
			Value: []byte("test-etag-value"),
		}
		err = store.Set(ctx, etagSetReq)
		require.NoError(ctx.T, err)
		
		// Get the value with ETag
		etagGetReq := &state.GetRequest{
			Key: "test-etag-key",
		}
		etagGetResp, err := store.Get(ctx, etagGetReq)
		require.NoError(ctx.T, err)
		assert.NotNil(ctx.T, etagGetResp.ETag)
		
		// Update with correct ETag
		etagUpdateReq := &state.SetRequest{
			Key:   "test-etag-key",
			Value: []byte("test-etag-value-updated"),
			ETag:  etagGetResp.ETag,
		}
		err = store.Set(ctx, etagUpdateReq)
		require.NoError(ctx.T, err)
		
		// Update with incorrect ETag
		badETag := "bad-etag"
		etagBadUpdateReq := &state.SetRequest{
			Key:   "test-etag-key",
			Value: []byte("test-etag-value-updated-again"),
			ETag:  &badETag,
		}
		err = store.Set(ctx, etagBadUpdateReq)
		require.Error(ctx.T, err)
		
		// Clean up
		err = store.Delete(ctx, &state.DeleteRequest{Key: "test-etag-key"})
		require.NoError(ctx.T, err)
		
		return nil
	}
}
