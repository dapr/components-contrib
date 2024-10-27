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

package immudb

import (
	"context"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	immudb "github.com/codenotary/immudb/pkg/client"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockImmuClient struct {
	immudb.ImmuClient
	verifiedGetFunc func(ctx context.Context, key []byte, opts ...immudb.GetOption) (*schema.Entry, error)
	verifiedSetFunc func(ctx context.Context, key []byte, value []byte) (*schema.TxHeader, error)
	deleteFunc      func(ctx context.Context, key *schema.DeleteKeysRequest) (*schema.TxHeader, error)
}

func (m *mockImmuClient) VerifiedGet(ctx context.Context, key []byte, opts ...immudb.GetOption) (*schema.Entry, error) {
	return m.verifiedGetFunc(ctx, key)
}

func (m *mockImmuClient) VerifiedSet(ctx context.Context, key []byte, value []byte) (*schema.TxHeader, error) {
	return m.verifiedSetFunc(ctx, key, value)
}

func (m *mockImmuClient) Delete(ctx context.Context, key *schema.DeleteKeysRequest) (*schema.TxHeader, error) {
	return m.deleteFunc(ctx, key)
}

func TestGetImmudbMetadata(t *testing.T) {
	t.Run("Valid metadata", func(t *testing.T) {
		properties := map[string]string{
			"host":     "localhost",
			"port":     "3322",
			"username": "immudb",
			"password": "immudb",
			"database": "defaultdb",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		meta, err := getImmudbMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, "localhost", meta.Host)
		assert.Equal(t, 3322, meta.Port)
		assert.Equal(t, "immudb", meta.Username)
		assert.Equal(t, "immudb", meta.Password)
		assert.Equal(t, "defaultdb", meta.Database)
	})

	t.Run("Missing required fields", func(t *testing.T) {
		properties := map[string]string{
			"port": "3322",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		_, err := getImmudbMetadata(m)
		require.Error(t, err)
	})
}

func TestImmudbStateStoreOperations(t *testing.T) {
	// Create a new store instance
	store := &ImmudbStateStore{
		logger: logger.NewLogger("test"),
	}
	// Initialize BulkStore
	store.BulkStore = state.NewDefaultBulkStore(store)

	// Set up mock client
	mockClient := &mockImmuClient{}
	store.client = mockClient

	t.Run("Test Get", func(t *testing.T) {
		mockClient.verifiedGetFunc = func(ctx context.Context, key []byte, opts ...immudb.GetOption) (*schema.Entry, error) {
			return &schema.Entry{
				Key:   []byte("testKey"),
				Value: []byte("testValue"),
			}, nil
		}

		resp, err := store.Get(context.Background(), &state.GetRequest{Key: "testKey"})
		require.NoError(t, err)
		assert.Equal(t, []byte("testValue"), resp.Data)
	})

	t.Run("Test Set", func(t *testing.T) {
		mockClient.verifiedSetFunc = func(ctx context.Context, key []byte, value []byte) (*schema.TxHeader, error) {
			return &schema.TxHeader{}, nil
		}

		err := store.Set(context.Background(), &state.SetRequest{Key: "testKey", Value: []byte("testValue")})
		require.NoError(t, err)
	})

	t.Run("Test Delete", func(t *testing.T) {
		mockClient.deleteFunc = func(ctx context.Context, key *schema.DeleteKeysRequest) (*schema.TxHeader, error) {
			return &schema.TxHeader{}, nil
		}

		err := store.Delete(context.Background(), &state.DeleteRequest{Key: "testKey"})
		require.NoError(t, err)
	})
}
