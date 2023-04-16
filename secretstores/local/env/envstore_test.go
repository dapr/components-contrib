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

package env

import (
	"context"
	"os"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

func TestEnvStore(t *testing.T) {
	s := envSecretStore{logger: logger.NewLogger("test")}

	t.Setenv("TEST_SECRET", "secret1")
	require.Equal(t, "secret1", os.Getenv("TEST_SECRET"))

	t.Run("Init", func(t *testing.T) {
		err := s.Init(context.Background(), secretstores.Metadata{})
		require.NoError(t, err)
	})

	t.Run("Get", func(t *testing.T) {
		resp, err := s.GetSecret(context.Background(), secretstores.GetSecretRequest{Name: "TEST_SECRET"})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "secret1", resp.Data["TEST_SECRET"])
	})

	if runtime.GOOS != "windows" {
		t.Run("Get is case-sensitive on *nix", func(t *testing.T) {
			resp, err := s.GetSecret(context.Background(), secretstores.GetSecretRequest{Name: "test_secret"})
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Empty(t, resp.Data["test_secret"])
		})
	} else {
		t.Run("Get is case-insensitive on Windows", func(t *testing.T) {
			resp, err := s.GetSecret(context.Background(), secretstores.GetSecretRequest{Name: "test_secret"})
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, "secret1", resp.Data["test_secret"])
		})
	}

	t.Run("Bulk get", func(t *testing.T) {
		resp, err := s.BulkGetSecret(context.Background(), secretstores.BulkGetSecretRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "secret1", resp.Data["TEST_SECRET"]["TEST_SECRET"])
	})

	t.Run("Disallowed keys", func(t *testing.T) {
		t.Setenv("APP_API_TOKEN", "ciao")
		t.Setenv("DAPR_API_TOKEN", "mondo")
		t.Setenv("dapr_notallowed", "notallowed")
		t.Setenv("FOO", "bar")

		t.Run("Get", func(t *testing.T) {
			resp, err := s.GetSecret(context.Background(), secretstores.GetSecretRequest{
				Name: "APP_API_TOKEN",
			})
			require.NoError(t, err)
			require.NotNil(t, resp.Data)
			assert.Empty(t, resp.Data["APP_API_TOKEN"])

			resp, err = s.GetSecret(context.Background(), secretstores.GetSecretRequest{
				Name: "dapr_notallowed",
			})
			require.NoError(t, err)
			require.NotNil(t, resp.Data)
			assert.Empty(t, resp.Data["dapr_notallowed"])

			resp, err = s.GetSecret(context.Background(), secretstores.GetSecretRequest{
				Name: "DAPR_NOTALLOWED",
			})
			require.NoError(t, err)
			require.NotNil(t, resp.Data)
			assert.Empty(t, resp.Data["DAPR_NOTALLOWED"])
		})

		t.Run("Bulk get", func(t *testing.T) {
			resp, err := s.BulkGetSecret(context.Background(), secretstores.BulkGetSecretRequest{})
			require.NoError(t, err)
			require.NotNil(t, resp.Data)
			assert.Empty(t, resp.Data["APP_API_TOKEN"])
			assert.Empty(t, resp.Data["DAPR_API_TOKEN"])
			assert.Empty(t, resp.Data["DAPR_NOTALLOWED"])
			assert.Equal(t, "bar", resp.Data["FOO"]["FOO"])
		})
	})
}

func TestEnvStoreWithPrefix(t *testing.T) {
	s := envSecretStore{logger: logger.NewLogger("test")}

	t.Setenv("TEST_SECRET", "test1")
	t.Setenv("test_secret2", "test2")
	t.Setenv("FOO_SECRET", "foo1")
	require.Equal(t, "test1", os.Getenv("TEST_SECRET"))
	require.Equal(t, "test2", os.Getenv("test_secret2"))
	require.Equal(t, "foo1", os.Getenv("FOO_SECRET"))

	t.Run("Get", func(t *testing.T) {
		err := s.Init(context.Background(), secretstores.Metadata{
			Base: metadata.Base{Properties: map[string]string{
				"prefix": "TEST_",
			}},
		})
		require.NoError(t, err)

		resp, err := s.GetSecret(context.Background(), secretstores.GetSecretRequest{Name: "SECRET"})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.Data, 1)
		assert.Equal(t, "test1", resp.Data["SECRET"])
	})

	t.Run("Bulk get", func(t *testing.T) {
		err := s.Init(context.Background(), secretstores.Metadata{
			Base: metadata.Base{Properties: map[string]string{
				// Prefix should be case-sensitive on *nix and case-insensitive on Windows
				"prefix": "TEST_",
			}},
		})
		require.NoError(t, err)

		resp, err := s.BulkGetSecret(context.Background(), secretstores.BulkGetSecretRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp)
		if runtime.GOOS != "windows" {
			assert.Len(t, resp.Data, 1)
			assert.Equal(t, "test1", resp.Data["SECRET"]["SECRET"])
		} else {
			assert.Len(t, resp.Data, 2)
			assert.Equal(t, "test1", resp.Data["SECRET"]["SECRET"])
			assert.Equal(t, "test2", resp.Data["secret2"]["secret2"])
		}
	})

	t.Run("Disallowed keys", func(t *testing.T) {
		t.Setenv("DAPR_API_TOKEN", "hi")

		t.Run("Get", func(t *testing.T) {
			err := s.Init(context.Background(), secretstores.Metadata{
				Base: metadata.Base{Properties: map[string]string{
					"prefix": "DAPR_",
				}},
			})
			require.NoError(t, err)

			resp, err := s.GetSecret(context.Background(), secretstores.GetSecretRequest{
				Name: "API_TOKEN",
			})
			require.NoError(t, err)
			require.NotNil(t, resp.Data)
			assert.Empty(t, resp.Data["API_TOKEN"])
		})

		t.Run("Get case insensitive", func(t *testing.T) {
			err := s.Init(context.Background(), secretstores.Metadata{
				Base: metadata.Base{Properties: map[string]string{
					"prefix": "dapr_",
				}},
			})
			require.NoError(t, err)

			resp, err := s.GetSecret(context.Background(), secretstores.GetSecretRequest{
				Name: "API_TOKEN",
			})
			require.NoError(t, err)
			require.NotNil(t, resp.Data)
			assert.Empty(t, resp.Data["API_TOKEN"])
		})

		t.Run("Bulk get", func(t *testing.T) {
			resp, err := s.BulkGetSecret(context.Background(), secretstores.BulkGetSecretRequest{})
			require.NoError(t, err)
			require.NotNil(t, resp.Data)
			assert.Empty(t, resp.Data["API_TOKEN"])
		})

		t.Run("Bulk get case insensitive", func(t *testing.T) {
			resp, err := s.BulkGetSecret(context.Background(), secretstores.BulkGetSecretRequest{})
			require.NoError(t, err)
			require.NotNil(t, resp.Data)
			assert.Empty(t, resp.Data["API_TOKEN"])
		})
	})
}

func TestGetFeatures(t *testing.T) {
	s := envSecretStore{logger: logger.NewLogger("test")}
	// Yes, we are skipping initialization as feature retrieval doesn't depend on it.
	t.Run("no features are advertised", func(t *testing.T) {
		f := s.Features()
		assert.Empty(t, f)
	})
}
