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
package file

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

const secretValue = "secret"

func TestInit(t *testing.T) {
	m := secretstores.Metadata{}
	s := localSecretStore{
		logger: logger.NewLogger("test"),
		readLocalFileFn: func(secretsFile string) (map[string]interface{}, error) {
			return nil, nil
		},
	}
	t.Run("Init with valid metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"secretsFile":     "a",
			"nestedSeparator": "a",
		}
		err := s.Init(context.Background(), m)
		require.NoError(t, err)
	})

	t.Run("Init with missing metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"dummy": "a",
		}
		err := s.Init(context.Background(), m)
		require.Error(t, err)
		assert.Equal(t, err, errors.New("missing local secrets file in metadata"))
	})
}

func TestSeparator(t *testing.T) {
	m := secretstores.Metadata{}
	s := localSecretStore{
		logger: logger.NewLogger("test"),
		readLocalFileFn: func(secretsFile string) (map[string]interface{}, error) {
			return map[string]interface{}{
				"root": map[string]interface{}{
					"key1": "value1",
					"key2": "value2",
				},
			}, nil
		},
	}
	t.Run("Init with custom separator", func(t *testing.T) {
		m.Properties = map[string]string{
			"secretsFile":     "a",
			"nestedSeparator": ".",
		}
		err := s.Init(context.Background(), m)
		require.NoError(t, err)

		req := secretstores.GetSecretRequest{
			Name:     "root.key1",
			Metadata: map[string]string{},
		}
		output, err := s.GetSecret(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, "value1", output.Data[req.Name])
	})

	t.Run("Init with default separator", func(t *testing.T) {
		m.Properties = map[string]string{
			"secretsFile": "a",
		}
		err := s.Init(context.Background(), m)
		require.NoError(t, err)

		req := secretstores.GetSecretRequest{
			Name:     "root:key2",
			Metadata: map[string]string{},
		}
		output, err := s.GetSecret(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, "value2", output.Data[req.Name])
	})
}

func TestGetSecret(t *testing.T) {
	m := secretstores.Metadata{}
	m.Properties = map[string]string{
		"secretsFile":     "a",
		"nestedSeparator": "a",
	}
	s := localSecretStore{
		logger: logger.NewLogger("test"),
		readLocalFileFn: func(secretsFile string) (map[string]interface{}, error) {
			secrets := make(map[string]interface{})
			secrets["secret"] = secretValue

			return secrets, nil
		},
	}
	s.Init(context.Background(), m)

	t.Run("successfully retrieve secrets", func(t *testing.T) {
		req := secretstores.GetSecretRequest{
			Name:     "secret",
			Metadata: map[string]string{},
		}
		output, e := s.GetSecret(context.Background(), req)
		require.NoError(t, e)
		assert.Equal(t, "secret", output.Data[req.Name])
	})

	t.Run("unsuccessfully retrieve secret", func(t *testing.T) {
		req := secretstores.GetSecretRequest{
			Name:     "NoSecret",
			Metadata: map[string]string{},
		}
		_, err := s.GetSecret(context.Background(), req)
		require.Error(t, err)
		assert.Equal(t, err, fmt.Errorf("secret %s not found", req.Name))
	})

	t.Run("Regular (non-MultiValued) secret store does not support MULTIPLE_KEY_VALUES_PER_SECRET", func(t *testing.T) {
		assert.False(t, secretstores.FeatureMultipleKeyValuesPerSecret.IsPresent(s.Features()))
	})
}

func TestBulkGetSecret(t *testing.T) {
	m := secretstores.Metadata{}
	m.Properties = map[string]string{
		"secretsFile":     "a",
		"nestedSeparator": "a",
	}
	s := localSecretStore{
		logger: logger.NewLogger("test"),
		readLocalFileFn: func(secretsFile string) (map[string]interface{}, error) {
			secrets := make(map[string]interface{})
			secrets["secret"] = secretValue

			return secrets, nil
		},
	}
	s.Init(context.Background(), m)

	t.Run("successfully retrieve secrets", func(t *testing.T) {
		req := secretstores.BulkGetSecretRequest{}
		output, e := s.BulkGetSecret(context.Background(), req)
		require.NoError(t, e)
		assert.Equal(t, "secret", output.Data["secret"]["secret"])
	})
}

func TestMultiValuedSecrets(t *testing.T) {
	m := secretstores.Metadata{}
	m.Properties = map[string]string{
		"secretsFile": "a",
		"multiValued": "true",
	}
	s := localSecretStore{
		logger: logger.NewLogger("test"),
		readLocalFileFn: func(secretsFile string) (map[string]interface{}, error) {
			//nolint:gosec
			secretsJSON := `
			{
				"parent": {
					"child1": "12345",
					"child2": {
						"child3": "67890",
						"child4": "00000"
					}
				}
			}
			`
			var secrets map[string]interface{}
			err := json.Unmarshal([]byte(secretsJSON), &secrets)

			return secrets, err
		},
	}
	err := s.Init(context.Background(), m)
	require.NoError(t, err)

	t.Run("MultiValued stores support MULTIPLE_KEY_VALUES_PER_SECRET", func(t *testing.T) {
		assert.True(t, secretstores.FeatureMultipleKeyValuesPerSecret.IsPresent(s.Features()))
	})

	t.Run("successfully retrieve a single multi-valued secret", func(t *testing.T) {
		req := secretstores.GetSecretRequest{
			Name: "parent",
		}
		resp, err := s.GetSecret(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{
			"child1":        "12345",
			"child2:child3": "67890",
			"child2:child4": "00000",
		}, resp.Data)
	})

	t.Run("successfully retrieve multi-valued secrets", func(t *testing.T) {
		req := secretstores.BulkGetSecretRequest{}
		resp, err := s.BulkGetSecret(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, map[string]map[string]string{
			"parent": {
				"child1":        "12345",
				"child2:child3": "67890",
				"child2:child4": "00000",
			},
		}, resp.Data)
	})
}
