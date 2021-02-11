// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------
package file

import (
	"fmt"
	"testing"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
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
			"SecretsFile":     "a",
			"NestedSeparator": "a",
		}
		err := s.Init(m)
		assert.Nil(t, err)
	})

	t.Run("Init with missing metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"Dummy": "a",
		}
		err := s.Init(m)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("missing local secrets file in metadata"))
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
			"SecretsFile":     "a",
			"NestedSeparator": ".",
		}
		err := s.Init(m)
		assert.Nil(t, err)

		req := secretstores.GetSecretRequest{
			Name:     "root.key1",
			Metadata: map[string]string{},
		}
		output, err := s.GetSecret(req)
		assert.Nil(t, err)
		assert.Equal(t, "value1", output.Data[req.Name])
	})

	t.Run("Init with default separator", func(t *testing.T) {
		m.Properties = map[string]string{
			"SecretsFile": "a",
		}
		err := s.Init(m)
		assert.Nil(t, err)

		req := secretstores.GetSecretRequest{
			Name:     "root:key2",
			Metadata: map[string]string{},
		}
		output, err := s.GetSecret(req)
		assert.Nil(t, err)
		assert.Equal(t, "value2", output.Data[req.Name])
	})
}

func TestGetSecret(t *testing.T) {
	m := secretstores.Metadata{}
	m.Properties = map[string]string{
		"SecretsFile":     "a",
		"NestedSeparator": "a",
	}
	s := localSecretStore{
		logger: logger.NewLogger("test"),
		readLocalFileFn: func(secretsFile string) (map[string]interface{}, error) {
			secrets := make(map[string]interface{})
			secrets["secret"] = secretValue

			return secrets, nil
		},
	}
	s.Init(m)

	t.Run("successfully retrieve secrets", func(t *testing.T) {
		req := secretstores.GetSecretRequest{
			Name:     "secret",
			Metadata: map[string]string{},
		}
		output, e := s.GetSecret(req)
		assert.Nil(t, e)
		assert.Equal(t, "secret", output.Data[req.Name])
	})

	t.Run("unsuccessfully retrieve secret", func(t *testing.T) {
		req := secretstores.GetSecretRequest{
			Name:     "NoSecret",
			Metadata: map[string]string{},
		}
		_, err := s.GetSecret(req)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("secret %s not found", req.Name))
	})
}

func TestBulkGetSecret(t *testing.T) {
	m := secretstores.Metadata{}
	m.Properties = map[string]string{
		"SecretsFile":     "a",
		"NestedSeparator": "a",
	}
	s := localSecretStore{
		logger: logger.NewLogger("test"),
		readLocalFileFn: func(secretsFile string) (map[string]interface{}, error) {
			secrets := make(map[string]interface{})
			secrets["secret"] = secretValue

			return secrets, nil
		},
	}
	s.Init(m)

	t.Run("successfully retrieve secrets", func(t *testing.T) {
		req := secretstores.BulkGetSecretRequest{}
		output, e := s.BulkGetSecret(req)
		assert.Nil(t, e)
		assert.Equal(t, "secret", output.Data["secret"]["secret"])
	})
}
