// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------
package jsonsecretstore

import (
	"fmt"
	"os"
	"testing"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

const secretValue = "secret"

func TestInit(t *testing.T) {
	os.Setenv(enableSecretStoreVariable, "1")
	m := secretstores.Metadata{}
	s := jsonSecretStore{
		logger: logger.NewLogger("test"),
		readJSONFileFn: func(secretsFile string) (map[string]interface{}, error) {
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
		assert.Equal(t, err, fmt.Errorf("missing JSON secrets file in metadata"))
	})

	t.Run("Init with disabled store", func(t *testing.T) {
		os.Setenv(enableSecretStoreVariable, "0")
		m.Properties = map[string]string{
			"Dummy": "a",
		}
		err := s.Init(m)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("jsonsecretstore must be explicitly enabled setting %s environment variable value to 1", enableSecretStoreVariable))
	})
}

func TestGetSecret(t *testing.T) {
	os.Setenv(enableSecretStoreVariable, "1")
	m := secretstores.Metadata{}
	m.Properties = map[string]string{
		"SecretsFile":     "a",
		"NestedSeparator": "a",
	}
	s := jsonSecretStore{
		logger: logger.NewLogger("test"),
		readJSONFileFn: func(secretsFile string) (map[string]interface{}, error) {
			secrets := make(map[string]interface{})
			secrets["secret"] = secretValue
			return secrets, nil
		},
	}
	s.Init(m)

	t.Run("successfully retrieve secret", func(t *testing.T) {
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
