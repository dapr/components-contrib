/*
Copyright 2022 The Dapr Authors
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

package keyring

import (
	"errors"
	"fmt"
	"testing"

	"github.com/99designs/keyring"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

func TestInit(t *testing.T) {
	m := secretstores.Metadata{}
	s := keyringSecretStore{
		logger: logger.NewLogger("test"),
	}
	t.Run("Init with invalid backend", func(t *testing.T) {
		backendType := "test"
		m.Properties = map[string]string{
			"backendType": backendType,
		}
		err := s.Init(m)
		assert.Equal(t, err, fmt.Errorf("invalid keyring backend type %s", backendType))
	})

	t.Run("Init with backendType is keychain and serviceName is empty", func(t *testing.T) {
		m.Properties = map[string]string{
			"backendType": string(keyring.KeychainBackend),
			"serviceName": "",
		}
		err := s.Init(m)
		assert.Equal(t, err, errors.New("metadata serviceName is required for keychain backend"))
	})

	t.Run("Init with backendType is file and fileDir is empty", func(t *testing.T) {
		m.Properties = map[string]string{
			"backendType": string(keyring.FileBackend),
			"fileDir":     "",
		}
		err := s.Init(m)
		assert.Equal(t, err, errors.New("metadata fileDir is required for file backend"))
	})

	t.Run("Init with valid metadata", func(t *testing.T) {
		backendType := keyring.FileBackend
		m.Properties = map[string]string{
			"backendType": string(backendType),
			"fileDir":     "./",
		}
		err := s.Init(m)
		assert.Nil(t, err)
	})
}

func TestGetSecret(t *testing.T) {
	t.Run("GetSecret with success", func(t *testing.T) {
		s := keyringSecretStore{
			logger: logger.NewLogger("test"),
			ring: keyring.NewArrayKeyring([]keyring.Item{
				{Key: "k1", Data: []byte("v1")},
				{Key: "k2", Data: []byte("v2")},
			}),
		}

		resp, err := s.GetSecret(secretstores.GetSecretRequest{Name: "k1"})
		assert.Nil(t, err)
		assert.Equal(t, resp.Data["k1"], "v1")
	})
}

func TestBulkGetSecret(t *testing.T) {
	s := keyringSecretStore{
		logger: logger.NewLogger("test"),
		ring: keyring.NewArrayKeyring([]keyring.Item{
			{Key: "k1", Data: []byte("v1")},
			{Key: "k2", Data: []byte("v2")},
		}),
	}

	resp, err := s.BulkGetSecret(secretstores.BulkGetSecretRequest{})
	assert.Nil(t, err)
	assert.Equal(t, resp.Data, map[string]map[string]string{
		"k1": {"k1": "v1"},
		"k2": {"k2": "v2"},
	})
}
