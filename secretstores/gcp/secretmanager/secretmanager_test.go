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

package secretmanager

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

func TestInit(t *testing.T) {
	m := secretstores.Metadata{}
	sm := NewSecreteManager(logger.NewLogger("test"))
	t.Run("Init with Wrong metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"type":                        "service_account",
			"project_id":                  "a",
			"private_key_id":              "a",
			"private_key":                 "a",
			"client_email":                "a",
			"client_id":                   "a",
			"auth_uri":                    "a",
			"token_uri":                   "a",
			"auth_provider_x509_cert_url": "a",
			"client_x509_cert_url":        "a",
		}

		err := sm.Init(m)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("failed to setup secretmanager client: google: could not parse key: private key should be a PEM or plain PKCS1 or PKCS8; parse error: asn1: syntax error: truncated tag or length"))
	})

	t.Run("Init with missing `type` metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"dummy": "a",
		}
		err := sm.Init(m)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("missing property `type` in metadata"))
	})

	t.Run("Init with missing `project_id` metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"type": "service_account",
		}
		err := sm.Init(m)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("missing property `project_id` in metadata"))
	})
}

func TestGetSecret(t *testing.T) {
	sm := NewSecreteManager(logger.NewLogger("test"))

	t.Run("Get Secret - without Init", func(t *testing.T) {
		v, err := sm.GetSecret(context.Background(), secretstores.GetSecretRequest{Name: "test"})
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("client is not initialized"))
		assert.Equal(t, secretstores.GetSecretResponse{Data: nil}, v)
	})

	t.Run("Get Secret - with wrong Init", func(t *testing.T) {
		m := secretstores.Metadata{Base: metadata.Base{
			Properties: map[string]string{
				"type":                        "service_account",
				"project_id":                  "a",
				"private_key_id":              "a",
				"private_key":                 "a",
				"client_email":                "a",
				"client_id":                   "a",
				"auth_uri":                    "a",
				"token_uri":                   "a",
				"auth_provider_x509_cert_url": "a",
				"client_x509_cert_url":        "a",
			},
		}}
		sm.Init(m)
		v, err := sm.GetSecret(context.Background(), secretstores.GetSecretRequest{Name: "test"})
		assert.NotNil(t, err)
		assert.Equal(t, secretstores.GetSecretResponse{Data: nil}, v)
	})
}

func TestBulkGetSecret(t *testing.T) {
	sm := NewSecreteManager(logger.NewLogger("test"))

	t.Run("Bulk Get Secret - without Init", func(t *testing.T) {
		v, err := sm.BulkGetSecret(context.Background(), secretstores.BulkGetSecretRequest{})
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("client is not initialized"))
		assert.Equal(t, secretstores.BulkGetSecretResponse{Data: nil}, v)
	})

	t.Run("Bulk Get Secret - with wrong Init", func(t *testing.T) {
		m := secretstores.Metadata{
			Base: metadata.Base{
				Properties: map[string]string{
					"type":                        "service_account",
					"project_id":                  "a",
					"private_key_id":              "a",
					"private_key":                 "a",
					"client_email":                "a",
					"client_id":                   "a",
					"auth_uri":                    "a",
					"token_uri":                   "a",
					"auth_provider_x509_cert_url": "a",
					"client_x509_cert_url":        "a",
				},
			},
		}
		sm.Init(m)
		v, err := sm.BulkGetSecret(context.Background(), secretstores.BulkGetSecretRequest{})
		assert.NotNil(t, err)
		assert.Equal(t, secretstores.BulkGetSecretResponse{Data: nil}, v)
	})
}
