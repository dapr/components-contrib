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

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

type MockStore struct {
	gcpSecretemanagerClient
}

func (s *MockStore) ListSecrets(ctx context.Context, req *secretmanagerpb.ListSecretsRequest, opts ...gax.CallOption) *secretmanager.SecretIterator {
	it := &secretmanager.SecretIterator{}
	it.PageInfo().MaxSize = 1
	it.Next()
	return it
}

func (s *MockStore) AccessSecretVersion(ctx context.Context, req *secretmanagerpb.AccessSecretVersionRequest, opts ...gax.CallOption) (*secretmanagerpb.AccessSecretVersionResponse, error) {
	return &secretmanagerpb.AccessSecretVersionResponse{
		Name: "test",
		Payload: &secretmanagerpb.SecretPayload{
			Data: []byte("test"),
		},
	}, nil
}

func (s *MockStore) Close() error {
	return nil
}

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

	t.Run("Get single secret - with no key param", func(t *testing.T) {
		s := sm.(*Store)
		s.client = &MockStore{}
		s.ProjectID = "test_project"

		resp, err := sm.GetSecret(context.Background(), secretstores.GetSecretRequest{})
		assert.NotNil(t, err)
		assert.Nil(t, resp.Data)
	})

	t.Run("Get single secret - success scenario", func(t *testing.T) {
		s := sm.(*Store)
		s.client = &MockStore{}
		s.ProjectID = "test_project"

		resp, err := sm.GetSecret(context.Background(), secretstores.GetSecretRequest{Name: "test"})
		assert.Nil(t, err)
		assert.NotNil(t, resp.Data)
		assert.Equal(t, resp.Data["test"], "test")
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

func TestGetFeatures(t *testing.T) {
	s := NewSecreteManager(logger.NewLogger("test"))
	// Yes, we are skipping initialization as feature retrieval doesn't depend on it.
	t.Run("no features are advertised", func(t *testing.T) {
		f := s.Features()
		assert.Empty(t, f)
	})
}
