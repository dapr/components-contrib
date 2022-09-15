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

package csms

import (
	"context"
	"fmt"
	"testing"

	"github.com/huaweicloud/huaweicloud-sdk-go-v3/services/csms/v1/model"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/secretstores"
)

const (
	secretName  = "secret-name"
	secretValue = "secret-value"
)

type mockedCsmsSecretStore struct {
	csmsClient
}

func (m *mockedCsmsSecretStore) ListSecrets(request *model.ListSecretsRequest) (*model.ListSecretsResponse, error) {
	name := secretName
	return &model.ListSecretsResponse{
		Secrets: &[]model.Secret{
			{
				Name: &name,
			},
		},
		PageInfo: &model.PageInfo{
			NextMarker: nil,
		},
	}, nil
}

func (m *mockedCsmsSecretStore) ShowSecretVersion(request *model.ShowSecretVersionRequest) (*model.ShowSecretVersionResponse, error) {
	secretString := secretValue
	return &model.ShowSecretVersionResponse{
		Version: &model.Version{
			SecretString: &secretString,
		},
	}, nil
}

type mockedCsmsSecretStoreReturnError struct {
	csmsClient
}

func (m *mockedCsmsSecretStoreReturnError) ListSecrets(request *model.ListSecretsRequest) (*model.ListSecretsResponse, error) {
	name := secretName
	return &model.ListSecretsResponse{
		Secrets: &[]model.Secret{
			{
				Name: &name,
			},
		},
		PageInfo: &model.PageInfo{
			NextMarker: nil,
		},
	}, nil
}

func (m *mockedCsmsSecretStoreReturnError) ShowSecretVersion(request *model.ShowSecretVersionRequest) (*model.ShowSecretVersionResponse, error) {
	return nil, fmt.Errorf("mocked error")
}

type mockedCsmsSecretStoreBothReturnError struct {
	csmsClient
}

func (m *mockedCsmsSecretStoreBothReturnError) ListSecrets(request *model.ListSecretsRequest) (*model.ListSecretsResponse, error) {
	return nil, fmt.Errorf("mocked error")
}

func (m *mockedCsmsSecretStoreBothReturnError) ShowSecretVersion(request *model.ShowSecretVersionRequest) (*model.ShowSecretVersionResponse, error) {
	return nil, fmt.Errorf("mocked error")
}

func TestGetSecret(t *testing.T) {
	t.Run("successfully get secret", func(t *testing.T) {
		c := csmsSecretStore{
			client: &mockedCsmsSecretStore{},
		}

		req := secretstores.GetSecretRequest{
			Name: secretName,
			Metadata: map[string]string{
				"version_id": "v1",
			},
		}

		resp, e := c.GetSecret(context.Background(), req)
		assert.Nil(t, e)
		assert.Equal(t, secretValue, resp.Data[req.Name])
	})

	t.Run("unsuccessfully get secret", func(t *testing.T) {
		c := csmsSecretStore{
			client: &mockedCsmsSecretStoreBothReturnError{},
		}

		req := secretstores.GetSecretRequest{
			Name:     secretName,
			Metadata: map[string]string{},
		}

		_, e := c.GetSecret(context.Background(), req)
		assert.NotNil(t, e)
	})
}

func TestBulkGetSecret(t *testing.T) {
	t.Run("successfully bulk get secret", func(t *testing.T) {
		c := csmsSecretStore{
			client: &mockedCsmsSecretStore{},
		}

		req := secretstores.BulkGetSecretRequest{}
		expectedSecrets := map[string]map[string]string{
			secretName: {
				secretName: secretValue,
			},
		}
		resp, e := c.BulkGetSecret(context.Background(), req)
		assert.Nil(t, e)
		assert.Equal(t, expectedSecrets, resp.Data)
	})

	t.Run("unsuccessfully bulk get secret", func(t *testing.T) {
		t.Run("with failed to retrieve list of secrets", func(t *testing.T) {
			c := csmsSecretStore{
				client: &mockedCsmsSecretStoreBothReturnError{},
			}

			req := secretstores.BulkGetSecretRequest{}
			_, e := c.BulkGetSecret(context.Background(), req)
			assert.NotNil(t, e)
		})

		t.Run("with failed to retrieve the secret", func(t *testing.T) {
			c := csmsSecretStore{
				client: &mockedCsmsSecretStoreReturnError{},
			}

			req := secretstores.BulkGetSecretRequest{}
			_, e := c.BulkGetSecret(context.Background(), req)
			assert.NotNil(t, e)
		})
	})
}

func TestGetFeatures(t *testing.T) {
	s := csmsSecretStore{
		client: &mockedCsmsSecretStore{},
	}
	// Yes, we are skipping initialization as feature retrieval doesn't depend on it.
	t.Run("no features are advertised", func(t *testing.T) {
		f := s.Features()
		assert.Empty(t, f)
	})
}
