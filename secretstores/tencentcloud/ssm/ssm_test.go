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

package ssm

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ssm "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/ssm/v20190923"

	"github.com/dapr/components-contrib/secretstores"
)

const (
	secretName  = "secret-name"
	secretValue = "secret-value"
)

type mockedSsmSecretStore struct{}

func (m *mockedSsmSecretStore) ListSecretsWithContext(ctx context.Context, request *ssm.ListSecretsRequest) (*ssm.ListSecretsResponse, error) {
	name := secretName
	requestID := "requestid"
	return &ssm.ListSecretsResponse{
		Response: &ssm.ListSecretsResponseParams{
			SecretMetadatas: []*ssm.SecretMetadata{
				{
					SecretName: &name,
				},
			},
			RequestId: &requestID,
		},
	}, nil
}

func (m *mockedSsmSecretStore) GetSecretValueWithContext(ctx context.Context, request *ssm.GetSecretValueRequest) (*ssm.GetSecretValueResponse, error) {
	secretString := secretValue
	requestID := "requestid"
	return &ssm.GetSecretValueResponse{
		Response: &ssm.GetSecretValueResponseParams{
			SecretName:   request.SecretName,
			SecretString: &secretString,
			RequestId:    &requestID,
		},
	}, nil
}

type mockedSsmSecretStoreReturnError struct{}

func (m *mockedSsmSecretStoreReturnError) ListSecretsWithContext(ctx context.Context, request *ssm.ListSecretsRequest) (*ssm.ListSecretsResponse, error) {
	name := secretName
	requestID := "requestid"
	return &ssm.ListSecretsResponse{
		Response: &ssm.ListSecretsResponseParams{
			SecretMetadatas: []*ssm.SecretMetadata{
				{
					SecretName: &name,
				},
			},
			RequestId: &requestID,
		},
	}, nil
}

func (m *mockedSsmSecretStoreReturnError) GetSecretValueWithContext(ctx context.Context, request *ssm.GetSecretValueRequest) (*ssm.GetSecretValueResponse, error) {
	return nil, errors.New("mocked error")
}

type mockedSsmSecretStoreBothReturnError struct{}

func (m *mockedSsmSecretStoreBothReturnError) ListSecretsWithContext(ctx context.Context, request *ssm.ListSecretsRequest) (*ssm.ListSecretsResponse, error) {
	return nil, errors.New("mocked error")
}

func (m *mockedSsmSecretStoreBothReturnError) GetSecretValueWithContext(ctx context.Context, request *ssm.GetSecretValueRequest) (*ssm.GetSecretValueResponse, error) {
	return nil, errors.New("mocked error")
}

func TestGetSecret(t *testing.T) {
	t.Run("successfully get secret", func(t *testing.T) {
		c := ssmSecretStore{
			client: &mockedSsmSecretStore{},
		}

		req := secretstores.GetSecretRequest{
			Name: secretName,
			Metadata: map[string]string{
				"VersionID": "v1",
			},
		}

		resp, e := c.GetSecret(context.Background(), req)
		require.NoError(t, e)
		assert.Equal(t, secretValue, resp.Data[req.Name])
	})

	t.Run("unsuccessfully get secret", func(t *testing.T) {
		c := ssmSecretStore{
			client: &mockedSsmSecretStoreBothReturnError{},
		}

		req := secretstores.GetSecretRequest{
			Name:     secretName,
			Metadata: map[string]string{},
		}

		_, e := c.GetSecret(context.Background(), req)
		require.Error(t, e)
	})
}

func TestBulkGetSecret(t *testing.T) {
	t.Run("successfully bulk get secret", func(t *testing.T) {
		c := ssmSecretStore{
			client: &mockedSsmSecretStore{},
		}

		req := secretstores.BulkGetSecretRequest{}
		expectedSecrets := map[string]map[string]string{
			secretName: {
				secretName: secretValue,
				RequestID:  "requestid",
				ValueType:  "10",
			},
		}
		resp, e := c.BulkGetSecret(context.Background(), req)
		require.NoError(t, e)
		assert.Equal(t, expectedSecrets, resp.Data)
	})

	t.Run("unsuccessfully bulk get secret", func(t *testing.T) {
		t.Run("with failed to retrieve list of secrets", func(t *testing.T) {
			c := ssmSecretStore{
				client: &mockedSsmSecretStoreBothReturnError{},
			}

			req := secretstores.BulkGetSecretRequest{}
			_, e := c.BulkGetSecret(context.Background(), req)
			require.Error(t, e)
		})

		t.Run("with failed to retrieve the secret", func(t *testing.T) {
			c := ssmSecretStore{
				client: &mockedSsmSecretStoreReturnError{},
			}

			req := secretstores.BulkGetSecretRequest{}
			_, e := c.BulkGetSecret(context.Background(), req)
			require.Error(t, e)
		})
	})
}
