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

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"github.com/aws/aws-sdk-go/service/secretsmanager/secretsmanageriface"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

const secretValue = "secret"

type mockedSM struct {
	GetSecretValueFn func(context.Context, *secretsmanager.GetSecretValueInput, ...request.Option) (*secretsmanager.GetSecretValueOutput, error)
	secretsmanageriface.SecretsManagerAPI
}

func (m *mockedSM) GetSecretValueWithContext(ctx context.Context, input *secretsmanager.GetSecretValueInput, option ...request.Option) (*secretsmanager.GetSecretValueOutput, error) {
	return m.GetSecretValueFn(ctx, input, option...)
}

func TestInit(t *testing.T) {
	m := secretstores.Metadata{}
	s := NewSecretManager(logger.NewLogger("test"))
	t.Run("Init with valid metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"AccessKey":    "a",
			"Region":       "a",
			"Endpoint":     "a",
			"SecretKey":    "a",
			"SessionToken": "a",
		}
		err := s.Init(m)
		assert.Nil(t, err)
	})
}

func TestGetSecret(t *testing.T) {
	t.Run("successfully retrieve secret", func(t *testing.T) {
		t.Run("without version id and version stage", func(t *testing.T) {
			s := smSecretStore{
				client: &mockedSM{
					GetSecretValueFn: func(ctx context.Context, input *secretsmanager.GetSecretValueInput, option ...request.Option) (*secretsmanager.GetSecretValueOutput, error) {
						assert.Nil(t, input.VersionId)
						assert.Nil(t, input.VersionStage)
						secret := secretValue

						return &secretsmanager.GetSecretValueOutput{
							Name:         input.SecretId,
							SecretString: &secret,
						}, nil
					},
				},
			}

			req := secretstores.GetSecretRequest{
				Name:     "/aws/secret/testing",
				Metadata: map[string]string{},
			}
			output, e := s.GetSecret(context.Background(), req)
			assert.Nil(t, e)
			assert.Equal(t, "secret", output.Data[req.Name])
		})

		t.Run("with version id", func(t *testing.T) {
			s := smSecretStore{
				client: &mockedSM{
					GetSecretValueFn: func(ctx context.Context, input *secretsmanager.GetSecretValueInput, option ...request.Option) (*secretsmanager.GetSecretValueOutput, error) {
						assert.NotNil(t, input.VersionId)
						secret := secretValue

						return &secretsmanager.GetSecretValueOutput{
							Name:         input.SecretId,
							SecretString: &secret,
						}, nil
					},
				},
			}

			req := secretstores.GetSecretRequest{
				Name: "/aws/secret/testing",
				Metadata: map[string]string{
					VersionID: "1",
				},
			}
			output, e := s.GetSecret(context.Background(), req)
			assert.Nil(t, e)
			assert.Equal(t, secretValue, output.Data[req.Name])
		})

		t.Run("with version stage", func(t *testing.T) {
			s := smSecretStore{
				client: &mockedSM{
					GetSecretValueFn: func(ctx context.Context, input *secretsmanager.GetSecretValueInput, option ...request.Option) (*secretsmanager.GetSecretValueOutput, error) {
						assert.NotNil(t, input.VersionStage)
						secret := secretValue

						return &secretsmanager.GetSecretValueOutput{
							Name:         input.SecretId,
							SecretString: &secret,
						}, nil
					},
				},
			}

			req := secretstores.GetSecretRequest{
				Name: "/aws/secret/testing",
				Metadata: map[string]string{
					VersionStage: "dev",
				},
			}
			output, e := s.GetSecret(context.Background(), req)
			assert.Nil(t, e)
			assert.Equal(t, secretValue, output.Data[req.Name])
		})
	})

	t.Run("unsuccessfully retrieve secret", func(t *testing.T) {
		s := smSecretStore{
			client: &mockedSM{
				GetSecretValueFn: func(ctx context.Context, input *secretsmanager.GetSecretValueInput, option ...request.Option) (*secretsmanager.GetSecretValueOutput, error) {
					return nil, fmt.Errorf("failed due to any reason")
				},
			},
		}
		req := secretstores.GetSecretRequest{
			Name:     "/aws/secret/testing",
			Metadata: map[string]string{},
		}
		_, err := s.GetSecret(context.Background(), req)
		assert.NotNil(t, err)
	})
}

func TestGetFeatures(t *testing.T) {
	s := smSecretStore{}
	t.Run("no features are advertised", func(t *testing.T) {
		f := s.Features()
		assert.Empty(t, f)
	})
}
