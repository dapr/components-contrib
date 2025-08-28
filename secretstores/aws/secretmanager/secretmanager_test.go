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
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	awsAuth "github.com/dapr/components-contrib/common/authentication/aws"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

const secretValue = "secret"

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
		err := s.Init(t.Context(), m)
		require.NoError(t, err)
	})
}

func TestGetSecret(t *testing.T) {
	t.Run("successfully retrieve secret", func(t *testing.T) {
		t.Run("without version id and version stage", func(t *testing.T) {
			mockSSM := &awsAuth.MockSecretManager{
				GetSecretValueFn: func(ctx context.Context, input *secretsmanager.GetSecretValueInput, option ...request.Option) (*secretsmanager.GetSecretValueOutput, error) {
					assert.Nil(t, input.VersionId)
					assert.Nil(t, input.VersionStage)
					secret := secretValue

					return &secretsmanager.GetSecretValueOutput{
						Name:         input.SecretId,
						SecretString: &secret,
					}, nil
				},
			}

			secret := awsAuth.SecretManagerClients{
				Manager: mockSSM,
			}

			mockedClients := awsAuth.Clients{
				Secret: &secret,
			}
			mockAuthProvider := &awsAuth.StaticAuth{}
			mockAuthProvider.WithMockClients(&mockedClients)
			s := smSecretStore{
				authProvider: mockAuthProvider,
			}

			req := secretstores.GetSecretRequest{
				Name:     "/aws/secret/testing",
				Metadata: map[string]string{},
			}
			output, e := s.GetSecret(t.Context(), req)
			require.NoError(t, e)
			assert.Equal(t, "secret", output.Data[req.Name])
		})

		t.Run("with version id", func(t *testing.T) {
			mockSSM := &awsAuth.MockSecretManager{
				GetSecretValueFn: func(ctx context.Context, input *secretsmanager.GetSecretValueInput, option ...request.Option) (*secretsmanager.GetSecretValueOutput, error) {
					assert.NotNil(t, input.VersionId)
					secret := secretValue

					return &secretsmanager.GetSecretValueOutput{
						Name:         input.SecretId,
						SecretString: &secret,
					}, nil
				},
			}

			secret := awsAuth.SecretManagerClients{
				Manager: mockSSM,
			}

			mockedClients := awsAuth.Clients{
				Secret: &secret,
			}

			mockAuthProvider := &awsAuth.StaticAuth{}
			mockAuthProvider.WithMockClients(&mockedClients)
			s := smSecretStore{
				authProvider: mockAuthProvider,
			}

			req := secretstores.GetSecretRequest{
				Name: "/aws/secret/testing",
				Metadata: map[string]string{
					VersionID: "1",
				},
			}
			output, e := s.GetSecret(t.Context(), req)
			require.NoError(t, e)
			assert.Equal(t, secretValue, output.Data[req.Name])
		})

		t.Run("with version stage", func(t *testing.T) {
			mockSSM := &awsAuth.MockSecretManager{
				GetSecretValueFn: func(ctx context.Context, input *secretsmanager.GetSecretValueInput, option ...request.Option) (*secretsmanager.GetSecretValueOutput, error) {
					assert.NotNil(t, input.VersionStage)
					secret := secretValue

					return &secretsmanager.GetSecretValueOutput{
						Name:         input.SecretId,
						SecretString: &secret,
					}, nil
				},
			}

			secret := awsAuth.SecretManagerClients{
				Manager: mockSSM,
			}

			mockedClients := awsAuth.Clients{
				Secret: &secret,
			}

			mockAuthProvider := &awsAuth.StaticAuth{}
			mockAuthProvider.WithMockClients(&mockedClients)
			s := smSecretStore{
				authProvider: mockAuthProvider,
			}

			req := secretstores.GetSecretRequest{
				Name: "/aws/secret/testing",
				Metadata: map[string]string{
					VersionStage: "dev",
				},
			}
			output, e := s.GetSecret(t.Context(), req)
			require.NoError(t, e)
			assert.Equal(t, secretValue, output.Data[req.Name])
		})

		t.Run("with multiple keys per secret", func(t *testing.T) {
			mockSSM := &awsAuth.MockSecretManager{
				GetSecretValueFn: func(ctx context.Context, input *secretsmanager.GetSecretValueInput, option ...request.Option) (*secretsmanager.GetSecretValueOutput, error) {
					assert.Nil(t, input.VersionId)
					assert.Nil(t, input.VersionStage)
					// #nosec G101: This is a mock secret used for testing purposes.
					secret := `{"key1":"value1","key2":"value2","key3":{"nested":"value3"}}`

					return &secretsmanager.GetSecretValueOutput{
						Name:         input.SecretId,
						SecretString: &secret,
					}, nil
				},
			}

			secret := awsAuth.SecretManagerClients{
				Manager: mockSSM,
			}

			mockedClients := awsAuth.Clients{
				Secret: &secret,
			}
			mockAuthProvider := &awsAuth.StaticAuth{}
			mockAuthProvider.WithMockClients(&mockedClients)
			s := smSecretStore{
				authProvider:               mockAuthProvider,
				multipleKeyValuesPerSecret: true,
			}

			req := secretstores.GetSecretRequest{
				Name:     "/aws/secret/testing",
				Metadata: map[string]string{},
			}
			output, e := s.GetSecret(t.Context(), req)
			require.NoError(t, e)
			assert.Len(t, output.Data, 3)
			assert.Equal(t, "value1", output.Data["key1"])
			assert.Equal(t, "value2", output.Data["key2"])
			assert.JSONEq(t, `{"nested":"value3"}`, output.Data["key3"])
		})

		t.Run("with multiple keys per secret and option disabled", func(t *testing.T) {
			mockSSM := &awsAuth.MockSecretManager{
				GetSecretValueFn: func(ctx context.Context, input *secretsmanager.GetSecretValueInput, option ...request.Option) (*secretsmanager.GetSecretValueOutput, error) {
					assert.Nil(t, input.VersionId)
					assert.Nil(t, input.VersionStage)
					// #nosec G101: This is a mock secret used for testing purposes.
					secret := `{"key1":"value1","key2":"value2"}`

					return &secretsmanager.GetSecretValueOutput{
						Name:         input.SecretId,
						SecretString: &secret,
					}, nil
				},
			}

			secret := awsAuth.SecretManagerClients{
				Manager: mockSSM,
			}

			mockedClients := awsAuth.Clients{
				Secret: &secret,
			}
			mockAuthProvider := &awsAuth.StaticAuth{}
			mockAuthProvider.WithMockClients(&mockedClients)
			s := smSecretStore{
				authProvider: mockAuthProvider,
			}

			req := secretstores.GetSecretRequest{
				Name:     "/aws/secret/testing",
				Metadata: map[string]string{},
			}
			output, e := s.GetSecret(t.Context(), req)
			require.NoError(t, e)
			assert.Len(t, output.Data, 1)
			assert.JSONEq(t, `{"key1":"value1","key2":"value2"}`, output.Data["/aws/secret/testing"])
		})

		t.Run("with multiple keys per secret and secret is NOT json", func(t *testing.T) {
			mockSSM := &awsAuth.MockSecretManager{
				GetSecretValueFn: func(ctx context.Context, input *secretsmanager.GetSecretValueInput, option ...request.Option) (*secretsmanager.GetSecretValueOutput, error) {
					assert.Nil(t, input.VersionId)
					assert.Nil(t, input.VersionStage)
					secret := "not json"

					return &secretsmanager.GetSecretValueOutput{
						Name:         input.SecretId,
						SecretString: &secret,
					}, nil
				},
			}

			secret := awsAuth.SecretManagerClients{
				Manager: mockSSM,
			}

			mockedClients := awsAuth.Clients{
				Secret: &secret,
			}
			mockAuthProvider := &awsAuth.StaticAuth{}
			mockAuthProvider.WithMockClients(&mockedClients)
			s := smSecretStore{
				authProvider:               mockAuthProvider,
				multipleKeyValuesPerSecret: true,
			}

			req := secretstores.GetSecretRequest{
				Name:     "/aws/secret/testing",
				Metadata: map[string]string{},
			}
			output, e := s.GetSecret(t.Context(), req)
			require.NoError(t, e)
			assert.Len(t, output.Data, 1)
			assert.Equal(t, "not json", output.Data["/aws/secret/testing"])
		})

		t.Run("with multiple keys per secret and secret is json collection", func(t *testing.T) {
			mockSSM := &awsAuth.MockSecretManager{
				GetSecretValueFn: func(ctx context.Context, input *secretsmanager.GetSecretValueInput, option ...request.Option) (*secretsmanager.GetSecretValueOutput, error) {
					assert.Nil(t, input.VersionId)
					assert.Nil(t, input.VersionStage)
					secret := `[{"key1":"value1"},{"key2":"value2"}]` // #nosec G101: This is a mock secret used for testing purposes.

					return &secretsmanager.GetSecretValueOutput{
						Name:         input.SecretId,
						SecretString: &secret,
					}, nil
				},
			}

			secret := awsAuth.SecretManagerClients{
				Manager: mockSSM,
			}

			mockedClients := awsAuth.Clients{
				Secret: &secret,
			}
			mockAuthProvider := &awsAuth.StaticAuth{}
			mockAuthProvider.WithMockClients(&mockedClients)
			s := smSecretStore{
				authProvider:               mockAuthProvider,
				multipleKeyValuesPerSecret: true,
			}

			req := secretstores.GetSecretRequest{
				Name:     "/aws/secret/testing",
				Metadata: map[string]string{},
			}
			output, e := s.GetSecret(t.Context(), req)
			require.NoError(t, e)
			assert.Len(t, output.Data, 1)
			assert.JSONEq(t, `[{"key1":"value1"},{"key2":"value2"}]`, output.Data["/aws/secret/testing"])
		})
	})

	t.Run("unsuccessfully retrieve secret", func(t *testing.T) {
		mockSSM := &awsAuth.MockSecretManager{
			GetSecretValueFn: func(ctx context.Context, input *secretsmanager.GetSecretValueInput, option ...request.Option) (*secretsmanager.GetSecretValueOutput, error) {
				return nil, errors.New("failed due to any reason")
			},
		}

		secret := awsAuth.SecretManagerClients{
			Manager: mockSSM,
		}

		mockedClients := awsAuth.Clients{
			Secret: &secret,
		}

		mockAuthProvider := &awsAuth.StaticAuth{}
		mockAuthProvider.WithMockClients(&mockedClients)
		s := smSecretStore{
			authProvider: mockAuthProvider,
		}

		req := secretstores.GetSecretRequest{
			Name:     "/aws/secret/testing",
			Metadata: map[string]string{},
		}
		_, err := s.GetSecret(t.Context(), req)
		require.Error(t, err)
	})
}

func TestBulkGetSecret(t *testing.T) {
	t.Run("returns all secrets in store", func(t *testing.T) {
		secret1 := "/aws/secret/testing1"
		secretValue1 := "secret1"
		secret2 := "/aws/secret/testing2"
		secretValue2 := "secret2"

		mockSSM := &awsAuth.MockSecretManager{
			GetSecretValueFn: func(ctx context.Context, input *secretsmanager.GetSecretValueInput, option ...request.Option) (*secretsmanager.GetSecretValueOutput, error) {
				assert.Nil(t, input.VersionId)
				assert.Nil(t, input.VersionStage)

				if input.SecretId == &secret1 {
					return &secretsmanager.GetSecretValueOutput{
						Name:         input.SecretId,
						SecretString: &secretValue1,
					}, nil
				} else {
					return &secretsmanager.GetSecretValueOutput{
						Name:         input.SecretId,
						SecretString: &secretValue2,
					}, nil
				}
			},

			ListSecretsFn: func(ctx context.Context, input *secretsmanager.ListSecretsInput, option ...request.Option) (*secretsmanager.ListSecretsOutput, error) {
				return &secretsmanager.ListSecretsOutput{
					SecretList: []*secretsmanager.SecretListEntry{
						{Name: &secret1},
						{Name: &secret2},
					},
				}, nil
			},
		}

		secret := awsAuth.SecretManagerClients{
			Manager: mockSSM,
		}

		mockedClients := awsAuth.Clients{
			Secret: &secret,
		}
		mockAuthProvider := &awsAuth.StaticAuth{}
		mockAuthProvider.WithMockClients(&mockedClients)
		s := smSecretStore{
			authProvider: mockAuthProvider,
		}

		req := secretstores.BulkGetSecretRequest{
			Metadata: map[string]string{},
		}
		output, e := s.BulkGetSecret(t.Context(), req)
		require.NoError(t, e)
		assert.Equal(t, map[string]map[string]string{
			secret1: {
				secret1: secretValue1,
			},
			secret2: {
				secret2: secretValue2,
			},
		}, output.Data)
	})

	t.Run("when multipleKeyValuesPerSecret = true, returns all secrets in store broken out by key", func(t *testing.T) {
		secret1 := "/aws/secret/testing1"
		// #nosec G101: This is a mock secret used for testing purposes.
		secretValue1 := `{"key1":"value1","key2":"value2"}`
		secret2 := "/aws/secret/testing2"
		// #nosec G101: This is a mock secret used for testing purposes.
		secretValue2 := `{"key3":"value3","key4":{"nested":"value4"}}`

		mockSSM := &awsAuth.MockSecretManager{
			GetSecretValueFn: func(ctx context.Context, input *secretsmanager.GetSecretValueInput, option ...request.Option) (*secretsmanager.GetSecretValueOutput, error) {
				assert.Nil(t, input.VersionId)
				assert.Nil(t, input.VersionStage)

				if input.SecretId == &secret1 {
					return &secretsmanager.GetSecretValueOutput{
						Name:         input.SecretId,
						SecretString: &secretValue1,
					}, nil
				} else {
					return &secretsmanager.GetSecretValueOutput{
						Name:         input.SecretId,
						SecretString: &secretValue2,
					}, nil
				}
			},

			ListSecretsFn: func(ctx context.Context, input *secretsmanager.ListSecretsInput, option ...request.Option) (*secretsmanager.ListSecretsOutput, error) {
				return &secretsmanager.ListSecretsOutput{
					SecretList: []*secretsmanager.SecretListEntry{
						{Name: &secret1},
						{Name: &secret2},
					},
				}, nil
			},
		}

		secret := awsAuth.SecretManagerClients{
			Manager: mockSSM,
		}

		mockedClients := awsAuth.Clients{
			Secret: &secret,
		}
		mockAuthProvider := &awsAuth.StaticAuth{}
		mockAuthProvider.WithMockClients(&mockedClients)
		s := smSecretStore{
			authProvider:               mockAuthProvider,
			multipleKeyValuesPerSecret: true,
		}

		req := secretstores.BulkGetSecretRequest{
			Metadata: map[string]string{},
		}
		output, e := s.BulkGetSecret(t.Context(), req)
		require.NoError(t, e)
		assert.Equal(t, map[string]map[string]string{
			secret1: {
				"key1": "value1",
				"key2": "value2",
			},
			secret2: {
				"key3": "value3",
				"key4": `{"nested":"value4"}`,
			},
		}, output.Data)
	})
}

func TestGetFeatures(t *testing.T) {
	s := smSecretStore{}
	t.Run("when multipleKeyValuesPerSecret = true, return feature", func(t *testing.T) {
		s.multipleKeyValuesPerSecret = true
		f := s.Features()
		assert.True(t, secretstores.FeatureMultipleKeyValuesPerSecret.IsPresent(f))
	})

	t.Run("when multipleKeyValuesPerSecret = false, no feature advertised", func(t *testing.T) {
		s.multipleKeyValuesPerSecret = false
		f := s.Features()
		assert.Empty(t, f)
	})

	t.Run("by default, no feature advertised", func(t *testing.T) {
		f := s.Features()
		assert.Empty(t, f)
	})
}
