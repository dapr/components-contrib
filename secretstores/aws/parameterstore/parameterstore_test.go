/*
Copyright 2025 The Dapr Authors
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
package parameterstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	awsMock "github.com/dapr/components-contrib/common/aws/mock"

	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmTypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

const secretValue = "secret"

func TestInit(t *testing.T) {
	m := secretstores.Metadata{}
	s := NewParameterStore(logger.NewLogger("test"))

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
		t.Run("with valid path", func(t *testing.T) {
			mockSSM := &awsMock.ParameterStoreClient{
				GetParameterFn: func(ctx context.Context, input *ssm.GetParameterInput, optFns ...func(*ssm.Options)) (*ssm.GetParameterOutput, error) {
					secret := secretValue
					return &ssm.GetParameterOutput{
						Parameter: &ssmTypes.Parameter{
							Name:  input.Name,
							Value: &secret,
						},
					}, nil
				},
			}

			s := ssmSecretStore{
				ssmClient: mockSSM,
			}

			req := secretstores.GetSecretRequest{
				Name:     "/aws/dev/secret",
				Metadata: map[string]string{},
			}
			output, e := s.GetSecret(t.Context(), req)
			require.NoError(t, e)
			assert.Equal(t, "secret", output.Data[req.Name])
		})

		t.Run("with version id", func(t *testing.T) {
			mockSSM := &awsMock.ParameterStoreClient{
				GetParameterFn: func(ctx context.Context, input *ssm.GetParameterInput, optFns ...func(*ssm.Options)) (*ssm.GetParameterOutput, error) {
					secret := secretValue
					keys := strings.Split(*input.Name, ":")
					assert.NotNil(t, keys)
					assert.Len(t, keys, 2)
					assert.Equalf(t, "1", keys[1], "Version IDs are same")

					return &ssm.GetParameterOutput{
						Parameter: &ssmTypes.Parameter{
							Name:  &keys[0],
							Value: &secret,
						},
					}, nil
				},
			}

			s := ssmSecretStore{
				ssmClient: mockSSM,
			}

			req := secretstores.GetSecretRequest{
				Name: "/aws/dev/secret",
				Metadata: map[string]string{
					VersionID: "1",
				},
			}
			output, e := s.GetSecret(t.Context(), req)
			require.NoError(t, e)
			assert.Equal(t, secretValue, output.Data[req.Name])
		})

		t.Run("with prefix", func(t *testing.T) {
			mockSSM := &awsMock.ParameterStoreClient{
				GetParameterFn: func(ctx context.Context, input *ssm.GetParameterInput, optFns ...func(*ssm.Options)) (*ssm.GetParameterOutput, error) {
					assert.Equal(t, "/prefix/aws/dev/secret", *input.Name)
					secret := secretValue

					return &ssm.GetParameterOutput{
						Parameter: &ssmTypes.Parameter{
							Name:  input.Name,
							Value: &secret,
						},
					}, nil
				},
			}

			s := ssmSecretStore{
				ssmClient: mockSSM,
				prefix:    "/prefix",
			}

			req := secretstores.GetSecretRequest{
				Name:     "/aws/dev/secret",
				Metadata: map[string]string{},
			}
			output, e := s.GetSecret(t.Context(), req)
			require.NoError(t, e)
			assert.Equal(t, "secret", output.Data[req.Name])
		})
	})

	t.Run("unsuccessfully retrieve secret", func(t *testing.T) {
		mockSSM := &awsMock.ParameterStoreClient{
			GetParameterFn: func(ctx context.Context, input *ssm.GetParameterInput, optFns ...func(*ssm.Options)) (*ssm.GetParameterOutput, error) {
				return nil, errors.New("failed due to any reason")
			},
		}

		s := ssmSecretStore{
			ssmClient: mockSSM,
			prefix:    "/prefix",
		}

		req := secretstores.GetSecretRequest{
			Name:     "/aws/dev/secret",
			Metadata: map[string]string{},
		}
		_, err := s.GetSecret(t.Context(), req)
		require.Error(t, err)
	})
}

func TestGetBulkSecrets(t *testing.T) {
	t.Run("successfully retrieve bulk secrets", func(t *testing.T) {
		mockSSM := &awsMock.ParameterStoreClient{
			DescribeParametersFn: func(ctx context.Context, input *ssm.DescribeParametersInput, optFns ...func(*ssm.Options)) (*ssm.DescribeParametersOutput, error) {
				secret1 := "/aws/dev/secret1" //nolint:gosec
				secret2 := "/aws/dev/secret2" //nolint:gosec
				return &ssm.DescribeParametersOutput{NextToken: nil, Parameters: []ssmTypes.ParameterMetadata{
					{
						Name: &secret1,
					},
					{
						Name: &secret2,
					},
				}}, nil
			},
			GetParameterFn: func(ctx context.Context, input *ssm.GetParameterInput, optFns ...func(*ssm.Options)) (*ssm.GetParameterOutput, error) {
				secret := fmt.Sprintf("%s-%s", *input.Name, secretValue)

				return &ssm.GetParameterOutput{
					Parameter: &ssmTypes.Parameter{
						Name:  input.Name,
						Value: &secret,
					},
				}, nil
			},
		}

		s := ssmSecretStore{
			ssmClient: mockSSM,
		}

		req := secretstores.BulkGetSecretRequest{
			Metadata: map[string]string{},
		}
		output, e := s.BulkGetSecret(t.Context(), req)
		require.NoError(t, e)
		assert.Contains(t, output.Data, "/aws/dev/secret1")
		assert.Contains(t, output.Data, "/aws/dev/secret2")
	})

	t.Run("successfully retrieve bulk secrets with prefix", func(t *testing.T) {
		mockSSM := &awsMock.ParameterStoreClient{
			DescribeParametersFn: func(ctx context.Context, input *ssm.DescribeParametersInput, optFns ...func(*ssm.Options)) (*ssm.DescribeParametersOutput, error) {
				secret1 := "/prefix/aws/dev/secret1" //nolint:gosec
				secret2 := "/prefix/aws/dev/secret2" //nolint:gosec
				return &ssm.DescribeParametersOutput{NextToken: nil, Parameters: []ssmTypes.ParameterMetadata{
					{
						Name: &secret1,
					},
					{
						Name: &secret2,
					},
				}}, nil
			},
			GetParameterFn: func(ctx context.Context, input *ssm.GetParameterInput, optFns ...func(*ssm.Options)) (*ssm.GetParameterOutput, error) {
				secret := fmt.Sprintf("%s-%s", *input.Name, secretValue)

				return &ssm.GetParameterOutput{
					Parameter: &ssmTypes.Parameter{
						Name:  input.Name,
						Value: &secret,
					},
				}, nil
			},
		}

		s := ssmSecretStore{
			ssmClient: mockSSM,
			prefix:    "/prefix",
		}

		req := secretstores.BulkGetSecretRequest{
			Metadata: map[string]string{},
		}
		output, e := s.BulkGetSecret(t.Context(), req)
		require.NoError(t, e)
		assert.Equal(t, "map[/aws/dev/secret1:/prefix/aws/dev/secret1-secret]", fmt.Sprint(output.Data["/aws/dev/secret1"]))
		assert.Equal(t, "map[/aws/dev/secret2:/prefix/aws/dev/secret2-secret]", fmt.Sprint(output.Data["/aws/dev/secret2"]))
	})

	t.Run("unsuccessfully retrieve bulk secrets on get parameter", func(t *testing.T) {
		mockSSM := &awsMock.ParameterStoreClient{
			DescribeParametersFn: func(ctx context.Context, input *ssm.DescribeParametersInput, optFns ...func(*ssm.Options)) (*ssm.DescribeParametersOutput, error) {
				secret1 := "/aws/dev/secret1" //nolint:gosec
				secret2 := "/aws/dev/secret2" //nolint:gosec
				return &ssm.DescribeParametersOutput{NextToken: nil, Parameters: []ssmTypes.ParameterMetadata{
					{
						Name: &secret1,
					},
					{
						Name: &secret2,
					},
				}}, nil
			},
			GetParameterFn: func(ctx context.Context, input *ssm.GetParameterInput, optFns ...func(*ssm.Options)) (*ssm.GetParameterOutput, error) {
				return nil, errors.New("failed due to any reason")
			},
		}

		s := ssmSecretStore{
			ssmClient: mockSSM,
		}

		req := secretstores.BulkGetSecretRequest{
			Metadata: map[string]string{},
		}
		_, err := s.BulkGetSecret(t.Context(), req)
		require.Error(t, err)
	})

	t.Run("unsuccessfully retrieve bulk secrets on describe parameter", func(t *testing.T) {
		mockSSM := &awsMock.ParameterStoreClient{
			DescribeParametersFn: func(ctx context.Context, input *ssm.DescribeParametersInput, optFns ...func(*ssm.Options)) (*ssm.DescribeParametersOutput, error) {
				return nil, errors.New("failed due to any reason")
			},
		}

		s := ssmSecretStore{
			ssmClient: mockSSM,
		}

		req := secretstores.BulkGetSecretRequest{
			Metadata: map[string]string{},
		}
		_, err := s.BulkGetSecret(t.Context(), req)
		require.Error(t, err)
	})
}

func TestGetFeatures(t *testing.T) {
	s := ssmSecretStore{}
	// Yes, we are skipping initialization as feature retrieval doesn't depend on it.
	t.Run("no features are advertised", func(t *testing.T) {
		f := s.Features()
		assert.Empty(t, f)
	})
}
