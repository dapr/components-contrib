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
package parameterstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/aws/aws-sdk-go/service/ssm/ssmiface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

const secretValue = "secret"

type mockedSSM struct {
	GetParameterFn       func(context.Context, *ssm.GetParameterInput, ...request.Option) (*ssm.GetParameterOutput, error)
	DescribeParametersFn func(context.Context, *ssm.DescribeParametersInput, ...request.Option) (*ssm.DescribeParametersOutput, error)
	ssmiface.SSMAPI
}

func (m *mockedSSM) GetParameterWithContext(ctx context.Context, input *ssm.GetParameterInput, option ...request.Option) (*ssm.GetParameterOutput, error) {
	return m.GetParameterFn(ctx, input, option...)
}

func (m *mockedSSM) DescribeParametersWithContext(ctx context.Context, input *ssm.DescribeParametersInput, option ...request.Option) (*ssm.DescribeParametersOutput, error) {
	return m.DescribeParametersFn(ctx, input, option...)
}

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
		err := s.Init(context.Background(), m)
		require.NoError(t, err)
	})
}

func TestGetSecret(t *testing.T) {
	t.Run("successfully retrieve secret", func(t *testing.T) {
		t.Run("with valid path", func(t *testing.T) {
			s := ssmSecretStore{
				client: &mockedSSM{
					GetParameterFn: func(ctx context.Context, input *ssm.GetParameterInput, option ...request.Option) (*ssm.GetParameterOutput, error) {
						secret := secretValue

						return &ssm.GetParameterOutput{
							Parameter: &ssm.Parameter{
								Name:  input.Name,
								Value: &secret,
							},
						}, nil
					},
				},
			}

			req := secretstores.GetSecretRequest{
				Name:     "/aws/dev/secret",
				Metadata: map[string]string{},
			}
			output, e := s.GetSecret(context.Background(), req)
			require.NoError(t, e)
			assert.Equal(t, "secret", output.Data[req.Name])
		})

		t.Run("with version id", func(t *testing.T) {
			s := ssmSecretStore{
				client: &mockedSSM{
					GetParameterFn: func(ctx context.Context, input *ssm.GetParameterInput, option ...request.Option) (*ssm.GetParameterOutput, error) {
						secret := secretValue
						keys := strings.Split(*input.Name, ":")
						assert.NotNil(t, keys)
						assert.Len(t, keys, 2)
						assert.Equalf(t, "1", keys[1], "Version IDs are same")

						return &ssm.GetParameterOutput{
							Parameter: &ssm.Parameter{
								Name:  &keys[0],
								Value: &secret,
							},
						}, nil
					},
				},
			}

			req := secretstores.GetSecretRequest{
				Name: "/aws/dev/secret",
				Metadata: map[string]string{
					VersionID: "1",
				},
			}
			output, e := s.GetSecret(context.Background(), req)
			require.NoError(t, e)
			assert.Equal(t, secretValue, output.Data[req.Name])
		})

		t.Run("with prefix", func(t *testing.T) {
			s := ssmSecretStore{
				client: &mockedSSM{
					GetParameterFn: func(ctx context.Context, input *ssm.GetParameterInput, option ...request.Option) (*ssm.GetParameterOutput, error) {
						assert.Equal(t, "/prefix/aws/dev/secret", *input.Name)
						secret := secretValue

						return &ssm.GetParameterOutput{
							Parameter: &ssm.Parameter{
								Name:  input.Name,
								Value: &secret,
							},
						}, nil
					},
				},
				prefix: "/prefix",
			}

			req := secretstores.GetSecretRequest{
				Name:     "/aws/dev/secret",
				Metadata: map[string]string{},
			}
			output, e := s.GetSecret(context.Background(), req)
			require.NoError(t, e)
			assert.Equal(t, "secret", output.Data[req.Name])
		})
	})

	t.Run("unsuccessfully retrieve secret", func(t *testing.T) {
		s := ssmSecretStore{
			client: &mockedSSM{
				GetParameterFn: func(ctx context.Context, input *ssm.GetParameterInput, option ...request.Option) (*ssm.GetParameterOutput, error) {
					return nil, errors.New("failed due to any reason")
				},
			},
		}
		req := secretstores.GetSecretRequest{
			Name:     "/aws/dev/secret",
			Metadata: map[string]string{},
		}
		_, err := s.GetSecret(context.Background(), req)
		require.Error(t, err)
	})
}

func TestGetBulkSecrets(t *testing.T) {
	t.Run("successfully retrieve bulk secrets", func(t *testing.T) {
		s := ssmSecretStore{
			client: &mockedSSM{
				DescribeParametersFn: func(context.Context, *ssm.DescribeParametersInput, ...request.Option) (*ssm.DescribeParametersOutput, error) {
					return &ssm.DescribeParametersOutput{NextToken: nil, Parameters: []*ssm.ParameterMetadata{
						{
							Name: aws.String("/aws/dev/secret1"),
						},
						{
							Name: aws.String("/aws/dev/secret2"),
						},
					}}, nil
				},
				GetParameterFn: func(ctx context.Context, input *ssm.GetParameterInput, option ...request.Option) (*ssm.GetParameterOutput, error) {
					secret := fmt.Sprintf("%s-%s", *input.Name, secretValue)

					return &ssm.GetParameterOutput{
						Parameter: &ssm.Parameter{
							Name:  input.Name,
							Value: &secret,
						},
					}, nil
				},
			},
		}

		req := secretstores.BulkGetSecretRequest{
			Metadata: map[string]string{},
		}
		output, e := s.BulkGetSecret(context.Background(), req)
		require.NoError(t, e)
		assert.Contains(t, output.Data, "/aws/dev/secret1")
		assert.Contains(t, output.Data, "/aws/dev/secret2")
	})

	t.Run("successfully retrieve bulk secrets with prefix", func(t *testing.T) {
		s := ssmSecretStore{
			client: &mockedSSM{
				DescribeParametersFn: func(context.Context, *ssm.DescribeParametersInput, ...request.Option) (*ssm.DescribeParametersOutput, error) {
					return &ssm.DescribeParametersOutput{NextToken: nil, Parameters: []*ssm.ParameterMetadata{
						{
							Name: aws.String("/prefix/aws/dev/secret1"),
						},
						{
							Name: aws.String("/prefix/aws/dev/secret2"),
						},
					}}, nil
				},
				GetParameterFn: func(ctx context.Context, input *ssm.GetParameterInput, option ...request.Option) (*ssm.GetParameterOutput, error) {
					secret := fmt.Sprintf("%s-%s", *input.Name, secretValue)

					return &ssm.GetParameterOutput{
						Parameter: &ssm.Parameter{
							Name:  input.Name,
							Value: &secret,
						},
					}, nil
				},
			},
			prefix: "/prefix",
		}

		req := secretstores.BulkGetSecretRequest{
			Metadata: map[string]string{},
		}
		output, e := s.BulkGetSecret(context.Background(), req)
		require.NoError(t, e)
		assert.Equal(t, "map[/aws/dev/secret1:/prefix/aws/dev/secret1-secret]", fmt.Sprint(output.Data["/aws/dev/secret1"]))
		assert.Equal(t, "map[/aws/dev/secret2:/prefix/aws/dev/secret2-secret]", fmt.Sprint(output.Data["/aws/dev/secret2"]))
	})

	t.Run("unsuccessfully retrieve bulk secrets on get parameter", func(t *testing.T) {
		s := ssmSecretStore{
			client: &mockedSSM{
				DescribeParametersFn: func(context.Context, *ssm.DescribeParametersInput, ...request.Option) (*ssm.DescribeParametersOutput, error) {
					return &ssm.DescribeParametersOutput{NextToken: nil, Parameters: []*ssm.ParameterMetadata{
						{
							Name: aws.String("/aws/dev/secret1"),
						},
						{
							Name: aws.String("/aws/dev/secret2"),
						},
					}}, nil
				},
				GetParameterFn: func(ctx context.Context, input *ssm.GetParameterInput, option ...request.Option) (*ssm.GetParameterOutput, error) {
					return nil, errors.New("failed due to any reason")
				},
			},
		}
		req := secretstores.BulkGetSecretRequest{
			Metadata: map[string]string{},
		}
		_, err := s.BulkGetSecret(context.Background(), req)
		require.Error(t, err)
	})

	t.Run("unsuccessfully retrieve bulk secrets on describe parameter", func(t *testing.T) {
		s := ssmSecretStore{
			client: &mockedSSM{
				DescribeParametersFn: func(context.Context, *ssm.DescribeParametersInput, ...request.Option) (*ssm.DescribeParametersOutput, error) {
					return nil, errors.New("failed due to any reason")
				},
			},
		}
		req := secretstores.BulkGetSecretRequest{
			Metadata: map[string]string{},
		}
		_, err := s.BulkGetSecret(context.Background(), req)
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
