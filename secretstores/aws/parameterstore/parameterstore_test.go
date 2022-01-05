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
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/aws/aws-sdk-go/service/ssm/ssmiface"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

const secretValue = "secret"

type mockedSSM struct {
	GetParameterFn       func(*ssm.GetParameterInput) (*ssm.GetParameterOutput, error)
	DescribeParametersFn func(*ssm.DescribeParametersInput) (*ssm.DescribeParametersOutput, error)
	ssmiface.SSMAPI
}

func (m *mockedSSM) GetParameter(input *ssm.GetParameterInput) (*ssm.GetParameterOutput, error) {
	return m.GetParameterFn(input)
}

func (m *mockedSSM) DescribeParameters(input *ssm.DescribeParametersInput) (*ssm.DescribeParametersOutput, error) {
	return m.DescribeParametersFn(input)
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
		err := s.Init(m)
		assert.Nil(t, err)
	})
}

func TestGetSecret(t *testing.T) {
	t.Run("successfully retrieve secret", func(t *testing.T) {
		t.Run("with valid path", func(t *testing.T) {
			s := ssmSecretStore{
				client: &mockedSSM{
					GetParameterFn: func(input *ssm.GetParameterInput) (*ssm.GetParameterOutput, error) {
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
			output, e := s.GetSecret(req)
			assert.Nil(t, e)
			assert.Equal(t, "secret", output.Data[req.Name])
		})

		t.Run("with version id", func(t *testing.T) {
			s := ssmSecretStore{
				client: &mockedSSM{
					GetParameterFn: func(input *ssm.GetParameterInput) (*ssm.GetParameterOutput, error) {
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
			output, e := s.GetSecret(req)
			assert.Nil(t, e)
			assert.Equal(t, secretValue, output.Data[req.Name])
		})
	})

	t.Run("unsuccessfully retrieve secret", func(t *testing.T) {
		s := ssmSecretStore{
			client: &mockedSSM{
				GetParameterFn: func(input *ssm.GetParameterInput) (*ssm.GetParameterOutput, error) {
					return nil, fmt.Errorf("failed due to any reason")
				},
			},
		}
		req := secretstores.GetSecretRequest{
			Name:     "/aws/dev/secret",
			Metadata: map[string]string{},
		}
		_, err := s.GetSecret(req)
		assert.NotNil(t, err)
	})
}

func TestGetBulkSecrets(t *testing.T) {
	t.Run("successfully retrieve bulk secrets", func(t *testing.T) {
		s := ssmSecretStore{
			client: &mockedSSM{
				DescribeParametersFn: func(*ssm.DescribeParametersInput) (*ssm.DescribeParametersOutput, error) {
					return &ssm.DescribeParametersOutput{NextToken: nil, Parameters: []*ssm.ParameterMetadata{
						{
							Name: aws.String("/aws/dev/secret1"),
						},
						{
							Name: aws.String("/aws/dev/secret2"),
						},
					}}, nil
				},
				GetParameterFn: func(input *ssm.GetParameterInput) (*ssm.GetParameterOutput, error) {
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
		output, e := s.BulkGetSecret(req)
		assert.Nil(t, e)
		assert.Contains(t, output.Data, "/aws/dev/secret1")
		assert.Contains(t, output.Data, "/aws/dev/secret2")
	})

	t.Run("unsuccessfully retrieve bulk secrets on get parameter", func(t *testing.T) {
		s := ssmSecretStore{
			client: &mockedSSM{
				DescribeParametersFn: func(*ssm.DescribeParametersInput) (*ssm.DescribeParametersOutput, error) {
					return &ssm.DescribeParametersOutput{NextToken: nil, Parameters: []*ssm.ParameterMetadata{
						{
							Name: aws.String("/aws/dev/secret1"),
						},
						{
							Name: aws.String("/aws/dev/secret2"),
						},
					}}, nil
				},
				GetParameterFn: func(input *ssm.GetParameterInput) (*ssm.GetParameterOutput, error) {
					return nil, fmt.Errorf("failed due to any reason")
				},
			},
		}
		req := secretstores.BulkGetSecretRequest{
			Metadata: map[string]string{},
		}
		_, err := s.BulkGetSecret(req)
		assert.NotNil(t, err)
	})

	t.Run("unsuccessfully retrieve bulk secrets on describe parameter", func(t *testing.T) {
		s := ssmSecretStore{
			client: &mockedSSM{
				DescribeParametersFn: func(*ssm.DescribeParametersInput) (*ssm.DescribeParametersOutput, error) {
					return nil, fmt.Errorf("failed due to any reason")
				},
			},
		}
		req := secretstores.BulkGetSecretRequest{
			Metadata: map[string]string{},
		}
		_, err := s.BulkGetSecret(req)
		assert.NotNil(t, err)
	})
}
