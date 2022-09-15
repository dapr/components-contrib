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
	"fmt"
	"testing"

	oos "github.com/alibabacloud-go/oos-20190601/client"
	util "github.com/alibabacloud-go/tea-utils/service"
	"github.com/alibabacloud-go/tea/tea"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

const (
	secretName  = "oos-secret-name"
	secretValue = "oos-secret-value"
)

type mockedParameterStore struct {
	parameterStoreClient
}

func (m *mockedParameterStore) GetSecretParameterWithOptions(request *oos.GetSecretParameterRequest, runtime *util.RuntimeOptions) (*oos.GetSecretParameterResponse, error) {
	return &oos.GetSecretParameterResponse{
		Body: &oos.GetSecretParameterResponseBody{
			Parameter: &oos.GetSecretParameterResponseBodyParameter{
				Name:  tea.String(secretName),
				Value: tea.String(secretValue),
			},
		},
	}, nil
}

func (m *mockedParameterStore) GetSecretParametersByPathWithOptions(request *oos.GetSecretParametersByPathRequest, runtime *util.RuntimeOptions) (*oos.GetSecretParametersByPathResponse, error) {
	return &oos.GetSecretParametersByPathResponse{
		Body: &oos.GetSecretParametersByPathResponseBody{
			Parameters: []*oos.GetSecretParametersByPathResponseBodyParameters{
				{
					Name:  tea.String(secretName),
					Value: tea.String(secretValue),
				},
			},
		},
	}, nil
}

type mockedParameterStoreReturnError struct {
	parameterStoreClient
}

func (m *mockedParameterStoreReturnError) GetSecretParameterWithOptions(request *oos.GetSecretParameterRequest, runtime *util.RuntimeOptions) (*oos.GetSecretParameterResponse, error) {
	return nil, fmt.Errorf("mocked error")
}

func (m *mockedParameterStoreReturnError) GetSecretParametersByPathWithOptions(request *oos.GetSecretParametersByPathRequest, runtime *util.RuntimeOptions) (*oos.GetSecretParametersByPathResponse, error) {
	return nil, fmt.Errorf("mocked error")
}

func TestInit(t *testing.T) {
	m := secretstores.Metadata{}
	s := NewParameterStore(logger.NewLogger("test"))
	t.Run("Init with valid metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"regionId":        "a",
			"accessKeyId":     "a",
			"accessKeySecret": "a",
		}
		err := s.Init(m)
		assert.Nil(t, err)
	})

	t.Run("Init without regionId", func(t *testing.T) {
		m.Properties = map[string]string{
			"accessKeyId":     "a",
			"accessKeySecret": "a",
		}
		err := s.Init(m)
		assert.NotNil(t, err)
	})
}

func TestGetSecret(t *testing.T) {
	t.Run("successfully get secret", func(t *testing.T) {
		t.Run("with valid secret name", func(t *testing.T) {
			s := oosSecretStore{
				client: &mockedParameterStore{},
			}

			req := secretstores.GetSecretRequest{
				Name:     secretName,
				Metadata: map[string]string{},
			}
			output, e := s.GetSecret(context.Background(), req)
			assert.Nil(t, e)
			assert.Equal(t, secretValue, output.Data[req.Name])
		})

		t.Run("with valid secret name and version", func(t *testing.T) {
			s := oosSecretStore{
				client: &mockedParameterStore{},
			}

			req := secretstores.GetSecretRequest{
				Name: secretName,
				Metadata: map[string]string{
					"version_id": "1",
				},
			}
			output, e := s.GetSecret(context.Background(), req)
			assert.Nil(t, e)
			assert.Equal(t, secretValue, output.Data[req.Name])
		})
	})

	t.Run("unsuccessfully get secret", func(t *testing.T) {
		t.Run("with invalid secret version", func(t *testing.T) {
			s := oosSecretStore{
				client: &mockedParameterStore{},
			}

			req := secretstores.GetSecretRequest{
				Name: secretName,
				Metadata: map[string]string{
					"version_id": "not-number",
				},
			}
			_, e := s.GetSecret(context.Background(), req)
			assert.NotNil(t, e)
		})

		t.Run("with parameter store retrieve error", func(t *testing.T) {
			s := oosSecretStore{
				client: &mockedParameterStoreReturnError{},
			}

			req := secretstores.GetSecretRequest{
				Name:     secretName,
				Metadata: map[string]string{},
			}
			_, e := s.GetSecret(context.Background(), req)
			assert.NotNil(t, e)
		})
	})
}

func TestBulkGetSecret(t *testing.T) {
	t.Run("successfully bulk get secret", func(t *testing.T) {
		t.Run("without path", func(t *testing.T) {
			s := oosSecretStore{
				client: &mockedParameterStore{},
			}

			req := secretstores.BulkGetSecretRequest{
				Metadata: map[string]string{},
			}
			output, e := s.BulkGetSecret(context.Background(), req)
			assert.Nil(t, e)
			assert.Contains(t, output.Data, secretName)
		})

		t.Run("with path", func(t *testing.T) {
			s := oosSecretStore{
				client: &mockedParameterStore{},
			}

			req := secretstores.BulkGetSecretRequest{
				Metadata: map[string]string{
					"path": "/oos/",
				},
			}
			output, e := s.BulkGetSecret(context.Background(), req)
			assert.Nil(t, e)
			assert.Contains(t, output.Data, secretName)
		})
	})

	t.Run("unsuccessfully bulk get secret", func(t *testing.T) {
		t.Run("with parameter store retrieve error", func(t *testing.T) {
			s := oosSecretStore{
				client: &mockedParameterStoreReturnError{},
			}

			req := secretstores.BulkGetSecretRequest{
				Metadata: map[string]string{},
			}
			_, e := s.BulkGetSecret(context.Background(), req)
			assert.NotNil(t, e)
		})
	})
}

func TestGetFeatures(t *testing.T) {
	m := secretstores.Metadata{}
	s := NewParameterStore(logger.NewLogger("test"))
	s.Init(m)
	t.Run("no features are advertised", func(t *testing.T) {
		f := s.Features()
		assert.Empty(t, f)
	})
}
