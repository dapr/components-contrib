package secretmanager

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"github.com/aws/aws-sdk-go/service/secretsmanager/secretsmanageriface"
	"testing"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/stretchr/testify/assert"
)

type mockedSM struct {
	GetSecretValueFn func(*secretsmanager.GetSecretValueInput) (*secretsmanager.GetSecretValueOutput, error)
	secretsmanageriface.SecretsManagerAPI
}

func (m *mockedSM) GetSecretValue(input *secretsmanager.GetSecretValueInput) (*secretsmanager.GetSecretValueOutput, error) {
	return m.GetSecretValueFn(input)
}

func TestInit(t *testing.T) {
	m := secretstores.Metadata{}
	s := NewSecretManager()
	t.Run("Init with valid metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"AccessKey": "a",
			"Region":    "a",
			"SecretKey": "a",
		}
		err := s.Init(m)
		assert.Nil(t, err)
	})

	t.Run("Init with missing metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"Dummy": "a",
		}
		err := s.Init(m)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("missing value in metadata"))
	})
}

func TestGetSecret(t *testing.T) {
	t.Run("successfully retrieve secret", func(t *testing.T) {

		t.Run("without version id and version stage", func(t *testing.T) {
			s := smSecretStore{
				client: &mockedSM{
					GetSecretValueFn: func(input *secretsmanager.GetSecretValueInput) (*secretsmanager.GetSecretValueOutput, error) {
						assert.Nil(t, input.VersionId)
						assert.Nil(t, input.VersionStage)
						value := "secret"
						return &secretsmanager.GetSecretValueOutput{
							Name:         input.SecretId,
							SecretString: &value,
						}, nil
					},
				},
			}

			req := secretstores.GetSecretRequest{
				Name:     "/aws/secret/testing",
				Metadata: map[string]string{},
			}
			output, e := s.GetSecret(req)
			assert.Nil(t, e)
			assert.Equal(t, "secret", output.Data[req.Name])
		})

		t.Run("with version id", func(t *testing.T) {
			s := smSecretStore{
				client: &mockedSM{
					GetSecretValueFn: func(input *secretsmanager.GetSecretValueInput) (*secretsmanager.GetSecretValueOutput, error) {
						assert.NotNil(t, input.VersionId)
						value := "secret"
						return &secretsmanager.GetSecretValueOutput{
							Name:         input.SecretId,
							SecretString: &value,
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
			output, e := s.GetSecret(req)
			assert.Nil(t, e)
			assert.Equal(t, "secret", output.Data[req.Name])
		})

		t.Run("with version stage", func(t *testing.T) {
			s := smSecretStore{
				client: &mockedSM{
					GetSecretValueFn: func(input *secretsmanager.GetSecretValueInput) (*secretsmanager.GetSecretValueOutput, error) {
						assert.NotNil(t, input.VersionStage)
						value := "secret"
						return &secretsmanager.GetSecretValueOutput{
							Name:         input.SecretId,
							SecretString: &value,
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
			output, e := s.GetSecret(req)
			assert.Nil(t, e)
			assert.Equal(t, "secret", output.Data[req.Name])
		})
	})

	t.Run("unsuccessfully retrieve secret", func(t *testing.T) {
		s := smSecretStore{
			client: &mockedSM{
				GetSecretValueFn: func(input *secretsmanager.GetSecretValueInput) (*secretsmanager.GetSecretValueOutput, error) {
					return nil, fmt.Errorf("failed due to any reason")
				},
			},
		}
		req := secretstores.GetSecretRequest{
			Name:     "/aws/secret/testing",
			Metadata: map[string]string{},
		}
		_, err := s.GetSecret(req)
		assert.NotNil(t, err)
	})
}
