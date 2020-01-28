package secretmanager

import (
	"testing"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	m := secretstores.Metadata{}
	sm := NewSecreteManager()
	t.Run("Init with valid metadata", func(t *testing.T) {
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
		assert.Nil(t, err)
	})

	t.Run("Get Secret", func(t *testing.T) {
		v, err := sm.GetSecret(secretstores.GetSecretRequest{Name: "test"})
		assert.Nil(t, err)
		assert.Equal(t, secretstores.GetSecretResponse{Data: map[string]string{"_value": "abcd"}}, v)
	})

	// t.Run("Init with missing metadata", func(t *testing.T) {
	// 	m.Properties = map[string]string{
	// 		"dummy": "a",
	// 	}
	// 	err := sm.Init(m)
	// 	assert.NotNil(t, err)
	// 	assert.Equal(t, err, fmt.Errorf("error creating cloudkms client: missing 'type' field in credentials"))
	// })
}
