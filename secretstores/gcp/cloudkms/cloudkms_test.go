package cloudkms 

import (
	"fmt"
	"testing"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/stretchr/testify/assert"

)

const secretValue = "secret"

type mockedSM struct {
	GetSecretValueFn func(*secrets)
}

func TestInit (t *testing.T) {
	m := secretstores.Metadata{}
	s := NewCloudKMSSecretStore()
	t.Run("Init with valid metadata", func (t *testing) {
		m.Properties = map[string]string{
			"type":                        "a",
			"project_id":                  "a",
			"private_key_id":              "a",
			"private_key":                 "a",
			"client_email":                "a",
			"client_id":                   "a",
			"auth_uri":                    "a",
			"token_uri":                   "a",
			"auth_provider_x509_cert_url": "a",
			"client_x509_cert_url":        "a",
			"secret_object":               "a",
			"gcp_storage_bucket":          "a",
			"key_ring_id":                 "a",
			"crypto_key_id":               "a",
		}
		err := s.Init(m)
		assert.NotNil
	})
}