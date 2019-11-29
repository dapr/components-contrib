package cloudkms

import (
	"testing"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	m := secretstores.Metadata{}
	s := NewCloudKMSSecretStore()
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
			"secret_object":               "a",
			"gcp_storage_bucket":          "a",
			"key_ring_id":                 "a",
			"crypto_key_id":               "a",
		}
		err := s.Init(m)

		assert.Nil(t, err)
		assert.NotNil(t, s.cloudkmsclient)
		assert.NotNil(t, s.storageclient)
		//Check that metadata is populated
		assert.Equal(t, s.metadata.Type, m.Properties["type"])
		assert.Equal(t, s.metadata.ProjectID, m.Properties["project_id"])
		assert.Equal(t, s.metadata.PrivateKeyID, m.Properties["private_key_id"])
		assert.Equal(t, s.metadata.PrivateKey, m.Properties["private_key"])
		assert.Equal(t, s.metadata.ClientEmail, m.Properties["client_email"])
	})
}
