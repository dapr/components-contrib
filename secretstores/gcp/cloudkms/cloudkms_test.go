package cloudkms

import (
	"fmt"
	"testing"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	m := secretstores.Metadata{}
	s := NewCloudKMSSecretStore(logger.NewLogger("test"))
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
	})

	t.Run("Init with missing metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"dummy": "a",
		}
		err := s.Init(m)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("error creating cloudkms client: missing 'type' field in credentials"))
	})
}
