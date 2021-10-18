package pubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
)

func TestInit(t *testing.T) {
	t.Run("metadata is correct with explicit creds", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId":               "superproject",
			"authProviderX509CertUrl": "https://authcerturl",
			"authUri":                 "https://auth",
			"clientX509CertUrl":       "https://cert",
			"clientEmail":             "test@test.com",
			"clientId":                "id",
			"privateKey":              "****",
			"privateKeyId":            "key_id",
			"identityProjectId":       "project1",
			"tokenUri":                "https://token",
			"type":                    "serviceaccount",
			"enableMessageOrdering":   "true",
		}
		b, err := createMetadata(m)
		assert.Nil(t, err)

		assert.Equal(t, "https://authcerturl", b.AuthProviderCertURL)
		assert.Equal(t, "https://auth", b.AuthURI)
		assert.Equal(t, "https://cert", b.ClientCertURL)
		assert.Equal(t, "test@test.com", b.ClientEmail)
		assert.Equal(t, "id", b.ClientID)
		assert.Equal(t, "****", b.PrivateKey)
		assert.Equal(t, "key_id", b.PrivateKeyID)
		assert.Equal(t, "project1", b.IdentityProjectID)
		assert.Equal(t, "https://token", b.TokenURI)
		assert.Equal(t, "serviceaccount", b.Type)
		assert.Equal(t, true, b.EnableMessageOrdering)
	})

	t.Run("metadata is correct with implicit creds", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId": "superproject",
		}

		b, err := createMetadata(m)
		assert.Nil(t, err)

		assert.Equal(t, "superproject", b.ProjectID)
		assert.Equal(t, "service_account", b.Type)
	})

	t.Run("missing project id", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{}
		_, err := createMetadata(m)
		assert.Error(t, err)
	})
}
