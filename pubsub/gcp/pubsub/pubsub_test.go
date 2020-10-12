package pubsub

import (
	"encoding/json"
	"testing"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{
		"auth_provider_x509_cert_url": "https://auth", "auth_uri": "https://auth", "client_x509_cert_url": "https://cert", "client_email": "test@test.com", "client_id": "id", "private_key": "****",
		"private_key_id": "key_id", "project_id": "project1", "token_uri": "https://token", "type": "serviceaccount",
	}
	ps := GCPPubSub{logger: logger.NewLogger("test")}
	b, err := ps.parseMetadata(m)
	assert.Nil(t, err)

	var pubsubMeta metadata
	err = json.Unmarshal(b, &pubsubMeta)
	assert.Nil(t, err)

	assert.Equal(t, "https://auth", pubsubMeta.AuthProviderCertURL)
	assert.Equal(t, "https://auth", pubsubMeta.AuthURI)
	assert.Equal(t, "https://cert", pubsubMeta.ClientCertURL)
	assert.Equal(t, "test@test.com", pubsubMeta.ClientEmail)
	assert.Equal(t, "id", pubsubMeta.ClientID)
	assert.Equal(t, "****", pubsubMeta.PrivateKey)
	assert.Equal(t, "key_id", pubsubMeta.PrivateKeyID)
	assert.Equal(t, "project1", pubsubMeta.ProjectID)
	assert.Equal(t, "https://token", pubsubMeta.TokenURI)
	assert.Equal(t, "serviceaccount", pubsubMeta.Type)
}
