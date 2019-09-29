package pubsub

import (
	"encoding/json"
	"testing"

	"github.com/actionscore/components-contrib/bindings"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"auth_provider_x509_cert_url": "a", "auth_uri": "a", "Bucket": "a", "client_x509_cert_url": "a", "client_email": "a", "client_id": "a", "private_key": "a",
		"private_key_id": "a", "project_id": "a", "token_uri": "a", "type": "a", "topic": "t1", "subscription": "s1"}
	ps := GCPPubSub{}
	b, err := ps.parseMetadata(m)
	assert.Nil(t, err)

	var pubsubMeta pubSubMetadata
	err = json.Unmarshal(b, &pubsubMeta)
	assert.Nil(t, err)

	assert.Equal(t, "s1", pubsubMeta.Subscription)
	assert.Equal(t, "t1", pubsubMeta.Topic)
	assert.Equal(t, "a", pubsubMeta.AuthProviderCertURL)
	assert.Equal(t, "a", pubsubMeta.AuthURI)
	assert.Equal(t, "a", pubsubMeta.ClientCertURL)
	assert.Equal(t, "a", pubsubMeta.ClientEmail)
	assert.Equal(t, "a", pubsubMeta.ClientID)
	assert.Equal(t, "a", pubsubMeta.PrivateKey)
	assert.Equal(t, "a", pubsubMeta.PrivateKeyID)
	assert.Equal(t, "a", pubsubMeta.ProjectID)
	assert.Equal(t, "a", pubsubMeta.TokenURI)
	assert.Equal(t, "a", pubsubMeta.Type)
}
