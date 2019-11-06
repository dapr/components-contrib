// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import (
	"encoding/json"
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"auth_provider_x509_cert_url": "https://auth", "auth_uri": "https://auth", "client_x509_cert_url": "https://cert", "client_email": "test@test.com", "client_id": "id", "private_key": "****",
		"private_key_id": "key_id", "project_id": "project1", "token_uri": "https://token", "type": "serviceaccount", "topic": "t1", "subscription": "s1"}
	ps := GCPPubSub{}
	b, err := ps.parseMetadata(m)
	assert.Nil(t, err)

	var pubsubMeta pubSubMetadata
	err = json.Unmarshal(b, &pubsubMeta)
	assert.Nil(t, err)

	assert.Equal(t, "s1", pubsubMeta.Subscription)
	assert.Equal(t, "t1", pubsubMeta.Topic)
	assert.Equal(t, "https://auth", pubsubMeta.AuthProviderCertURL)
	assert.Equal(t, "https://auth", pubsubMeta.AuthURI)
	assert.Equal(t, "https://cert", pubsubMeta.ClientCertURL)
	assert.Equal(t, "test@test.com", pubsubMeta.ClientEmail)
	assert.Equal(t, "id", pubsubMeta.ClientID)
	privateKey, err := pubsubMeta.PrivateKey.Open()
	assert.NoError(t, err)
	defer privateKey.Destroy()
	assert.Equal(t, "****", privateKey.String())
	assert.Equal(t, "key_id", pubsubMeta.PrivateKeyID)
	assert.Equal(t, "project1", pubsubMeta.ProjectID)
	assert.Equal(t, "https://token", pubsubMeta.TokenURI)
	assert.Equal(t, "serviceaccount", pubsubMeta.Type)
}
