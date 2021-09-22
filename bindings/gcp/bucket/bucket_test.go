// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package bucket

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestInit(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"auth_provider_x509_cert_url": "a", "auth_uri": "a", "Bucket": "a", "client_x509_cert_url": "a", "client_email": "a", "client_id": "a", "private_key": "a",
		"private_key_id": "a", "project_id": "a", "token_uri": "a", "type": "a",
	}
	gs := GCPStorage{logger: logger.NewLogger("test")}
	b, err := gs.parseMetadata(m)
	assert.Nil(t, err)

	var gm gcpMetadata
	err = json.Unmarshal(b, &gm)
	assert.Nil(t, err)

	assert.Equal(t, "a", gm.AuthProviderCertURL)
	assert.Equal(t, "a", gm.AuthURI)
	assert.Equal(t, "a", gm.Bucket)
	assert.Equal(t, "a", gm.ClientCertURL)
	assert.Equal(t, "a", gm.ClientEmail)
	assert.Equal(t, "a", gm.ClientID)
	assert.Equal(t, "a", gm.PrivateKey)
	assert.Equal(t, "a", gm.PrivateKeyID)
	assert.Equal(t, "a", gm.ProjectID)
	assert.Equal(t, "a", gm.TokenURI)
	assert.Equal(t, "a", gm.Type)
}
