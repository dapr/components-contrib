/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	kitmd "github.com/dapr/kit/metadata"
)

func TestInit(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"auth_provider_x509_cert_url": "https://auth", "auth_uri": "https://auth", "client_x509_cert_url": "https://cert", "client_email": "test@test.com", "client_id": "id", "private_key": "****",
		"private_key_id": "key_id", "project_id": "project1", "token_uri": "https://token", "type": "serviceaccount", "topic": "t1", "subscription": "s1",
	}
	var pubsubMeta pubSubMetadata
	err := kitmd.DecodeMetadata(m.Properties, &pubsubMeta)
	require.NoError(t, err)

	assert.Equal(t, "s1", pubsubMeta.Subscription)
	assert.Equal(t, "t1", pubsubMeta.Topic)
	assert.Equal(t, "https://auth", pubsubMeta.AuthProviderX509CertURL)
	assert.Equal(t, "https://auth", pubsubMeta.AuthURI)
	assert.Equal(t, "https://cert", pubsubMeta.ClientX509CertURL)
	assert.Equal(t, "test@test.com", pubsubMeta.ClientEmail)
	assert.Equal(t, "id", pubsubMeta.ClientID)
	assert.Equal(t, "****", pubsubMeta.PrivateKey)
	assert.Equal(t, "key_id", pubsubMeta.PrivateKeyID)
	assert.Equal(t, "project1", pubsubMeta.ProjectID)
	assert.Equal(t, "https://token", pubsubMeta.TokenURI)
	assert.Equal(t, "serviceaccount", pubsubMeta.Type)
}

func TestInit_MetadataCaseInsensitive(t *testing.T) {
	t.Run("snake_case metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"project_id":     "snake-project",
			"private_key_id": "snake-key",
			"client_email":   "snake@test.com",
			"topic":          "snake-topic",
		}

		var pubsubMeta pubSubMetadata
		err := kitmd.DecodeMetadata(m.Properties, &pubsubMeta)
		require.NoError(t, err)

		assert.Equal(t, "snake-project", pubsubMeta.ProjectID)
		assert.Equal(t, "snake-key", pubsubMeta.PrivateKeyID)
		assert.Equal(t, "snake@test.com", pubsubMeta.ClientEmail)
		assert.Equal(t, "snake-topic", pubsubMeta.Topic)
	})

	t.Run("camelCase metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"projectID":    "camel-project",
			"privateKeyID": "camel-key",
			"clientEmail":  "camel@test.com",
			"topic":        "camel-topic",
		}

		var pubsubMeta pubSubMetadata
		err := kitmd.DecodeMetadata(m.Properties, &pubsubMeta)
		require.NoError(t, err)

		assert.Equal(t, "camel-project", pubsubMeta.ProjectID)
		assert.Equal(t, "camel-key", pubsubMeta.PrivateKeyID)
		assert.Equal(t, "camel@test.com", pubsubMeta.ClientEmail)
		assert.Equal(t, "camel-topic", pubsubMeta.Topic)
	})
}
