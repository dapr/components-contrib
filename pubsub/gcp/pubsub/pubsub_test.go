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

	"github.com/dapr/components-contrib/pubsub"
)

const (
	invalidNumber = "invalid_number"
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

	t.Run("missing optional maxReconnectionAttempts", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId": "superproject",
		}
		m.Properties[metadataMaxReconnectionAttemptsKey] = ""

		pubSubMetadata, err := createMetadata(m)

		assert.Equal(t, 30, pubSubMetadata.MaxReconnectionAttempts)
		assert.Nil(t, err)
	})

	t.Run("invalid optional maxReconnectionAttempts", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId": "superproject",
		}
		m.Properties[metadataMaxReconnectionAttemptsKey] = invalidNumber

		_, err := createMetadata(m)

		assert.Error(t, err)
		assertValidErrorMessage(t, err)
	})

	t.Run("missing optional connectionRecoveryInSec", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId": "superproject",
		}
		m.Properties[metadataConnectionRecoveryInSecKey] = ""

		pubSubMetadata, err := createMetadata(m)

		assert.Equal(t, 2, pubSubMetadata.ConnectionRecoveryInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid optional connectionRecoveryInSec", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId": "superproject",
		}
		m.Properties[metadataConnectionRecoveryInSecKey] = invalidNumber

		_, err := createMetadata(m)

		assert.Error(t, err)
		assertValidErrorMessage(t, err)
	})
}

func assertValidErrorMessage(t *testing.T, err error) {
	assert.Contains(t, err.Error(), errorMessagePrefix)
}
