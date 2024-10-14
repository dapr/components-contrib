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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
		require.NoError(t, err)

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
		assert.True(t, b.EnableMessageOrdering)
	})

	t.Run("metadata is correct with implicit creds", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId": "superproject",
		}

		b, err := createMetadata(m)
		require.NoError(t, err)

		assert.Equal(t, "superproject", b.ProjectID)
		assert.Equal(t, "service_account", b.Type)
	})

	t.Run("missing project id", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{}
		_, err := createMetadata(m)
		require.Error(t, err)
	})

	t.Run("missing optional maxReconnectionAttempts", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId": "superproject",
		}

		pubSubMetadata, err := createMetadata(m)

		assert.Equal(t, 30, pubSubMetadata.MaxReconnectionAttempts)
		require.NoError(t, err)
	})

	t.Run("invalid optional maxReconnectionAttempts", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId": "superproject",
		}
		m.Properties["maxReconnectionAttempts"] = invalidNumber

		_, err := createMetadata(m)

		require.Error(t, err)
		require.ErrorContains(t, err, "maxReconnectionAttempts")
	})

	t.Run("missing optional connectionRecoveryInSec", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId": "superproject",
		}

		pubSubMetadata, err := createMetadata(m)

		assert.Equal(t, 2, pubSubMetadata.ConnectionRecoveryInSec)
		require.NoError(t, err)
	})

	t.Run("invalid optional connectionRecoveryInSec", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId": "superproject",
		}
		m.Properties["connectionRecoveryInSec"] = invalidNumber

		_, err := createMetadata(m)

		require.Error(t, err)
		require.ErrorContains(t, err, "connectionRecoveryInSec")
	})

	t.Run("valid optional maxOutstandingMessages", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId":              "test-project",
			"maxOutstandingMessages": "50",
		}

		md, err := createMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, 50, md.MaxOutstandingMessages, "MaxOutstandingMessages should match the provided configuration")
	})

	t.Run("missing optional maxOutstandingMessages", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId": "test-project",
		}

		md, err := createMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, 0, md.MaxOutstandingMessages)
	})

	t.Run("valid negative optional maxOutstandingMessages", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId":              "test-project",
			"maxOutstandingMessages": "-1",
		}

		md, err := createMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, -1, md.MaxOutstandingMessages, "MaxOutstandingMessages should match the provided configuration")
	})

	t.Run("invalid optional maxOutstandingMessages", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId":              "test-project",
			"maxOutstandingMessages": "foobar",
		}

		_, err := createMetadata(m)
		require.Error(t, err)
		require.ErrorContains(t, err, "maxOutstandingMessages")
	})

	t.Run("valid optional maxOutstandingBytes", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId":           "test-project",
			"maxOutstandingBytes": "1000000000",
		}

		md, err := createMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, 1000000000, md.MaxOutstandingBytes, "MaxOutstandingBytes should match the provided configuration")
	})

	t.Run("missing optional maxOutstandingBytes", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId": "test-project",
		}

		md, err := createMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, 0, md.MaxOutstandingBytes)
	})

	t.Run("valid negative optional maxOutstandingBytes", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId":           "test-project",
			"maxOutstandingBytes": "-1",
		}

		md, err := createMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, -1, md.MaxOutstandingBytes, "MaxOutstandingBytes should match the provided configuration")
	})

	t.Run("invalid optional maxOutstandingBytes", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId":           "test-project",
			"maxOutstandingBytes": "foobar",
		}

		_, err := createMetadata(m)
		require.Error(t, err)
		require.ErrorContains(t, err, "maxOutstandingBytes")
	})

	t.Run("valid optional maxConcurrentConnections", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId":                "test-project",
			"maxConcurrentConnections": "2",
		}

		md, err := createMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, 2, md.MaxConcurrentConnections, "MaxConcurrentConnections should match the provided configuration")
	})

	t.Run("missing optional maxConcurrentConnections", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId": "test-project",
		}

		md, err := createMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, 0, md.MaxConcurrentConnections)
	})

	t.Run("invalid optional maxConcurrentConnections", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId":                "test-project",
			"maxConcurrentConnections": "foobar",
		}

		_, err := createMetadata(m)
		require.Error(t, err)
		require.ErrorContains(t, err, "maxConcurrentConnections")
	})
	t.Run("valid ackDeadline", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId":   "test-project",
			"ackDeadline": "30s", // Valid custom ack deadline in seconds
		}

		md, err := createMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, 30*time.Second, md.AckDeadline, "AckDeadline should match the provided configuration")
	})

	t.Run("invalid ackDeadline", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId":   "test-project",
			"ackDeadline": "-10m", // Invalid ack deadline
		}

		_, err := createMetadata(m)
		require.Error(t, err, "Should return an error for invalid ackDeadline")
		assert.Contains(t, err.Error(), "invalid AckDeadline", "Error message should indicate the invalid ack deadline")
	})

	t.Run("default ackDeadline when not specified", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"projectId": "test-project", // No ackDeadline specified
		}

		md, err := createMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, defaultAckDeadline, md.AckDeadline, "Should use the default AckDeadline when none is specified")
	})
}
