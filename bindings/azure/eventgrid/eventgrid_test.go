// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package eventgrid

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"tenantId":              "a",
		"subscriptionId":        "a",
		"clientId":              "a",
		"clientSecret":          "a",
		"subscriberEndpoint":    "a",
		"handshakePort":         "a",
		"scope":                 "a",
		"eventSubscriptionName": "a",
		"accessKey":             "a",
		"topicEndpoint":         "a",
	}

	eh := AzureEventGrid{}
	meta, err := eh.parseMetadata(m)

	assert.Nil(t, err)
	assert.Equal(t, "a", meta.TenantID)
	assert.Equal(t, "a", meta.SubscriptionID)
	assert.Equal(t, "a", meta.ClientID)
	assert.Equal(t, "a", meta.ClientSecret)
	assert.Equal(t, "a", meta.SubscriberEndpoint)
	assert.Equal(t, "a", meta.HandshakePort)
	assert.Equal(t, "a", meta.Scope)
	assert.Equal(t, "a", meta.EventSubscriptionName)
	assert.Equal(t, "a", meta.AccessKey)
	assert.Equal(t, "a", meta.TopicEndpoint)
}
