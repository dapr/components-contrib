// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package eventgrid

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"clientId":                  "a",
		"clientSecret":              "a",
		"eventGridSubscriptionName": "a",
		"resourceGroupName":         "a",
		"subscriberEndpoint":        "a",
		"subscriptionId":            "a",
		"tenantId":                  "a",
		"topicName":                 "a",
	}

	eh := AzureEventGrid{}
	meta, err := eh.parseMetadata(m)

	assert.Nil(t, err)
	assert.Equal(t, "a", meta.ClientID)
	assert.Equal(t, "a", meta.ClientSecret)
	assert.Equal(t, "a", meta.EventGridSubscriptionName)
	assert.Equal(t, "a", meta.ResourceGroupName)
	assert.Equal(t, "a", meta.SubscriberEndpoint)
	assert.Equal(t, "a", meta.SubscriptionID)
	assert.Equal(t, "a", meta.TenantID)
	assert.Equal(t, "a", meta.TopicName)
}
