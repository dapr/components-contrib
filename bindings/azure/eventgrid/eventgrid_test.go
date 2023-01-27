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

package eventgrid

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"tenantId":              "a",
		"subscriptionId":        "b",
		"clientId":              "c",
		"clientSecret":          "d",
		"subscriberEndpoint":    "e",
		"handshakePort":         "f",
		"scope":                 "g",
		"eventSubscriptionName": "h",
		"accessKey":             "i",
		"topicEndpoint":         "j",
	}

	eh := AzureEventGrid{
		logger: logger.NewLogger("test"),
	}
	meta, err := eh.parseMetadata(m)

	assert.Nil(t, err)
	assert.Equal(t, "a", meta.azureTenantID)
	assert.Equal(t, "b", meta.azureSubscriptionID)
	assert.Equal(t, "c", meta.azureClientID)
	assert.Equal(t, "e", meta.SubscriberEndpoint)
	assert.Equal(t, "f", meta.HandshakePort)
	assert.Equal(t, "g", meta.Scope)
	assert.Equal(t, "h", meta.EventSubscriptionName)
	assert.Equal(t, "i", meta.AccessKey)
	assert.Equal(t, "j", meta.TopicEndpoint)
}
