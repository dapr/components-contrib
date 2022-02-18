/*
Copyright 2022 The Dapr Authors
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
