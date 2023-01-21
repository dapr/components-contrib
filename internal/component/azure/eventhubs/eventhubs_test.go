/*
Copyright 2023 The Dapr Authors
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

package eventhubs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/kit/logger"
)

var testLogger = logger.NewLogger("test")

func TestParseEventHubsMetadata(t *testing.T) {
	t.Run("test valid connectionString configuration", func(t *testing.T) {
		metadata := map[string]string{"connectionString": "fake"}

		m, err := parseEventHubsMetadata(metadata, testLogger)

		require.NoError(t, err)
		assert.Equal(t, "fake", m.ConnectionString)
	})

	t.Run("test namespace given", func(t *testing.T) {
		metadata := map[string]string{"eventHubNamespace": "fake.servicebus.windows.net"}

		m, err := parseEventHubsMetadata(metadata, testLogger)

		require.NoError(t, err)
		assert.Equal(t, "fake.servicebus.windows.net", m.EventHubNamespace)
	})

	t.Run("test namespace adds FQDN", func(t *testing.T) {
		metadata := map[string]string{"eventHubNamespace": "fake"}

		m, err := parseEventHubsMetadata(metadata, testLogger)

		require.NoError(t, err)
		assert.Equal(t, "fake.servicebus.windows.net", m.EventHubNamespace)
	})

	t.Run("test both connectionString and eventHubNamespace given", func(t *testing.T) {
		metadata := map[string]string{
			"connectionString":  "fake",
			"eventHubNamespace": "fake",
		}

		_, err := parseEventHubsMetadata(metadata, testLogger)

		require.Error(t, err)
		assert.ErrorContains(t, err, "only one of connectionString or eventHubNamespace should be passed")
	})

	t.Run("test missing metadata", func(t *testing.T) {
		metadata := map[string]string{}

		_, err := parseEventHubsMetadata(metadata, testLogger)

		require.Error(t, err)
		assert.ErrorContains(t, err, "one of connectionString or eventHubNamespace is required")
	})
}

func TestConstructConnectionStringFromTopic(t *testing.T) {
	t.Run("valid connectionString without hub name", func(t *testing.T) {
		connectionString := "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=fakeKey;SharedAccessKey=key"
		topic := "testHub"

		metadata := map[string]string{
			"connectionString": connectionString,
		}
		aeh := &AzureEventHubs{logger: testLogger}
		err := aeh.Init(metadata)
		require.NoError(t, err)

		c, err := aeh.constructConnectionStringFromTopic(topic)
		require.NoError(t, err)
		assert.Equal(t, connectionString+";EntityPath=testHub", c)
	})

	t.Run("valid connectionString with hub name", func(t *testing.T) {
		connectionString := "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=fakeKey;SharedAccessKey=key;EntityPath=testHub"
		topic := "testHub"

		metadata := map[string]string{
			"connectionString": connectionString,
		}
		aeh := &AzureEventHubs{logger: testLogger}
		err := aeh.Init(metadata)
		require.NoError(t, err)

		c, err := aeh.constructConnectionStringFromTopic(topic)
		require.NoError(t, err)
		assert.Equal(t, connectionString, c)
	})

	t.Run("valid connectionString with different hub name", func(t *testing.T) {
		connectionString := "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=fakeKey;SharedAccessKey=key;EntityPath=testHub"
		topic := "differentHub"

		metadata := map[string]string{
			"connectionString": connectionString,
		}
		aeh := &AzureEventHubs{logger: testLogger}
		err := aeh.Init(metadata)
		require.NoError(t, err)

		c, err := aeh.constructConnectionStringFromTopic(topic)
		require.Error(t, err)
		assert.ErrorContains(t, err, "does not match the Event Hub name in the connection string")
		assert.Equal(t, "", c)
	})
}
