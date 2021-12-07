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

package eventhubs

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
)

func TestParseEventHubsMetadata(t *testing.T) {
	t.Run("test valid configuration", func(t *testing.T) {
		props := map[string]string{"connectionString": "fake", "consumerID": "mygroup", "storageAccountName": "account", "storageAccountKey": "key", "storageContainerName": "container"}

		metadata := pubsub.Metadata{Properties: props}
		m, err := parseEventHubsMetadata(metadata)

		assert.NoError(t, err)
		assert.Equal(t, m.connectionString, "fake")
		assert.Equal(t, m.storageAccountName, "account")
		assert.Equal(t, m.storageAccountKey, "key")
		assert.Equal(t, m.storageContainerName, "container")
		assert.Equal(t, m.consumerGroup, "mygroup")
	})

	type invalidConfigTestCase struct {
		name   string
		config map[string]string
		errMsg string
	}
	invalidConfigTestCases := []invalidConfigTestCase{
		{
			"missing consumerID",
			map[string]string{"connectionString": "fake", "storageAccountName": "account", "storageAccountKey": "key", "storageContainerName": "container"},
			missingConsumerIDErrorMsg,
		},
		{
			"missing connectionString",
			map[string]string{"consumerID": "fake", "storageAccountName": "account", "storageAccountKey": "key", "storageContainerName": "container"},
			missingConnectionStringErrorMsg,
		},
		{
			"missing storageAccountName",
			map[string]string{"consumerID": "fake", "connectionString": "fake", "storageAccountKey": "key", "storageContainerName": "container"},
			missingStorageAccountNameErrorMsg,
		},
		{
			"missing storageAccountKey",
			map[string]string{"consumerID": "fake", "connectionString": "fake", "storageAccountName": "name", "storageContainerName": "container"},
			missingStorageAccountKeyErrorMsg,
		},
		{
			"missing storageContainerName",
			map[string]string{"consumerID": "fake", "connectionString": "fake", "storageAccountName": "name", "storageAccountKey": "key"},
			missingStorageContainerNameErrorMsg,
		},
	}

	for _, c := range invalidConfigTestCases {
		t.Run(c.name, func(t *testing.T) {
			metadata := pubsub.Metadata{Properties: c.config}
			_, err := parseEventHubsMetadata(metadata)
			assert.Error(t, err)
			assert.Equal(t, err.Error(), c.errMsg)
		})
	}
}
