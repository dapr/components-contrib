// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package eventhubs

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	t.Run("test valid configuration", func(t *testing.T) {

		props := map[string]string{connectionString: "fake", consumerGroup: "mygroup", storageAccountName: "account", storageAccountKey: "key", storageContainerName: "container"}

		bindingsMetadata := bindings.Metadata{Properties: props}

		m, err := parseMetadata(bindingsMetadata)

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
			"missing consumerGroup",
			map[string]string{connectionString: "fake", storageAccountName: "account", storageAccountKey: "key", storageContainerName: "container"},
			missingConsumerGroupErrorMsg,
		},
		{
			"missing connectionString",
			map[string]string{consumerGroup: "fake", storageAccountName: "account", storageAccountKey: "key", storageContainerName: "container"},
			missingConnectionStringErrorMsg,
		},
		{
			"missing storageAccountName",
			map[string]string{consumerGroup: "fake", connectionString: "fake", storageAccountKey: "key", storageContainerName: "container"},
			missingStorageAccountNameErrorMsg,
		},
		{
			"missing storageAccountKey",
			map[string]string{consumerGroup: "fake", connectionString: "fake", storageAccountName: "name", storageContainerName: "container"},
			missingStorageAccountKeyErrorMsg,
		},
		{
			"missing storageContainerName",
			map[string]string{consumerGroup: "fake", connectionString: "fake", storageAccountName: "name", storageAccountKey: "key"},
			missingStorageContainerNameErrorMsg,
		}}

	for _, c := range invalidConfigTestCases {
		t.Run(c.name, func(t *testing.T) {
			bindingsMetadata := bindings.Metadata{Properties: c.config}
			_, err := parseMetadata(bindingsMetadata)
			assert.Error(t, err)
			assert.Equal(t, err.Error(), c.errMsg)
		})
	}
}
