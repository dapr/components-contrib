// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package azureservicebus

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
)

func getFakeProperties() map[string]string {
	return map[string]string{
		connStringKey:       "fakeConnectionString",
		consumerIDKey:       "fakeConId",
		maxDeliveryCountKey: "10",
		timeoutInSecKey:     "90",
	}
}

func TestParseServiceBusMetadata(t *testing.T) {
	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}

		// act
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[connStringKey], m.ConnectionString)
		assert.Equal(t, fakeProperties[consumerIDKey], m.ConsumerID)
		assert.Equal(t, 10, m.MaxDeliveryCount)
		assert.Equal(t, 90, m.TimeoutInSec)
	})

	t.Run("connectionstring is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[connStringKey] = ""

		// act
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Error(t, errors.New(""), err)
		assert.Empty(t, m.ConnectionString)
	})

	t.Run("default max delivery count", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[maxDeliveryCountKey] = ""

		// act
		m, _ := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Equal(t, m.MaxDeliveryCount, 10)
	})

	t.Run("invalid max delivery count", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[maxDeliveryCountKey] = "invalid_number"

		// act
		m, _ := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Equal(t, m.MaxDeliveryCount, 10)
	})

	t.Run("default timeout", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[timeoutInSecKey] = ""

		// act
		m, _ := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Equal(t, m.TimeoutInSec, 60)
	})

	t.Run("invalid timeout", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[timeoutInSecKey] = "invalid_number"

		// act
		m, _ := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Equal(t, m.TimeoutInSec, 60)
	})
}
