// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package azureservicebus

import (
	"errors"
	"testing"
	"context"

	"github.com/Azure/azure-service-bus-go"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
)

type fakeSubscription struct {
}

func (f *fakeSubscription) Close(ctx context.Context) error {
	return nil
}

func (f *fakeSubscription) Receive(ctx context.Context, handler servicebus.Handler) error {
	
	return nil
}

func getFakeProperties() map[string]string {
	return map[string]string{
		connString: "fakeConnectionString",
		subscriberID: "fakeSubId",
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
		assert.Equal(t, fakeProperties[connString], m.connectionString)
	})

	t.Run("connectionstring is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[connString] = ""

		// act
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Error(t, errors.New(""), err)
		assert.Empty(t, m.connectionString)
	})

	t.Run("default subscriptionId", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[subscriberID] = ""

		// act
		m, _ := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.NotEmpty(t, m.subscriberID)
	})
}