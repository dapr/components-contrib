// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package servicebusqueues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	var oneSecondDuration time.Duration = time.Second

	testCases := []struct {
		name                     string
		properties               map[string]string
		expectedConnectionString string
		expectedQueueName        string
		expectedTTL              time.Duration
	}{
		{
			name:                     "ConnectionString and queue name",
			properties:               map[string]string{"connectionString": "connString", "queueName": "queue1"},
			expectedConnectionString: "connString",
			expectedQueueName:        "queue1",
			expectedTTL:              AzureServiceBusDefaultMessageTimeToLive,
		},
		{
			name:                     "Empty TTL",
			properties:               map[string]string{"connectionString": "connString", "queueName": "queue1", metadata.TTLMetadataKey: ""},
			expectedConnectionString: "connString",
			expectedQueueName:        "queue1",
			expectedTTL:              AzureServiceBusDefaultMessageTimeToLive,
		},
		{
			name:                     "With TTL",
			properties:               map[string]string{"connectionString": "connString", "queueName": "queue1", metadata.TTLMetadataKey: "1"},
			expectedConnectionString: "connString",
			expectedQueueName:        "queue1",
			expectedTTL:              oneSecondDuration,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			m := bindings.Metadata{}
			m.Properties = tt.properties
			a := NewAzureServiceBusQueues(logger.NewLogger("test"))
			meta, err := a.parseMetadata(m)
			assert.Nil(t, err)
			assert.Equal(t, tt.expectedConnectionString, meta.ConnectionString)
			assert.Equal(t, tt.expectedQueueName, meta.QueueName)
			assert.Equal(t, tt.expectedTTL, meta.ttl)
		})
	}
}

func TestParseMetadataWithInvalidTTL(t *testing.T) {
	testCases := []struct {
		name       string
		properties map[string]string
	}{
		{
			name:       "Whitespaces TTL",
			properties: map[string]string{"connectionString": "connString", "queueName": "queue1", metadata.TTLMetadataKey: "  "},
		},
		{
			name:       "Negative ttl",
			properties: map[string]string{"connectionString": "connString", "queueName": "queue1", metadata.TTLMetadataKey: "-1"},
		},
		{
			name:       "Non-numeric ttl",
			properties: map[string]string{"connectionString": "connString", "queueName": "queue1", metadata.TTLMetadataKey: "abc"},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			m := bindings.Metadata{}
			m.Properties = tt.properties

			a := NewAzureServiceBusQueues(logger.NewLogger("test"))
			_, err := a.parseMetadata(m)
			assert.NotNil(t, err)
		})
	}
}

func TestParseMetadataConnectionStringAndNamespaceNameExclusivity(t *testing.T) {
	testCases := []struct {
		name                     string
		properties               map[string]string
		expectedConnectionString string
		expectedNamespaceName    string
		expectedQueueName        string
		expectedErr              bool
	}{
		{
			name:                     "ConnectionString and queue name",
			properties:               map[string]string{"connectionString": "connString", "queueName": "queue1"},
			expectedConnectionString: "connString",
			expectedNamespaceName:    "",
			expectedQueueName:        "queue1",
			expectedErr:              false,
		},
		{
			name:                     "Empty TTL",
			properties:               map[string]string{"namespaceName": "testNamespace", "queueName": "queue1", metadata.TTLMetadataKey: ""},
			expectedConnectionString: "",
			expectedNamespaceName:    "testNamespace",
			expectedQueueName:        "queue1",
			expectedErr:              false,
		},
		{
			name:                     "With TTL",
			properties:               map[string]string{"connectionString": "connString", "namespaceName": "testNamespace", "queueName": "queue1", metadata.TTLMetadataKey: "1"},
			expectedConnectionString: "",
			expectedNamespaceName:    "",
			expectedQueueName:        "queue1",
			expectedErr:              true,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			m := bindings.Metadata{}
			m.Properties = tt.properties
			a := NewAzureServiceBusQueues(logger.NewLogger("test"))
			meta, err := a.parseMetadata(m)
			if tt.expectedErr {
				assert.NotNil(t, err)
			} else {
				assert.Equal(t, tt.expectedConnectionString, meta.ConnectionString)
				assert.Equal(t, tt.expectedQueueName, meta.QueueName)
				assert.Equal(t, tt.expectedNamespaceName, meta.NamespaceName)
			}
		})
	}
}
