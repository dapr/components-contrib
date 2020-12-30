// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rabbitmq

import (
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	const queueName = "test-queue"
	const host = "test-host"
	var oneSecondTTL time.Duration = time.Second

	testCases := []struct {
		name                     string
		properties               map[string]string
		expectedDeleteWhenUnused bool
		expectedDurable          bool
		expectedTTL              *time.Duration
		expectedPrefetchCount    int
	}{
		{
			name:                     "Delete / Durable",
			properties:               map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "true", "durable": "true"},
			expectedDeleteWhenUnused: true,
			expectedDurable:          true,
		},
		{
			name:                     "Not Delete / Not durable",
			properties:               map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "false", "durable": "false"},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
		},
		{
			name:                     "With one second TTL",
			properties:               map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "false", "durable": "false", metadata.TTLMetadataKey: "1"},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
			expectedTTL:              &oneSecondTTL,
		},
		{
			name:                     "Empty TTL",
			properties:               map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "false", "durable": "false", metadata.TTLMetadataKey: ""},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
		},
		{
			name:                     "With one prefetchCount",
			properties:               map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "false", "durable": "false", "prefetchCount": "1"},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
			expectedPrefetchCount:    1,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			m := bindings.Metadata{}
			m.Properties = tt.properties
			r := RabbitMQ{logger: logger.NewLogger("test")}
			err := r.parseMetadata(m)
			assert.Nil(t, err)
			assert.Equal(t, queueName, r.metadata.QueueName)
			assert.Equal(t, host, r.metadata.Host)
			assert.Equal(t, tt.expectedDeleteWhenUnused, r.metadata.DeleteWhenUnused)
			assert.Equal(t, tt.expectedDurable, r.metadata.Durable)
			assert.Equal(t, tt.expectedTTL, r.metadata.defaultQueueTTL)
			assert.Equal(t, tt.expectedPrefetchCount, r.metadata.PrefetchCount)
		})
	}
}

func TestParseMetadataWithInvalidTTL(t *testing.T) {
	const queueName = "test-queue"
	const host = "test-host"

	testCases := []struct {
		name       string
		properties map[string]string
	}{
		{
			name:       "Whitespaces TTL",
			properties: map[string]string{"queueName": queueName, "host": host, metadata.TTLMetadataKey: "  "},
		},
		{
			name:       "Negative ttl",
			properties: map[string]string{"queueName": queueName, "host": host, metadata.TTLMetadataKey: "-1"},
		},
		{
			name:       "Non-numeric ttl",
			properties: map[string]string{"queueName": queueName, "host": host, metadata.TTLMetadataKey: "abc"},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			m := bindings.Metadata{}
			m.Properties = tt.properties
			r := RabbitMQ{logger: logger.NewLogger("test")}
			err := r.parseMetadata(m)
			assert.NotNil(t, err)
		})
	}
}
