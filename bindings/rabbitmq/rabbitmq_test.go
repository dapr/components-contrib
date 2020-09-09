// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rabbitmq

import (
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	const queueName = "test-queue"
	const host = "test-host"
	var oneSecondTTL time.Duration = time.Second

	testCases := []struct {
		name                        string
		properties                  map[string]string
		expectedDeleteWhenUnused    bool
		expectedDurable             bool
		expectedTTL                 *time.Duration
		expectedStringPrefetchCount string
		expectedIntPrefetchCount    int
	}{
		{
			name:                     "Delete / Durable",
			properties:               map[string]string{"QueueName": queueName, "Host": host, "DeleteWhenUnused": "true", "Durable": "true"},
			expectedDeleteWhenUnused: true,
			expectedDurable:          true,
		},
		{
			name:                     "Not Delete / Not Durable",
			properties:               map[string]string{"QueueName": queueName, "Host": host, "DeleteWhenUnused": "false", "Durable": "false"},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
		},
		{
			name:                     "With one second TTL",
			properties:               map[string]string{"QueueName": queueName, "Host": host, "DeleteWhenUnused": "false", "Durable": "false", bindings.TTLMetadataKey: "1"},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
			expectedTTL:              &oneSecondTTL,
		},
		{
			name:                     "Empty TTL",
			properties:               map[string]string{"QueueName": queueName, "Host": host, "DeleteWhenUnused": "false", "Durable": "false", bindings.TTLMetadataKey: ""},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
		},
		{
			name:                        "With one PrefetchCount",
			properties:                  map[string]string{"QueueName": queueName, "Host": host, "DeleteWhenUnused": "false", "Durable": "false", "PrefetchCount": "1"},
			expectedDeleteWhenUnused:    false,
			expectedDurable:             false,
			expectedStringPrefetchCount: "1",
			expectedIntPrefetchCount:    1,
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
			assert.Equal(t, tt.expectedStringPrefetchCount, r.metadata.PrefetchCount)
			assert.Equal(t, tt.expectedIntPrefetchCount, r.metadata.prefetchCount)
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
			properties: map[string]string{"QueueName": queueName, "Host": host, bindings.TTLMetadataKey: "  "},
		},
		{
			name:       "Negative ttl",
			properties: map[string]string{"QueueName": queueName, "Host": host, bindings.TTLMetadataKey: "-1"},
		},
		{
			name:       "Non-numeric ttl",
			properties: map[string]string{"QueueName": queueName, "Host": host, bindings.TTLMetadataKey: "abc"},
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
