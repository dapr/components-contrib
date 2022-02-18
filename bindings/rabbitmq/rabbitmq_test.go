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

package rabbitmq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
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
		expectedExclusive        bool
		expectedTTL              *time.Duration
		expectedPrefetchCount    int
		expectedMaxPriority      *uint8
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
		{
			name:                     "Exclusive Queue",
			properties:               map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "false", "durable": "false", "exclusive": "true"},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
			expectedExclusive:        true,
		},
		{
			name:                     "With maxPriority",
			properties:               map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "false", "durable": "false", "maxPriority": "1"},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
			expectedMaxPriority: func() *uint8 {
				v := uint8(1)

				return &v
			}(),
		},
		{
			name:                     "With maxPriority(> 255)",
			properties:               map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "false", "durable": "false", "maxPriority": "256"},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
			expectedMaxPriority: func() *uint8 {
				v := uint8(255)

				return &v
			}(),
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
			assert.Equal(t, tt.expectedExclusive, r.metadata.Exclusive)
			assert.Equal(t, tt.expectedMaxPriority, r.metadata.MaxPriority)
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

func TestParseMetadataWithInvalidMaxPriority(t *testing.T) {
	const queueName = "test-queue"
	const host = "test-host"

	testCases := []struct {
		name       string
		properties map[string]string
	}{
		{
			name:       "Whitespaces maxPriority",
			properties: map[string]string{"queueName": queueName, "host": host, "maxPriority": "  "},
		},
		{
			name:       "Negative maxPriority",
			properties: map[string]string{"queueName": queueName, "host": host, "maxPriority": "-1"},
		},
		{
			name:       "Non-numeric maxPriority",
			properties: map[string]string{"queueName": queueName, "host": host, "maxPriority": "abc"},
		},
		{
			name:       "Negative maxPriority",
			properties: map[string]string{"queueName": queueName, "host": host, "maxPriority": "-1"},
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
