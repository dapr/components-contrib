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
	oneSecondDuration := time.Second

	testCases := []struct {
		name                               string
		properties                         map[string]string
		expectedConnectionString           string
		expectedQueueName                  string
		expectedTTL                        time.Duration
		expectedTimeoutInSec               int
		expectedMaxConnectionRecoveryInSec int
		expectedMinConnectionRecoveryInSec int
		expectedMaxRetriableErrorsPerSec   int
		expectedMaxActiveMessages          int
		expectedLockRenewalInSec           int
		expectedMaxConcurrentHandlers      int
	}{
		{
			name:                               "ConnectionString and queue name",
			properties:                         map[string]string{"connectionString": "connString", "queueName": "queue1"},
			expectedConnectionString:           "connString",
			expectedQueueName:                  "queue1",
			expectedTTL:                        defaultMessageTimeToLive,
			expectedTimeoutInSec:               defaultTimeoutInSec,
			expectedMaxConnectionRecoveryInSec: defaultMaxConnectionRecoveryInSec,
			expectedMinConnectionRecoveryInSec: defaultMinConnectionRecoveryInSec,
			expectedMaxRetriableErrorsPerSec:   defaultMaxRetriableErrorsPerSec,
			expectedMaxActiveMessages:          defaultMaxActiveMessages,
			expectedLockRenewalInSec:           defaultLockRenewalInSec,
			expectedMaxConcurrentHandlers:      defaultMaxConcurrentHandlers,
		},
		{
			name:                               "ConnectionString, queue name and all optional values",
			properties:                         map[string]string{"connectionString": "connString", "queueName": "queue1", "timeoutInSec": "30", "minConnectionRecoveryInSec": "1", "maxConnectionRecoveryInSec": "200", "maxRetriableErrorsPerSec": "20", "maxActiveMessages": "10", "maxConcurrentHandlers": "2", "lockRenewalInSec": "30"},
			expectedConnectionString:           "connString",
			expectedQueueName:                  "queue1",
			expectedTTL:                        defaultMessageTimeToLive,
			expectedTimeoutInSec:               30,
			expectedMaxConnectionRecoveryInSec: 200,
			expectedMinConnectionRecoveryInSec: 1,
			expectedMaxRetriableErrorsPerSec:   20,
			expectedMaxActiveMessages:          10,
			expectedMaxConcurrentHandlers:      2,
			expectedLockRenewalInSec:           30,
		},
		{
			name:                               "Empty TTL",
			properties:                         map[string]string{"connectionString": "connString", "queueName": "queue1", metadata.TTLMetadataKey: ""},
			expectedConnectionString:           "connString",
			expectedQueueName:                  "queue1",
			expectedTTL:                        defaultMessageTimeToLive,
			expectedTimeoutInSec:               defaultTimeoutInSec,
			expectedMaxConnectionRecoveryInSec: defaultMaxConnectionRecoveryInSec,
			expectedMinConnectionRecoveryInSec: defaultMinConnectionRecoveryInSec,
			expectedMaxRetriableErrorsPerSec:   defaultMaxRetriableErrorsPerSec,
			expectedMaxActiveMessages:          defaultMaxActiveMessages,
			expectedLockRenewalInSec:           defaultLockRenewalInSec,
			expectedMaxConcurrentHandlers:      defaultMaxConcurrentHandlers,
		},
		{
			name:                               "With TTL",
			properties:                         map[string]string{"connectionString": "connString", "queueName": "queue1", metadata.TTLMetadataKey: "1"},
			expectedConnectionString:           "connString",
			expectedQueueName:                  "queue1",
			expectedTTL:                        oneSecondDuration,
			expectedTimeoutInSec:               defaultTimeoutInSec,
			expectedMaxConnectionRecoveryInSec: defaultMaxConnectionRecoveryInSec,
			expectedMinConnectionRecoveryInSec: defaultMinConnectionRecoveryInSec,
			expectedMaxRetriableErrorsPerSec:   defaultMaxRetriableErrorsPerSec,
			expectedMaxActiveMessages:          defaultMaxActiveMessages,
			expectedLockRenewalInSec:           defaultLockRenewalInSec,
			expectedMaxConcurrentHandlers:      defaultMaxConcurrentHandlers,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			m := bindings.Metadata{}
			m.Properties = tt.properties
			a := NewAzureServiceBusQueues(logger.NewLogger("test")).(*AzureServiceBusQueues)
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

			a := NewAzureServiceBusQueues(logger.NewLogger("test")).(*AzureServiceBusQueues)
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
			a := NewAzureServiceBusQueues(logger.NewLogger("test")).(*AzureServiceBusQueues)
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
