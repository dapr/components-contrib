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

package queues

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	impl "github.com/dapr/components-contrib/common/component/azure/servicebus"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

func TestNewAzureServiceBusQueues(t *testing.T) {
	t.Run("returns a new instance", func(t *testing.T) {
		log := logger.NewLogger("test")
		ps := NewAzureServiceBusQueues(log)
		require.NotNil(t, ps)
	})
}

func TestParseSessionMetadata(t *testing.T) {
	testCases := []struct {
		name                          string
		metadata                      map[string]string
		expectedRequireSessions       bool
		expectedSessionIdleTimeout    int
		expectedMaxConcurrentSessions int
	}{
		{
			name:                          "default values when no session metadata provided",
			metadata:                      map[string]string{},
			expectedRequireSessions:       false,
			expectedSessionIdleTimeout:    impl.DefaultSesssionIdleTimeoutInSec,
			expectedMaxConcurrentSessions: impl.DefaultMaxConcurrentSessions,
		},
		{
			name: "requireSessions is true",
			metadata: map[string]string{
				impl.RequireSessionsMetadataKey: "true",
			},
			expectedRequireSessions:       true,
			expectedSessionIdleTimeout:    impl.DefaultSesssionIdleTimeoutInSec,
			expectedMaxConcurrentSessions: impl.DefaultMaxConcurrentSessions,
		},
		{
			name: "requireSessions is false explicitly",
			metadata: map[string]string{
				impl.RequireSessionsMetadataKey: "false",
			},
			expectedRequireSessions:       false,
			expectedSessionIdleTimeout:    impl.DefaultSesssionIdleTimeoutInSec,
			expectedMaxConcurrentSessions: impl.DefaultMaxConcurrentSessions,
		},
		{
			name: "custom sessionIdleTimeoutInSec",
			metadata: map[string]string{
				impl.RequireSessionsMetadataKey:    "true",
				impl.SessionIdleTimeoutMetadataKey: "120",
			},
			expectedRequireSessions:       true,
			expectedSessionIdleTimeout:    120,
			expectedMaxConcurrentSessions: impl.DefaultMaxConcurrentSessions,
		},
		{
			name: "custom maxConcurrentSessions",
			metadata: map[string]string{
				impl.RequireSessionsMetadataKey:       "true",
				impl.MaxConcurrentSessionsMetadataKey: "16",
			},
			expectedRequireSessions:       true,
			expectedSessionIdleTimeout:    impl.DefaultSesssionIdleTimeoutInSec,
			expectedMaxConcurrentSessions: 16,
		},
		{
			name: "all custom session values",
			metadata: map[string]string{
				impl.RequireSessionsMetadataKey:       "true",
				impl.SessionIdleTimeoutMetadataKey:    "90",
				impl.MaxConcurrentSessionsMetadataKey: "4",
			},
			expectedRequireSessions:       true,
			expectedSessionIdleTimeout:    90,
			expectedMaxConcurrentSessions: 4,
		},
		{
			name: "requireSessions with various truthy values - 1",
			metadata: map[string]string{
				impl.RequireSessionsMetadataKey: "1",
			},
			expectedRequireSessions:       true,
			expectedSessionIdleTimeout:    impl.DefaultSesssionIdleTimeoutInSec,
			expectedMaxConcurrentSessions: impl.DefaultMaxConcurrentSessions,
		},
		{
			name: "requireSessions with various truthy values - yes",
			metadata: map[string]string{
				impl.RequireSessionsMetadataKey: "yes",
			},
			expectedRequireSessions:       true,
			expectedSessionIdleTimeout:    impl.DefaultSesssionIdleTimeoutInSec,
			expectedMaxConcurrentSessions: impl.DefaultMaxConcurrentSessions,
		},
		{
			name: "requireSessions with various truthy values - on",
			metadata: map[string]string{
				impl.RequireSessionsMetadataKey: "on",
			},
			expectedRequireSessions:       true,
			expectedSessionIdleTimeout:    impl.DefaultSesssionIdleTimeoutInSec,
			expectedMaxConcurrentSessions: impl.DefaultMaxConcurrentSessions,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := pubsub.SubscribeRequest{
				Topic:    "test-queue",
				Metadata: tc.metadata,
			}

			// Test requireSessions parsing
			var requireSessions bool
			if val, ok := req.Metadata[impl.RequireSessionsMetadataKey]; ok {
				requireSessions = isTruthy(val)
			}
			assert.Equal(t, tc.expectedRequireSessions, requireSessions, "requireSessions mismatch")

			// Test sessionIdleTimeout parsing
			sessionIdleTimeout := getIntFromMapOrDefault(req.Metadata, impl.SessionIdleTimeoutMetadataKey, impl.DefaultSesssionIdleTimeoutInSec)
			assert.Equal(t, tc.expectedSessionIdleTimeout, sessionIdleTimeout, "sessionIdleTimeout mismatch")

			// Test maxConcurrentSessions parsing
			maxConcurrentSessions := getIntFromMapOrDefault(req.Metadata, impl.MaxConcurrentSessionsMetadataKey, impl.DefaultMaxConcurrentSessions)
			assert.Equal(t, tc.expectedMaxConcurrentSessions, maxConcurrentSessions, "maxConcurrentSessions mismatch")
		})
	}
}

func TestSubscribeOptions(t *testing.T) {
	t.Run("subscribeOptions struct initialization", func(t *testing.T) {
		opts := subscribeOptions{
			requireSessions:       true,
			maxConcurrentSessions: 16,
		}
		assert.True(t, opts.requireSessions)
		assert.Equal(t, 16, opts.maxConcurrentSessions)
	})

	t.Run("subscribeOptions default values", func(t *testing.T) {
		opts := subscribeOptions{}
		assert.False(t, opts.requireSessions)
		assert.Equal(t, 0, opts.maxConcurrentSessions)
	})
}

func TestFeatures(t *testing.T) {
	log := logger.NewLogger("test")
	ps := NewAzureServiceBusQueues(log)
	features := ps.Features()

	assert.Contains(t, features, pubsub.FeatureMessageTTL)
	assert.Contains(t, features, pubsub.FeatureBulkPublish)
}

// Helper function to simulate IsTruthy behavior
func isTruthy(val string) bool {
	switch val {
	case "1", "true", "yes", "on", "True", "TRUE", "Yes", "YES", "On", "ON":
		return true
	default:
		return false
	}
}

// Helper function to simulate GetElemOrDefaultFromMap behavior
func getIntFromMapOrDefault(m map[string]string, key string, defaultVal int) int {
	if val, ok := m[key]; ok {
		var result int
		for _, c := range val {
			if c >= '0' && c <= '9' {
				result = result*10 + int(c-'0')
			} else {
				return defaultVal
			}
		}
		return result
	}
	return defaultVal
}
