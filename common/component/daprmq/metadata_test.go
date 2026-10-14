/*
Copyright 2026 The Dapr Authors
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

package daprmq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/pubsub"
)

func requiredProps() map[string]string {
	return map[string]string{
		"httpEndpoint": "http://localhost:8002",
		"grpcEndpoint": "localhost:8102",
		"consumerID":   "app",
	}
}

func TestParseMetadataDefaults(t *testing.T) {
	md, err := ParseMetadata(requiredProps(), true)
	require.NoError(t, err)

	assert.Equal(t, "http://localhost:8002", md.HTTPEndpoint)
	assert.Equal(t, "localhost:8102", md.GRPCEndpoint)
	assert.Equal(t, "app", md.ConsumerID)
	assert.Equal(t, 30*time.Second, md.LockTTL)
	assert.Equal(t, 10, md.DequeueCount)
	assert.Equal(t, time.Second, md.PollInterval)
	assert.True(t, md.AllowCompetingConsumers)
	assert.Equal(t, pubsub.Parallel, md.ConcurrencyMode)
	assert.Equal(t, 0, md.MaxConcurrentHandlers)
	assert.Equal(t, 10*time.Second, md.LockRenewalInterval)
	assert.Equal(t, ReceiveModeStream, md.ReceiveMode)
	assert.Equal(t, 100, md.MaxActiveMessages)
	assert.Equal(t, 10, md.MaxRetriableErrorsPerSec)
}

func TestParseMetadataOverrides(t *testing.T) {
	props := requiredProps()
	props["lockTTL"] = "2m"
	props["dequeueCount"] = "50"
	props["pollInterval"] = "250ms"
	props["allowCompetingConsumers"] = "false"
	props["concurrencyMode"] = "single"
	props["maxConcurrentHandlers"] = "3"
	props["lockRenewalInterval"] = "45s"
	props["receiveMode"] = "poll"
	props["maxActiveMessages"] = "500"
	props["maxRetriableErrorsPerSec"] = "0"

	md, err := ParseMetadata(props, true)
	require.NoError(t, err)

	assert.Equal(t, 2*time.Minute, md.LockTTL)
	assert.Equal(t, 50, md.DequeueCount)
	assert.Equal(t, 250*time.Millisecond, md.PollInterval)
	assert.False(t, md.AllowCompetingConsumers)
	assert.Equal(t, pubsub.Single, md.ConcurrencyMode)
	assert.Equal(t, 3, md.MaxConcurrentHandlers)
	assert.Equal(t, 45*time.Second, md.LockRenewalInterval)
	assert.Equal(t, ReceiveModePoll, md.ReceiveMode)
	assert.Equal(t, 500, md.MaxActiveMessages)
	assert.Equal(t, 0, md.MaxRetriableErrorsPerSec)
}

func TestParseMetadataZeroLockRenewalIntervalDisablesRenewal(t *testing.T) {
	props := requiredProps()
	props["lockRenewalInterval"] = "0s"

	md, err := ParseMetadata(props, true)
	require.NoError(t, err)

	assert.Zero(t, md.LockRenewalInterval)
}

func TestParseMetadataRejectsInvalidValues(t *testing.T) {
	cases := map[string]func(map[string]string){
		"missing httpEndpoint":                  func(p map[string]string) { delete(p, "httpEndpoint") },
		"missing grpcEndpoint":                  func(p map[string]string) { delete(p, "grpcEndpoint") },
		"missing consumerID":                    func(p map[string]string) { delete(p, "consumerID") },
		"lockTTL under 1s":                      func(p map[string]string) { p["lockTTL"] = "500ms" },
		"dequeueCount 0":                        func(p map[string]string) { p["dequeueCount"] = "0" },
		"dequeueCount over 1000":                func(p map[string]string) { p["dequeueCount"] = "1001" },
		"pollInterval 0":                        func(p map[string]string) { p["pollInterval"] = "0s" },
		"maxConcurrentHandlers negative":        func(p map[string]string) { p["maxConcurrentHandlers"] = "-1" },
		"unknown receiveMode":                   func(p map[string]string) { p["receiveMode"] = "push" },
		"maxActiveMessages 0":                   func(p map[string]string) { p["maxActiveMessages"] = "0" },
		"maxActiveMessages over 1000":           func(p map[string]string) { p["maxActiveMessages"] = "1001" },
		"maxRetriableErrorsPerSec negative":     func(p map[string]string) { p["maxRetriableErrorsPerSec"] = "-1" },
		"unknown concurrencyMode":               func(p map[string]string) { p["concurrencyMode"] = "sometimes" },
		"lockRenewalInterval under 1s":          func(p map[string]string) { p["lockRenewalInterval"] = "500ms" },
		"lockRenewalInterval negative":          func(p map[string]string) { p["lockRenewalInterval"] = "-1s" },
		"lockRenewalInterval not under lockTTL": func(p map[string]string) { p["lockRenewalInterval"] = "30s" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			props := requiredProps()
			mutate(props)
			_, err := ParseMetadata(props, true)
			require.Error(t, err)
		})
	}
}

func TestParseMetadataConsumerIDIsOptionalWhenNotRequired(t *testing.T) {
	props := requiredProps()
	delete(props, "consumerID")

	_, err := ParseMetadata(props, false)

	require.NoError(t, err)
}

func TestParseSubscriptionInheritsTheComponentDefaults(t *testing.T) {
	md, err := ParseMetadata(requiredProps(), true)
	require.NoError(t, err)

	opts, err := ParseSubscription(md.SubscriptionOptions, nil)
	require.NoError(t, err)

	assert.Equal(t, md.SubscriptionOptions, opts)
}

func TestParseSubscriptionOverridesTheComponentDefaults(t *testing.T) {
	props := requiredProps()
	props["concurrencyMode"] = "single"
	props["lockTTL"] = "2m"
	md, err := ParseMetadata(props, true)
	require.NoError(t, err)

	opts, err := ParseSubscription(md.SubscriptionOptions, map[string]string{
		"allowCompetingConsumers":  "false",
		"maxActiveMessages":        "5",
		"receiveMode":              "poll",
		"maxRetriableErrorsPerSec": "0",
		"httpEndpoint":             "http://elsewhere", // component-only: ignored
		"rawPayload":               "true",             // not ours: ignored
	})
	require.NoError(t, err)

	assert.False(t, opts.AllowCompetingConsumers)
	assert.Equal(t, 5, opts.MaxActiveMessages)
	assert.Equal(t, ReceiveModePoll, opts.ReceiveMode)
	assert.Equal(t, 0, opts.MaxRetriableErrorsPerSec)
	// Not overridden: inherited.
	assert.Equal(t, pubsub.Single, opts.ConcurrencyMode)
	assert.Equal(t, 2*time.Minute, opts.LockTTL)
	assert.Equal(t, 10*time.Second, opts.LockRenewalInterval)
}

func TestParseSubscriptionRejectsInvalidValues(t *testing.T) {
	md, err := ParseMetadata(requiredProps(), true)
	require.NoError(t, err)

	cases := map[string]map[string]string{
		"lockTTL under 1s":                       {"lockTTL": "500ms"},
		"unknown concurrencyMode":                {"concurrencyMode": "sometimes"},
		"maxActiveMessages 0":                    {"maxActiveMessages": "0"},
		"lockTTL not over the inherited renewal": {"lockTTL": "10s"},
	}
	for name, props := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseSubscription(md.SubscriptionOptions, props)
			require.Error(t, err)
		})
	}
}
