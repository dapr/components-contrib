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

package natsstreaming

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
)

func TestParseNATSStreamingForMetadataMandatoryOptionsMissing(t *testing.T) {
	type test struct {
		name       string
		properties map[string]string
	}
	tests := []test{
		{"nats URL missing", map[string]string{
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
		}},
		{"consumer ID missing", map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			subscriptionType:       "topic",
		}},
		{"cluster ID missing", map[string]string{
			natsURL:          "nats://foo.bar:4222",
			consumerID:       "consumer1",
			subscriptionType: "topic",
		}},
	}
	for _, _test := range tests {
		t.Run(_test.name, func(t *testing.T) {
			fakeMetaData := pubsub.Metadata{
				Properties: _test.properties,
			}
			_, err := parseNATSStreamingMetadata(fakeMetaData)
			assert.NotEmpty(t, err)
		})
	}
}

func TestParseNATSStreamingMetadataForInvalidSubscriptionOptions(t *testing.T) {
	type test struct {
		name       string
		properties map[string]string
	}

	tests := []test{
		{"invalid value (less than 1) for startAtSequence", map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			startAtSequence:        "0",
		}},
		{"non integer value for startAtSequence", map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			startAtSequence:        "foo",
		}},
		{"startWithLastReceived is other than true", map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			startWithLastReceived:  "foo",
		}},
		{"deliverAll is other than true", map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			deliverAll:             "foo",
		}},
		{"deliverNew is other than true", map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			deliverNew:             "foo",
		}},
		{"invalid value for startAtTimeDelta", map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			startAtTimeDelta:       "foo",
		}},
		{"startAtTime provided without startAtTimeFormat", map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			startAtTime:            "foo",
		}},
	}

	for _, _test := range tests {
		t.Run(_test.name, func(t *testing.T) {
			fakeMetaData := pubsub.Metadata{
				Properties: _test.properties,
			}
			_, err := parseNATSStreamingMetadata(fakeMetaData)
			assert.NotEmpty(t, err)
		})
	}
}

func TestParseNATSStreamingMetadataForValidSubscriptionOptions(t *testing.T) {
	type test struct {
		name                  string
		properties            map[string]string
		expectedMetadataName  string
		expectedMetadataValue string
	}

	tests := []test{

		{
			"using startWithLastReceived",
			map[string]string{
				natsURL:                "nats://foo.bar:4222",
				natsStreamingClusterID: "testcluster",
				consumerID:             "consumer1",
				subscriptionType:       "topic",
				startWithLastReceived:  "true",
			},
			"startWithLastReceived", "true",
		},

		{
			"using deliverAll",
			map[string]string{
				natsURL:                "nats://foo.bar:4222",
				natsStreamingClusterID: "testcluster",
				consumerID:             "consumer1",
				subscriptionType:       "topic",
				deliverAll:             "true",
			},
			"deliverAll", "true",
		},

		{
			"using deliverNew",
			map[string]string{
				natsURL:                "nats://foo.bar:4222",
				natsStreamingClusterID: "testcluster",
				consumerID:             "consumer1",
				subscriptionType:       "topic",
				deliverNew:             "true",
			},
			"deliverNew", "true",
		},

		{
			"using startAtSequence",
			map[string]string{
				natsURL:                "nats://foo.bar:4222",
				natsStreamingClusterID: "testcluster",
				consumerID:             "consumer1",
				subscriptionType:       "topic",
				startAtSequence:        "42",
			},
			"startAtSequence", "42",
		},

		{
			"using startAtTimeDelta",
			map[string]string{
				natsURL:                "nats://foo.bar:4222",
				natsStreamingClusterID: "testcluster",
				consumerID:             "consumer1",
				subscriptionType:       "topic",
				startAtTimeDelta:       "1h",
			},
			"startAtTimeDelta", "1h",
		},
	}

	for _, _test := range tests {
		t.Run(_test.name, func(t *testing.T) {
			fakeMetaData := pubsub.Metadata{
				Properties: _test.properties,
			}
			m, err := parseNATSStreamingMetadata(fakeMetaData)

			assert.NoError(t, err)

			assert.NotEmpty(t, m.natsURL)
			assert.NotEmpty(t, m.natsStreamingClusterID)
			assert.NotEmpty(t, m.subscriptionType)
			assert.NotEmpty(t, m.natsQueueGroupName)
			assert.NotEmpty(t, _test.expectedMetadataValue)

			assert.Equal(t, _test.properties[natsURL], m.natsURL)
			assert.Equal(t, _test.properties[natsStreamingClusterID], m.natsStreamingClusterID)
			assert.Equal(t, _test.properties[subscriptionType], m.subscriptionType)
			assert.Equal(t, _test.properties[consumerID], m.natsQueueGroupName)
			assert.Equal(t, _test.properties[_test.expectedMetadataName], _test.expectedMetadataValue)
		})
	}
}

func TestParseNATSStreamingMetadata(t *testing.T) {
	t.Run("mandatory metadata provided", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
		}
		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		m, err := parseNATSStreamingMetadata(fakeMetaData)

		assert.NoError(t, err)
		assert.NotEmpty(t, m.natsURL)
		assert.NotEmpty(t, m.natsStreamingClusterID)
		assert.NotEmpty(t, m.natsQueueGroupName)
		assert.Equal(t, fakeProperties[natsURL], m.natsURL)
		assert.Equal(t, fakeProperties[natsStreamingClusterID], m.natsStreamingClusterID)
		assert.Equal(t, fakeProperties[consumerID], m.natsQueueGroupName)
	})

	t.Run("subscription type missing", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
		}
		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		_, err := parseNATSStreamingMetadata(fakeMetaData)
		assert.Empty(t, err)
	})
	t.Run("invalid value for subscription type", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "baz",
		}
		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		_, err := parseNATSStreamingMetadata(fakeMetaData)
		assert.NotEmpty(t, err)
	})
	t.Run("more than one subscription option provided", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			startAtSequence:        "42",
			startWithLastReceived:  "true",
			deliverAll:             "true",
		}
		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		m, err := parseNATSStreamingMetadata(fakeMetaData)
		assert.NoError(t, err)
		assert.NotEmpty(t, m.natsURL)
		assert.NotEmpty(t, m.natsStreamingClusterID)
		assert.NotEmpty(t, m.subscriptionType)
		assert.NotEmpty(t, m.natsQueueGroupName)
		assert.NotEmpty(t, m.startAtSequence)
		// startWithLastReceived ignored
		assert.Empty(t, m.startWithLastReceived)
		// deliverAll will be ignored
		assert.Empty(t, m.deliverAll)

		assert.Equal(t, fakeProperties[natsURL], m.natsURL)
		assert.Equal(t, fakeProperties[natsStreamingClusterID], m.natsStreamingClusterID)
		assert.Equal(t, fakeProperties[subscriptionType], m.subscriptionType)
		assert.Equal(t, fakeProperties[consumerID], m.natsQueueGroupName)
		assert.Equal(t, fakeProperties[startAtSequence], strconv.FormatUint(m.startAtSequence, 10))
	})
}

func TestSubscriptionOptionsForValidOptions(t *testing.T) {
	type test struct {
		name                    string
		m                       metadata
		expectedNumberOfOptions int
	}

	tests := []test{
		{"using durableSubscriptionName", metadata{durableSubscriptionName: "foobar"}, 2},
		{"durableSubscriptionName is empty", metadata{durableSubscriptionName: ""}, 1},
		{"using startAtSequence", metadata{startAtSequence: uint64(42)}, 2},
		{"using startWithLastReceived", metadata{startWithLastReceived: startWithLastReceivedTrue}, 2},
		{"using deliverAll", metadata{deliverAll: deliverAllTrue}, 2},
		{"using startAtTimeDelta", metadata{startAtTimeDelta: 1 * time.Hour}, 2},
		{"using startAtTime and startAtTimeFormat", metadata{startAtTime: "Feb 3, 2013 at 7:54pm (PST)", startAtTimeFormat: "Jan 2, 2006 at 3:04pm (MST)"}, 2},
		{"using manual ack with ackWaitTime", metadata{ackWaitTime: 30 * time.Second}, 2},
		{"using manual ack with maxInFlight", metadata{maxInFlight: uint64(42)}, 2},
	}

	for _, _test := range tests {
		t.Run(_test.name, func(t *testing.T) {
			natsStreaming := natsStreamingPubSub{metadata: _test.m}
			opts, err := natsStreaming.subscriptionOptions()
			assert.Empty(t, err)
			assert.NotEmpty(t, opts)
			assert.Equal(t, _test.expectedNumberOfOptions, len(opts))
		})
	}
}

func TestSubscriptionOptionsForInvalidOptions(t *testing.T) {
	type test struct {
		name string
		m    metadata
	}

	tests := []test{
		{"startAtSequence is less than 1", metadata{startAtSequence: uint64(0)}},
		{"startWithLastReceived is other than true", metadata{startWithLastReceived: "foo"}},
		{"deliverAll is other than true", metadata{deliverAll: "foo"}},
		{"deliverNew is other than true", metadata{deliverNew: "foo"}},
		{"startAtTime is empty", metadata{startAtTime: "", startAtTimeFormat: "Jan 2, 2006 at 3:04pm (MST)"}},
		{"startAtTimeFormat is empty", metadata{startAtTime: "Feb 3, 2013 at 7:54pm (PST)", startAtTimeFormat: ""}},
	}

	for _, _test := range tests {
		t.Run(_test.name, func(t *testing.T) {
			natsStreaming := natsStreamingPubSub{metadata: _test.m}
			opts, err := natsStreaming.subscriptionOptions()
			assert.Empty(t, err)
			assert.NotEmpty(t, opts)
			assert.Equal(t, 1, len(opts))
		})
	}
}

func TestSubscriptionOptions(t *testing.T) {
	// general
	t.Run("manual ACK option is present by default", func(t *testing.T) {
		natsStreaming := natsStreamingPubSub{metadata: metadata{}}
		opts, err := natsStreaming.subscriptionOptions()
		assert.Empty(t, err)
		assert.NotEmpty(t, opts)
		assert.Equal(t, 1, len(opts))
	})

	t.Run("only one subscription option will be honored", func(t *testing.T) {
		m := metadata{deliverNew: deliverNewTrue, deliverAll: deliverAllTrue, startAtTimeDelta: 1 * time.Hour}
		natsStreaming := natsStreamingPubSub{metadata: m}
		opts, err := natsStreaming.subscriptionOptions()
		assert.Empty(t, err)
		assert.NotEmpty(t, opts)
		assert.Equal(t, 2, len(opts))
	})

	// invalid subscription options

	t.Run("startAtTime is invalid", func(t *testing.T) {
		m := metadata{startAtTime: "foobar", startAtTimeFormat: "Jan 2, 2006 at 3:04pm (MST)"}
		natsStreaming := natsStreamingPubSub{metadata: m}
		opts, err := natsStreaming.subscriptionOptions()
		assert.NotEmpty(t, err)
		assert.Nil(t, opts)
	})

	t.Run("startAtTimeFormat is invalid", func(t *testing.T) {
		m := metadata{startAtTime: "Feb 3, 2013 at 7:54pm (PST)", startAtTimeFormat: "foo"}

		natsStreaming := natsStreamingPubSub{metadata: m}
		opts, err := natsStreaming.subscriptionOptions()
		assert.NotEmpty(t, err)
		assert.Nil(t, opts)
	})
}

func TestGenRandomString(t *testing.T) {
	t.Run("random client ID is not empty", func(t *testing.T) {
		clientID := genRandomString(20)
		assert.NotEmpty(t, clientID)
	})

	t.Run("random client ID is not nil", func(t *testing.T) {
		clientID := genRandomString(20)
		assert.NotNil(t, clientID)
	})

	t.Run("random client ID length is 20", func(t *testing.T) {
		clientID := genRandomString(20)
		assert.NotEmpty(t, clientID)
		assert.NotNil(t, clientID)
		assert.Equal(t, 20, len(clientID))
	})
}
