// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package natsstreaming

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
)

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
	t.Run("nats URL missing", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
		}
		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		_, err := parseNATSStreamingMetadata(fakeMetaData)
		assert.NotEmpty(t, err)
	})
	t.Run("consumer ID missing", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			subscriptionType:       "topic",
		}
		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		_, err := parseNATSStreamingMetadata(fakeMetaData)
		assert.NotEmpty(t, err)
	})
	t.Run("cluster ID missing", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:          "nats://foo.bar:4222",
			consumerID:       "consumer1",
			subscriptionType: "topic",
		}
		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		_, err := parseNATSStreamingMetadata(fakeMetaData)
		assert.NotEmpty(t, err)
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
	//invalid cases for subscription options
	t.Run("invalid value (less than 1) for startAtSequence", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			startAtSequence:        "0",
		}
		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		_, err := parseNATSStreamingMetadata(fakeMetaData)
		assert.NotEmpty(t, err)
	})

	t.Run("non integer value for startAtSequence", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			startAtSequence:        "foo",
		}
		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		_, err := parseNATSStreamingMetadata(fakeMetaData)
		assert.NotEmpty(t, err)
	})
	t.Run("startWithLastReceived is other than true", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			startWithLastReceived:  "foo",
		}
		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		_, err := parseNATSStreamingMetadata(fakeMetaData)
		assert.NotEmpty(t, err)
	})
	t.Run("deliverAll is other than true", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			deliverAll:             "foo",
		}
		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		_, err := parseNATSStreamingMetadata(fakeMetaData)
		assert.NotEmpty(t, err)
	})
	t.Run("deliverNew is other than true", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			deliverNew:             "foo",
		}
		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		_, err := parseNATSStreamingMetadata(fakeMetaData)
		assert.NotEmpty(t, err)
	})
	t.Run("invalid value for startAtTimeDelta", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			startAtTimeDelta:       "foo",
		}
		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		_, err := parseNATSStreamingMetadata(fakeMetaData)
		assert.NotEmpty(t, err)
	})

	t.Run("startAtTime provided without startAtTimeFormat", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			startAtTime:            "foo",
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
		//startWithLastReceived ignore
		assert.Empty(t, m.startWithLastReceived)
		//deliverAll will be ignored
		assert.Empty(t, m.deliverAll)

		assert.Equal(t, fakeProperties[natsURL], m.natsURL)
		assert.Equal(t, fakeProperties[natsStreamingClusterID], m.natsStreamingClusterID)
		assert.Equal(t, fakeProperties[subscriptionType], m.subscriptionType)
		assert.Equal(t, fakeProperties[consumerID], m.natsQueueGroupName)
		assert.Equal(t, fakeProperties[startAtSequence], strconv.FormatUint(m.startAtSequence, 10))
	})

	//valid cases for subscription options
	t.Run("using startAtSequence", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			startAtSequence:        "42",
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

		assert.Equal(t, fakeProperties[natsURL], m.natsURL)
		assert.Equal(t, fakeProperties[natsStreamingClusterID], m.natsStreamingClusterID)
		assert.Equal(t, fakeProperties[subscriptionType], m.subscriptionType)
		assert.Equal(t, fakeProperties[consumerID], m.natsQueueGroupName)
		assert.Equal(t, fakeProperties[startAtSequence], strconv.FormatUint(m.startAtSequence, 10))
	})
	t.Run("using startWithLastReceived", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			startWithLastReceived:  "true",
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
		assert.NotEmpty(t, m.startWithLastReceived)

		assert.Equal(t, fakeProperties[natsURL], m.natsURL)
		assert.Equal(t, fakeProperties[natsStreamingClusterID], m.natsStreamingClusterID)
		assert.Equal(t, fakeProperties[subscriptionType], m.subscriptionType)
		assert.Equal(t, fakeProperties[consumerID], m.natsQueueGroupName)
		assert.Equal(t, fakeProperties[startWithLastReceived], m.startWithLastReceived)
	})
	t.Run("using deliverAll", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
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
		assert.NotEmpty(t, m.deliverAll)

		assert.Equal(t, fakeProperties[natsURL], m.natsURL)
		assert.Equal(t, fakeProperties[natsStreamingClusterID], m.natsStreamingClusterID)
		assert.Equal(t, fakeProperties[subscriptionType], m.subscriptionType)
		assert.Equal(t, fakeProperties[consumerID], m.natsQueueGroupName)
		assert.Equal(t, fakeProperties[deliverAll], m.deliverAll)
	})
	t.Run("using deliverNew", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			deliverNew:             "true",
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
		assert.NotEmpty(t, m.deliverNew)

		assert.Equal(t, fakeProperties[natsURL], m.natsURL)
		assert.Equal(t, fakeProperties[natsStreamingClusterID], m.natsStreamingClusterID)
		assert.Equal(t, fakeProperties[subscriptionType], m.subscriptionType)
		assert.Equal(t, fakeProperties[consumerID], m.natsQueueGroupName)
		assert.Equal(t, fakeProperties[deliverNew], m.deliverNew)
	})

	t.Run("using startAtTimeDelta", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:                "nats://foo.bar:4222",
			natsStreamingClusterID: "testcluster",
			consumerID:             "consumer1",
			subscriptionType:       "topic",
			startAtTimeDelta:       "1h",
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
		assert.NotEmpty(t, m.startAtTimeDelta)

		assert.Equal(t, fakeProperties[natsURL], m.natsURL)
		assert.Equal(t, fakeProperties[natsStreamingClusterID], m.natsStreamingClusterID)
		assert.Equal(t, fakeProperties[subscriptionType], m.subscriptionType)
		assert.Equal(t, fakeProperties[consumerID], m.natsQueueGroupName)
		dur, _ := time.ParseDuration(fakeProperties[startAtTimeDelta])
		assert.Equal(t, m.startAtTimeDelta, dur)
	})
	//t.Run("using startAtTime and startAtTimeFormat", func(t *testing.T) {})
}

func TestSubscriptionOptions(t *testing.T) {
	//valid cases for options
	t.Run("using durableSubscriptionName", func(t *testing.T) {
		m := metadata{durableSubscriptionName: "foobar"}
		natsStreaming := natsStreamingPubSub{metadata: m}
		opts, err := natsStreaming.subscriptionOptions()
		assert.Empty(t, err)
		assert.NotEmpty(t, opts)
		assert.Equal(t, 2, len(opts))
	})
	t.Run("durableSubscriptionName is empty", func(t *testing.T) {
		m := metadata{durableSubscriptionName: ""}
		natsStreaming := natsStreamingPubSub{metadata: m}
		opts, err := natsStreaming.subscriptionOptions()
		assert.Empty(t, err)
		assert.NotEmpty(t, opts)
		assert.Equal(t, 1, len(opts))
	})
	t.Run("using startAtSequence", func(t *testing.T) {
		m := metadata{startAtSequence: uint64(42)}
		natsStreaming := natsStreamingPubSub{metadata: m}
		opts, err := natsStreaming.subscriptionOptions()
		assert.Empty(t, err)
		assert.NotEmpty(t, opts)
		assert.Equal(t, 2, len(opts))
	})
	t.Run("using startWithLastReceived", func(t *testing.T) {
		m := metadata{startWithLastReceived: startWithLastReceivedTrue}
		natsStreaming := natsStreamingPubSub{metadata: m}
		opts, err := natsStreaming.subscriptionOptions()
		assert.Empty(t, err)
		assert.NotEmpty(t, opts)
		assert.Equal(t, 2, len(opts))
	})
	t.Run("using deliverAll", func(t *testing.T) {
		m := metadata{deliverAll: deliverAllTrue}
		natsStreaming := natsStreamingPubSub{metadata: m}
		opts, err := natsStreaming.subscriptionOptions()
		assert.Empty(t, err)
		assert.NotEmpty(t, opts)
		assert.Equal(t, 2, len(opts))
	})
	t.Run("using startAtTimeDelta", func(t *testing.T) {
		m := metadata{startAtTimeDelta: 1 * time.Hour}
		natsStreaming := natsStreamingPubSub{metadata: m}
		opts, err := natsStreaming.subscriptionOptions()
		assert.Empty(t, err)
		assert.NotEmpty(t, opts)
		assert.Equal(t, 2, len(opts))
	})
	t.Run("using startAtTime and startAtTimeFormat", func(t *testing.T) {
		m := metadata{startAtTime: "Feb 3, 2013 at 7:54pm (PST)", startAtTimeFormat: "Jan 2, 2006 at 3:04pm (MST)"}
		natsStreaming := natsStreamingPubSub{metadata: m}
		opts, err := natsStreaming.subscriptionOptions()
		assert.Empty(t, err)
		assert.NotEmpty(t, opts)
		assert.Equal(t, 2, len(opts))
	})

	//general
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

	//invalid subscription options
	t.Run("startAtSequence is less than 1", func(t *testing.T) {
		m := metadata{startAtSequence: uint64(0)}
		natsStreaming := natsStreamingPubSub{metadata: m}
		opts, err := natsStreaming.subscriptionOptions()
		assert.Empty(t, err)
		assert.NotEmpty(t, opts)
		assert.Equal(t, 1, len(opts))
	})
	t.Run("startWithLastReceived is other than true", func(t *testing.T) {
		m := metadata{startWithLastReceived: "foo"}
		natsStreaming := natsStreamingPubSub{metadata: m}
		opts, err := natsStreaming.subscriptionOptions()
		assert.Empty(t, err)
		assert.NotEmpty(t, opts)
		assert.Equal(t, 1, len(opts))
	})
	t.Run("deliverAll is other than true", func(t *testing.T) {
		m := metadata{deliverAll: "foo"}

		natsStreaming := natsStreamingPubSub{metadata: m}
		opts, err := natsStreaming.subscriptionOptions()
		assert.Empty(t, err)
		assert.NotEmpty(t, opts)
		assert.Equal(t, 1, len(opts))
	})
	t.Run("deliverNew is other than true", func(t *testing.T) {
		m := metadata{deliverNew: "foo"}

		natsStreaming := natsStreamingPubSub{metadata: m}
		opts, err := natsStreaming.subscriptionOptions()
		assert.Empty(t, err)
		assert.NotEmpty(t, opts)
		assert.Equal(t, 1, len(opts))
	})
	t.Run("startAtTime is empty", func(t *testing.T) {
		m := metadata{startAtTime: "", startAtTimeFormat: "Jan 2, 2006 at 3:04pm (MST)"}
		natsStreaming := natsStreamingPubSub{metadata: m}
		opts, err := natsStreaming.subscriptionOptions()
		assert.Empty(t, err)
		assert.NotEmpty(t, opts)
		assert.Equal(t, 1, len(opts))
	})
	t.Run("startAtTime is invalid", func(t *testing.T) {
		m := metadata{startAtTime: "foobar", startAtTimeFormat: "Jan 2, 2006 at 3:04pm (MST)"}
		natsStreaming := natsStreamingPubSub{metadata: m}
		opts, err := natsStreaming.subscriptionOptions()
		assert.NotEmpty(t, err)
		assert.Nil(t, opts)
	})
	t.Run("startAtTimeFormat is empty", func(t *testing.T) {
		m := metadata{startAtTime: "Feb 3, 2013 at 7:54pm (PST)", startAtTimeFormat: ""}

		natsStreaming := natsStreamingPubSub{metadata: m}
		opts, err := natsStreaming.subscriptionOptions()
		assert.Empty(t, err)
		assert.NotEmpty(t, opts)
		assert.Equal(t, 1, len(opts))
	})
	t.Run("startAtTimeFormat is invalid", func(t *testing.T) {
		m := metadata{startAtTime: "Feb 3, 2013 at 7:54pm (PST)", startAtTimeFormat: "foo"}

		natsStreaming := natsStreamingPubSub{metadata: m}
		opts, err := natsStreaming.subscriptionOptions()
		assert.NotEmpty(t, err)
		assert.Nil(t, opts)
	})
}
