/*
Copyright 2023 The Dapr Authors
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

package eventhubs

import (
	"context"
	"errors"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/kit/logger"
)

var testLogger = logger.NewLogger("test")

func TestParseEventHubsMetadata(t *testing.T) {
	t.Run("test valid connectionString configuration", func(t *testing.T) {
		metadata := map[string]string{"connectionString": "fake"}

		m, err := parseEventHubsMetadata(metadata, false, testLogger)

		require.NoError(t, err)
		assert.Equal(t, "fake", m.ConnectionString)
	})

	t.Run("test namespace given", func(t *testing.T) {
		metadata := map[string]string{"eventHubNamespace": "fake.servicebus.windows.net"}

		m, err := parseEventHubsMetadata(metadata, false, testLogger)

		require.NoError(t, err)
		assert.Equal(t, "fake.servicebus.windows.net", m.EventHubNamespace)
	})

	t.Run("test namespace adds FQDN", func(t *testing.T) {
		metadata := map[string]string{"eventHubNamespace": "fake"}

		m, err := parseEventHubsMetadata(metadata, false, testLogger)

		require.NoError(t, err)
		assert.Equal(t, "fake.servicebus.windows.net", m.EventHubNamespace)
	})

	t.Run("test both connectionString and eventHubNamespace given", func(t *testing.T) {
		metadata := map[string]string{
			"connectionString":  "fake",
			"eventHubNamespace": "fake",
		}

		_, err := parseEventHubsMetadata(metadata, false, testLogger)

		require.Error(t, err)
		require.ErrorContains(t, err, "only one of connectionString or eventHubNamespace should be passed")
	})

	t.Run("test missing metadata", func(t *testing.T) {
		metadata := map[string]string{}

		_, err := parseEventHubsMetadata(metadata, false, testLogger)

		require.Error(t, err)
		require.ErrorContains(t, err, "one of connectionString or eventHubNamespace is required")
	})

	t.Run("test in order delivery", func(t *testing.T) {
		metadata := map[string]string{
			"enableInOrderMessageDelivery": "true",
			"connectionString":             "fake",
		}

		m, err := parseEventHubsMetadata(metadata, false, testLogger)

		require.NoError(t, err)
		require.True(t, m.EnableInOrderMessageDelivery)
	})

	t.Run("test in order delivery enabled by default", func(t *testing.T) {
		m, err := parseEventHubsMetadata(map[string]string{"connectionString": "fake"}, false, testLogger)

		require.NoError(t, err)
		require.True(t, m.EnableInOrderMessageDelivery)
	})

	t.Run("test in order delivery can be disabled explicitly", func(t *testing.T) {
		m, err := parseEventHubsMetadata(map[string]string{
			"enableInOrderMessageDelivery": "false",
			"connectionString":             "fake",
		}, false, testLogger)

		require.NoError(t, err)
		require.False(t, m.EnableInOrderMessageDelivery)
	})
}

type fakeProcessorPartitionClient struct {
	batches         [][]*azeventhubs.ReceivedEventData
	receiveCalls    int
	checkpointCalls []*azeventhubs.ReceivedEventData
	closed          bool
	checkpointHook  func()
	receiveErr      error
}

func (f *fakeProcessorPartitionClient) ReceiveEvents(ctx context.Context, _ int, _ *azeventhubs.ReceiveEventsOptions) ([]*azeventhubs.ReceivedEventData, error) {
	if f.receiveErr != nil {
		f.receiveCalls++
		return nil, f.receiveErr
	}
	if f.receiveCalls >= len(f.batches) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	batch := f.batches[f.receiveCalls]
	f.receiveCalls++
	return batch, nil
}

func (f *fakeProcessorPartitionClient) UpdateCheckpoint(_ context.Context, event *azeventhubs.ReceivedEventData, _ *azeventhubs.UpdateCheckpointOptions) error {
	f.checkpointCalls = append(f.checkpointCalls, event)
	if f.checkpointHook != nil {
		f.checkpointHook()
	}
	return nil
}

func (f *fakeProcessorPartitionClient) PartitionID() string {
	return "0"
}

func (f *fakeProcessorPartitionClient) Close(context.Context) error {
	f.closed = true
	return nil
}

func TestProcessEventsDoesNotReceiveNextBatchAfterHandlerFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	messageIDA := "A"
	messageIDB := "B"
	eventA := &azeventhubs.ReceivedEventData{
		EventData:      azeventhubs.EventData{MessageID: &messageIDA},
		SequenceNumber: 1,
		Offset:         10,
	}
	eventB := &azeventhubs.ReceivedEventData{
		EventData:      azeventhubs.EventData{MessageID: &messageIDB},
		SequenceNumber: 2,
		Offset:         20,
	}
	client := &fakeProcessorPartitionClient{
		batches: [][]*azeventhubs.ReceivedEventData{{eventA}, {eventB}},
	}
	aeh := &AzureEventHubs{
		logger: testLogger,
		metadata: &AzureEventHubsMetadata{
			EnableInOrderMessageDelivery: true,
		},
	}
	handlerErr := errors.New("retry exhausted")
	var handled []int64

	err := aeh.processEvents(ctx, client, SubscribeConfig{
		Topic:                           "topic",
		MaxBulkSubCount:                 1,
		MaxBulkSubAwaitDurationMs:       100,
		CheckPointFrequencyPerPartition: 1,
		Handler: func(_ context.Context, events []*azeventhubs.ReceivedEventData) ([]HandlerResponseItem, error) {
			handled = append(handled, events[0].SequenceNumber)
			cancel()
			return nil, handlerErr
		},
	})

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []int64{1}, handled)
	assert.Equal(t, 1, client.receiveCalls, "must not receive B after A remains unresolved")
	assert.Empty(t, client.checkpointCalls, "must not checkpoint A or advance past it")
	assert.True(t, client.closed)
}

func TestProcessEventsExplicitAsyncModeDoesNotWaitForHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	handlerFinished := make(chan struct{})
	client := &fakeProcessorPartitionClient{
		batches: [][]*azeventhubs.ReceivedEventData{{receivedEvent("A", 1, 10)}},
		checkpointHook: func() {
			cancel()
		},
	}
	aeh := &AzureEventHubs{
		logger:   testLogger,
		metadata: &AzureEventHubsMetadata{EnableInOrderMessageDelivery: false},
	}

	err := aeh.processEvents(ctx, client, SubscribeConfig{
		Topic:                           "topic",
		MaxBulkSubCount:                 1,
		MaxBulkSubAwaitDurationMs:       100,
		CheckPointFrequencyPerPartition: 1,
		Handler: func(context.Context, []*azeventhubs.ReceivedEventData) ([]HandlerResponseItem, error) {
			close(handlerStarted)
			<-releaseHandler
			close(handlerFinished)
			return nil, nil
		},
	})

	require.ErrorIs(t, err, context.Canceled)
	<-handlerStarted
	require.Len(t, client.checkpointCalls, 1, "explicit async mode must retain legacy checkpoint behavior")
	assert.Equal(t, int64(1), client.checkpointCalls[0].SequenceNumber)
	assert.True(t, client.closed)

	close(releaseHandler)
	<-handlerFinished
}

func TestProcessEventsDoesNotReceiveNextBatchAfterBulkFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	eventA := receivedEvent("A", 1, 10)
	eventB := receivedEvent("B", 2, 20)
	eventC := receivedEvent("C", 3, 30)
	client := &fakeProcessorPartitionClient{
		batches: [][]*azeventhubs.ReceivedEventData{{eventA, eventB}, {eventC}},
	}
	aeh := &AzureEventHubs{
		logger:   testLogger,
		metadata: &AzureEventHubsMetadata{EnableInOrderMessageDelivery: true},
	}
	entryErr := errors.New("entry B failed")

	err := aeh.processEvents(ctx, client, SubscribeConfig{
		Topic:                           "topic",
		MaxBulkSubCount:                 2,
		MaxBulkSubAwaitDurationMs:       100,
		CheckPointFrequencyPerPartition: 1,
		Handler: func(context.Context, []*azeventhubs.ReceivedEventData) ([]HandlerResponseItem, error) {
			cancel()
			return []HandlerResponseItem{{EntryID: "A"}, {EntryID: "B", Error: entryErr}}, entryErr
		},
	})

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, client.receiveCalls, "must replay the whole batch instead of receiving C")
	assert.Empty(t, client.checkpointCalls, "must not checkpoint a partially failed batch")
	assert.True(t, client.closed)
}

func TestProcessEventsRetriesFailedBatchBeforeReceivingNextBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeProcessorPartitionClient{
		batches: [][]*azeventhubs.ReceivedEventData{
			{receivedEvent("A", 1, 10)},
			{receivedEvent("B", 2, 20)},
		},
	}
	client.checkpointHook = func() {
		if len(client.checkpointCalls) == 2 {
			cancel()
		}
	}
	aeh := &AzureEventHubs{
		logger:   testLogger,
		metadata: &AzureEventHubsMetadata{EnableInOrderMessageDelivery: true},
	}
	var handled []int64
	attempts := 0

	err := aeh.processEvents(ctx, client, SubscribeConfig{
		Topic:                           "topic",
		MaxBulkSubCount:                 1,
		MaxBulkSubAwaitDurationMs:       100,
		CheckPointFrequencyPerPartition: 1,
		Handler: func(_ context.Context, events []*azeventhubs.ReceivedEventData) ([]HandlerResponseItem, error) {
			handled = append(handled, events[0].SequenceNumber)
			attempts++
			if attempts == 1 {
				return nil, errors.New("A remains unresolved")
			}
			return nil, nil
		},
	})

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []int64{1, 1, 2}, handled)
	require.Len(t, client.checkpointCalls, 2)
	assert.Equal(t, int64(1), client.checkpointCalls[0].SequenceNumber)
	assert.Equal(t, int64(2), client.checkpointCalls[1].SequenceNumber)
	assert.True(t, client.closed)
}

func TestProcessEventsCheckpointsSuccessfulBatchesAtConfiguredFrequency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeProcessorPartitionClient{
		batches: [][]*azeventhubs.ReceivedEventData{
			{receivedEvent("A", 1, 10)},
			{receivedEvent("B", 2, 20)},
			{receivedEvent("C", 3, 30)},
		},
	}
	client.checkpointHook = func() {
		if len(client.checkpointCalls) == 2 {
			cancel()
		}
	}
	aeh := &AzureEventHubs{
		logger:   testLogger,
		metadata: &AzureEventHubsMetadata{EnableInOrderMessageDelivery: true},
	}

	err := aeh.processEvents(ctx, client, SubscribeConfig{
		Topic:                           "topic",
		MaxBulkSubCount:                 1,
		MaxBulkSubAwaitDurationMs:       100,
		CheckPointFrequencyPerPartition: 2,
		Handler: func(context.Context, []*azeventhubs.ReceivedEventData) ([]HandlerResponseItem, error) {
			return nil, nil
		},
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, client.checkpointCalls, 2)
	assert.Equal(t, int64(1), client.checkpointCalls[0].SequenceNumber)
	assert.Equal(t, int64(3), client.checkpointCalls[1].SequenceNumber)
	assert.True(t, client.closed)
}

func TestProcessEventsDoesNotCheckpointAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeProcessorPartitionClient{
		batches: [][]*azeventhubs.ReceivedEventData{{receivedEvent("A", 1, 10)}},
	}
	aeh := &AzureEventHubs{
		logger:   testLogger,
		metadata: &AzureEventHubsMetadata{EnableInOrderMessageDelivery: true},
	}

	err := aeh.processEvents(ctx, client, SubscribeConfig{
		Topic:                           "topic",
		MaxBulkSubCount:                 1,
		MaxBulkSubAwaitDurationMs:       100,
		CheckPointFrequencyPerPartition: 1,
		Handler: func(context.Context, []*azeventhubs.ReceivedEventData) ([]HandlerResponseItem, error) {
			cancel()
			return nil, nil
		},
	})

	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, client.checkpointCalls)
	assert.True(t, client.closed)
}

func TestProcessEventsDoesNotCheckpointAfterOwnershipLoss(t *testing.T) {
	client := &fakeProcessorPartitionClient{
		receiveErr: &azeventhubs.Error{Code: azeventhubs.ErrorCodeOwnershipLost},
	}
	aeh := &AzureEventHubs{
		logger:   testLogger,
		metadata: &AzureEventHubsMetadata{EnableInOrderMessageDelivery: true},
	}

	err := aeh.processEvents(context.Background(), client, SubscribeConfig{
		Topic:                           "topic",
		MaxBulkSubCount:                 1,
		MaxBulkSubAwaitDurationMs:       100,
		CheckPointFrequencyPerPartition: 1,
		Handler: func(context.Context, []*azeventhubs.ReceivedEventData) ([]HandlerResponseItem, error) {
			require.FailNow(t, "handler must not run after ownership loss")
			return nil, nil
		},
	})

	require.NoError(t, err)
	assert.Equal(t, 1, client.receiveCalls)
	assert.Empty(t, client.checkpointCalls)
	assert.True(t, client.closed)
}

func TestProcessorOptions(t *testing.T) {
	t.Run("ordered delivery starts at earliest when no checkpoint exists", func(t *testing.T) {
		aeh := &AzureEventHubs{
			metadata: &AzureEventHubsMetadata{EnableInOrderMessageDelivery: true},
		}

		options := aeh.processorOptions()

		require.NotNil(t, options)
		require.NotNil(t, options.StartPositions.Default.Earliest)
		assert.True(t, *options.StartPositions.Default.Earliest)
	})

	t.Run("explicit async mode preserves SDK default latest position", func(t *testing.T) {
		aeh := &AzureEventHubs{
			metadata: &AzureEventHubsMetadata{EnableInOrderMessageDelivery: false},
		}

		assert.Nil(t, aeh.processorOptions())
	})
}

func receivedEvent(messageID string, sequenceNumber, offset int64) *azeventhubs.ReceivedEventData {
	return &azeventhubs.ReceivedEventData{
		EventData:      azeventhubs.EventData{MessageID: &messageID},
		SequenceNumber: sequenceNumber,
		Offset:         offset,
	}
}

func TestConstructConnectionStringFromTopic(t *testing.T) {
	t.Run("valid connectionString without hub name", func(t *testing.T) {
		connectionString := "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=fakeKey;SharedAccessKey=key"
		topic := "testHub"

		metadata := map[string]string{
			"connectionString": connectionString,
		}
		aeh := &AzureEventHubs{logger: testLogger}
		err := aeh.Init(metadata)
		require.NoError(t, err)

		c, err := aeh.constructConnectionStringFromTopic(topic)
		require.NoError(t, err)
		assert.Equal(t, connectionString+";EntityPath=testHub", c)
	})

	t.Run("valid connectionString with hub name", func(t *testing.T) {
		connectionString := "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=fakeKey;SharedAccessKey=key;EntityPath=testHub"
		topic := "testHub"

		metadata := map[string]string{
			"connectionString": connectionString,
		}
		aeh := &AzureEventHubs{logger: testLogger}
		err := aeh.Init(metadata)
		require.NoError(t, err)

		c, err := aeh.constructConnectionStringFromTopic(topic)
		require.NoError(t, err)
		assert.Equal(t, connectionString, c)
	})

	t.Run("valid connectionString with different hub name", func(t *testing.T) {
		connectionString := "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=fakeKey;SharedAccessKey=key;EntityPath=testHub"
		topic := "differentHub"

		metadata := map[string]string{
			"connectionString": connectionString,
		}
		aeh := &AzureEventHubs{logger: testLogger}
		err := aeh.Init(metadata)
		require.NoError(t, err)

		c, err := aeh.constructConnectionStringFromTopic(topic)
		require.Error(t, err)
		require.ErrorContains(t, err, "does not match the Event Hub name in the connection string")
		assert.Empty(t, c)
	})
}
