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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
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

	t.Run("test concurrent delivery enabled by default", func(t *testing.T) {
		m, err := parseEventHubsMetadata(map[string]string{"connectionString": "fake"}, false, testLogger)

		require.NoError(t, err)
		require.False(t, m.EnableInOrderMessageDelivery)
		require.Equal(t, DefaultMaxConcurrentHandlers, m.MaxConcurrentHandlers)
	})

	t.Run("test max concurrent handlers", func(t *testing.T) {
		m, err := parseEventHubsMetadata(map[string]string{
			"maxConcurrentHandlers": "7",
			"connectionString":      "fake",
		}, false, testLogger)

		require.NoError(t, err)
		require.Equal(t, 7, m.MaxConcurrentHandlers)
	})

	t.Run("test max concurrent handlers must be positive", func(t *testing.T) {
		_, err := parseEventHubsMetadata(map[string]string{
			"maxConcurrentHandlers": "0",
			"connectionString":      "fake",
		}, false, testLogger)

		require.ErrorContains(t, err, "maxConcurrentHandlers must be greater than 0")
	})
}

type fakeProcessorPartitionClient struct {
	mu              sync.Mutex
	batches         [][]*azeventhubs.ReceivedEventData
	receiveCalls    int
	checkpointCalls []*azeventhubs.ReceivedEventData
	closed          bool
	checkpointHook  func()
	checkpointErr   error
	receiveErr      error
}

func (f *fakeProcessorPartitionClient) ReceiveEvents(ctx context.Context, _ int, _ *azeventhubs.ReceiveEventsOptions) ([]*azeventhubs.ReceivedEventData, error) {
	f.mu.Lock()
	if f.receiveErr != nil {
		f.receiveCalls++
		f.mu.Unlock()
		return nil, f.receiveErr
	}
	if f.receiveCalls >= len(f.batches) {
		f.mu.Unlock()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	batch := f.batches[f.receiveCalls]
	f.receiveCalls++
	f.mu.Unlock()
	return batch, nil
}

func (f *fakeProcessorPartitionClient) UpdateCheckpoint(_ context.Context, event *azeventhubs.ReceivedEventData, _ *azeventhubs.UpdateCheckpointOptions) error {
	f.mu.Lock()
	f.checkpointCalls = append(f.checkpointCalls, event)
	f.mu.Unlock()
	if f.checkpointHook != nil {
		f.checkpointHook()
	}
	return f.checkpointErr
}

func (f *fakeProcessorPartitionClient) PartitionID() string {
	return "0"
}

func (f *fakeProcessorPartitionClient) Close(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeProcessorPartitionClient) checkpointSequences() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	sequences := make([]int64, len(f.checkpointCalls))
	for i, event := range f.checkpointCalls {
		sequences[i] = event.SequenceNumber
	}
	return sequences
}

func (f *fakeProcessorPartitionClient) receivedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.receiveCalls
}

func (f *fakeProcessorPartitionClient) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
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
	assert.Equal(t, 1, client.receivedCount(), "must not receive B after A remains unresolved")
	assert.Empty(t, client.checkpointSequences(), "must not checkpoint A or advance past it")
	assert.True(t, client.isClosed())
}

func TestProcessEventsConcurrentModeWaitsForContiguousSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	releaseFirst := make(chan struct{})
	laterCompleted := make(chan struct{}, 2)
	client := &fakeProcessorPartitionClient{
		batches: [][]*azeventhubs.ReceivedEventData{
			{receivedEvent("A", 1, 10)},
			{receivedEvent("B", 2, 20)},
			{receivedEvent("C", 3, 30)},
		},
	}
	client.checkpointHook = func() {
		if len(client.checkpointSequences()) == 1 {
			cancel()
		}
	}
	aeh := &AzureEventHubs{
		logger: testLogger,
		metadata: &AzureEventHubsMetadata{
			EnableInOrderMessageDelivery: false,
			MaxConcurrentHandlers:        3,
		},
	}

	result := make(chan error, 1)
	go func() {
		result <- aeh.processEvents(ctx, client, SubscribeConfig{
			Topic:                           "topic",
			MaxBulkSubCount:                 1,
			MaxBulkSubAwaitDurationMs:       100,
			CheckPointFrequencyPerPartition: 1,
			Handler: func(_ context.Context, events []*azeventhubs.ReceivedEventData) ([]HandlerResponseItem, error) {
				if events[0].SequenceNumber == 1 {
					<-releaseFirst
				} else {
					laterCompleted <- struct{}{}
				}
				return nil, nil
			},
		})
	}()

	<-laterCompleted
	<-laterCompleted
	assert.Empty(t, client.checkpointSequences(), "later successes must not checkpoint past unresolved A")
	close(releaseFirst)
	require.ErrorIs(t, <-result, context.Canceled)
	assert.Equal(t, []int64{3}, client.checkpointSequences())
	assert.True(t, client.isClosed())
}

func TestProcessEventsConcurrentModeStopsReceivingAtCapacity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	releases := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	started := make(chan int64, 3)
	client := &fakeProcessorPartitionClient{
		batches: [][]*azeventhubs.ReceivedEventData{
			{receivedEvent("A", 1, 10)},
			{receivedEvent("B", 2, 20)},
			{receivedEvent("C", 3, 30)},
		},
	}
	aeh := &AzureEventHubs{
		logger: testLogger,
		metadata: &AzureEventHubsMetadata{
			MaxConcurrentHandlers: 2,
		},
	}
	result := make(chan error, 1)
	go func() {
		result <- aeh.processEvents(ctx, client, SubscribeConfig{
			Topic:                           "topic",
			MaxBulkSubCount:                 1,
			MaxBulkSubAwaitDurationMs:       100,
			CheckPointFrequencyPerPartition: 1,
			Handler: func(_ context.Context, events []*azeventhubs.ReceivedEventData) ([]HandlerResponseItem, error) {
				sequence := events[0].SequenceNumber
				started <- sequence
				<-releases[sequence-1]
				return nil, nil
			},
		})
	}()

	<-started
	<-started
	assert.Equal(t, 2, client.receivedCount())
	close(releases[1])
	select {
	case sequence := <-started:
		require.Failf(t, "received over capacity", "started handler for sequence %d while A pins the checkpoint", sequence)
	case <-time.After(50 * time.Millisecond):
	}
	assert.Equal(t, 2, client.receivedCount())
	close(releases[0])
	assert.Equal(t, int64(3), <-started)
	cancel()
	close(releases[2])
	require.ErrorIs(t, <-result, context.Canceled)
	assert.Equal(t, []int64{2}, client.checkpointSequences())
}

func TestProcessEventsConcurrentCheckpointWritesAreSerialized(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var activeWrites atomic.Int32
	var maxActiveWrites atomic.Int32
	checkpointStarted := make(chan struct{})
	releaseCheckpoint := make(chan struct{})
	secondHandlerReturned := make(chan struct{})
	var checkpointStartedOnce sync.Once
	client := &fakeProcessorPartitionClient{
		batches: [][]*azeventhubs.ReceivedEventData{
			{receivedEvent("A", 1, 10)},
			{receivedEvent("B", 2, 20)},
		},
	}
	client.checkpointHook = func() {
		active := activeWrites.Add(1)
		for {
			max := maxActiveWrites.Load()
			if active <= max || maxActiveWrites.CompareAndSwap(max, active) {
				break
			}
		}
		checkpointStartedOnce.Do(func() { close(checkpointStarted) })
		<-releaseCheckpoint
		if activeWrites.Add(-1) == 0 && len(client.checkpointSequences()) == 2 {
			cancel()
		}
	}
	aeh := &AzureEventHubs{
		logger: testLogger,
		metadata: &AzureEventHubsMetadata{
			MaxConcurrentHandlers: 2,
		},
	}

	result := make(chan error, 1)
	go func() {
		result <- aeh.processEvents(ctx, client, SubscribeConfig{
			Topic:                           "topic",
			MaxBulkSubCount:                 1,
			MaxBulkSubAwaitDurationMs:       100,
			CheckPointFrequencyPerPartition: 1,
			Handler: func(_ context.Context, events []*azeventhubs.ReceivedEventData) ([]HandlerResponseItem, error) {
				if events[0].SequenceNumber == 2 {
					<-checkpointStarted
					close(secondHandlerReturned)
				}
				return nil, nil
			},
		})
	}()

	<-checkpointStarted
	<-secondHandlerReturned
	close(releaseCheckpoint)
	require.ErrorIs(t, <-result, context.Canceled)
	assert.Equal(t, int32(1), maxActiveWrites.Load())
	assert.Equal(t, []int64{1, 2}, client.checkpointSequences())
}

func TestProcessEventsConcurrentModeReturnsCheckpointError(t *testing.T) {
	checkpointErr := errors.New("checkpoint unavailable")
	client := &fakeProcessorPartitionClient{
		batches:       [][]*azeventhubs.ReceivedEventData{{receivedEvent("A", 1, 10)}},
		checkpointErr: checkpointErr,
	}
	aeh := &AzureEventHubs{
		logger: testLogger,
		metadata: &AzureEventHubsMetadata{
			MaxConcurrentHandlers: 1,
		},
	}

	err := aeh.processEvents(context.Background(), client, SubscribeConfig{
		Topic:                           "topic",
		MaxBulkSubCount:                 1,
		MaxBulkSubAwaitDurationMs:       100,
		CheckPointFrequencyPerPartition: 1,
		Handler: func(context.Context, []*azeventhubs.ReceivedEventData) ([]HandlerResponseItem, error) {
			return nil, nil
		},
	})

	require.ErrorIs(t, err, checkpointErr)
	assert.True(t, client.isClosed())
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
	assert.Equal(t, 1, client.receivedCount(), "must replay the whole batch instead of receiving C")
	assert.Empty(t, client.checkpointSequences(), "must not checkpoint a partially failed batch")
	assert.True(t, client.isClosed())
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
		if len(client.checkpointSequences()) == 2 {
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
	assert.Equal(t, []int64{1, 2}, client.checkpointSequences())
	assert.True(t, client.isClosed())
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
		if len(client.checkpointSequences()) == 2 {
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
	assert.Equal(t, []int64{1, 3}, client.checkpointSequences())
	assert.True(t, client.isClosed())
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
	assert.Empty(t, client.checkpointSequences())
	assert.True(t, client.isClosed())
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
	assert.Equal(t, 1, client.receivedCount())
	assert.Empty(t, client.checkpointSequences())
	assert.True(t, client.isClosed())
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

	t.Run("concurrent delivery starts at earliest when no checkpoint exists", func(t *testing.T) {
		aeh := &AzureEventHubs{
			metadata: &AzureEventHubsMetadata{EnableInOrderMessageDelivery: false},
		}

		options := aeh.processorOptions()

		require.NotNil(t, options)
		require.NotNil(t, options.StartPositions.Default.Earliest)
		assert.True(t, *options.StartPositions.Default.Earliest)
	})
}

func TestHandleWithRetryRetriesPartialBulkFailure(t *testing.T) {
	aeh := &AzureEventHubs{
		logger:        testLogger,
		backOffConfig: retryConfigWithoutDelay(),
	}
	attempts := 0

	err := aeh.handleWithRetry(context.Background(), "topic", []*azeventhubs.ReceivedEventData{
		receivedEvent("A", 1, 10),
		receivedEvent("B", 2, 20),
	}, func(context.Context, []*azeventhubs.ReceivedEventData) ([]HandlerResponseItem, error) {
		attempts++
		if attempts == 1 {
			return []HandlerResponseItem{
				{EntryID: "A"},
				{EntryID: "B", Error: errors.New("B failed")},
			}, nil
		}
		return []HandlerResponseItem{{EntryID: "A"}, {EntryID: "B"}}, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 2, attempts)
}

func retryConfigWithoutDelay() retry.Config {
	config := retry.DefaultConfig()
	config.Duration = 0
	return config
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
