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

package eventhubs

import (
	"context"
	"fmt"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/Azure/azure-event-hubs-go/v3/persist"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/google/uuid"
)

type persistRecord struct {
	namespace     string
	name          string
	consumerGroup string
	partitionID   string
	checkpoint    persist.Checkpoint
}

type bulkReceiver struct {
	persister persist.CheckpointPersister
	handler   pubsub.BulkHandler
	logger    logger.Logger

	bulkEntries        chan pubsub.BulkMessageEntry
	bulkSize           int
	bulk               []pubsub.BulkMessageEntry
	topic              string
	persistRecords     []*persistRecord
	flushed            *persistRecord
	maxAwaitDuration   time.Duration
	flushHandlerStopCh chan struct{}
}

// NewBulkReceiver creates an object that can be used as both a `persist.CheckpointPersister` and an Event Hubs Event Handler `batchWriter.HandleEvent`.
func NewBulkReceiver(persister persist.CheckpointPersister, bulkSize int, topic string, maxAwaitDuration time.Duration, handler pubsub.BulkHandler, logger logger.Logger) (*bulkReceiver, error) {
	r := &bulkReceiver{
		persister:          persister,
		handler:            handler,
		logger:             logger,
		bulkEntries:        make(chan pubsub.BulkMessageEntry, bulkSize),
		bulkSize:           bulkSize,
		topic:              topic,
		bulk:               make([]pubsub.BulkMessageEntry, 0, bulkSize),
		persistRecords:     make([]*persistRecord, 0, bulkSize),
		maxAwaitDuration:   maxAwaitDuration,
		flushHandlerStopCh: make(chan struct{}),
	}

	go r.startFlushHandler()

	return r, nil
}

// Read reads the last checkpoint.
func (r *bulkReceiver) Read(namespace, name, consumerGroup, partitionID string) (persist.Checkpoint, error) {
	r.logger.Debugf("Reading checkpoint for %s/%s/%s/%s", namespace, name, consumerGroup, partitionID)
	checkpoint, err := r.persister.Read(namespace, name, consumerGroup, partitionID)
	r.logger.Debugf("Read checkpoint %+v", checkpoint)
	return checkpoint, err
}

// Write will write the last checkpoint of the last event flushed and record persist records for future use.
func (r *bulkReceiver) Write(namespace, name, consumerGroup, partitionID string, checkpoint persist.Checkpoint) error {
	r.logger.Debugf("Writing checkpoint for %s/%s/%s/%s", namespace, name, consumerGroup, partitionID)
	var err error
	if r.flushed != nil {
		pr := r.flushed
		err = r.persister.Write(pr.namespace, pr.name, pr.consumerGroup, pr.partitionID, pr.checkpoint)
		if err != nil {
			r.flushed = nil
		}
		r.logger.Debugf("Wrote checkpoint %+v, error: %v", pr.checkpoint, err)
	}
	r.persistRecords = append(r.persistRecords, &persistRecord{
		namespace:     namespace,
		name:          name,
		consumerGroup: consumerGroup,
		partitionID:   partitionID,
		checkpoint:    checkpoint,
	})
	return err
}

// HandleEvent is a event handler for the event hub receiver.
// It will add the event to the bulk messages channel.
func (r *bulkReceiver) HandleEvent(ctx context.Context, event *eventhub.Event) error {
	r.logger.Debugf("Handling event, buffer size: %d", len(r.bulk))
	// Create a new pubsub BulkMessageEntry from the event.
	entryID, err := uuid.NewRandom()
	if err != nil {
		return err
	}

	// TODO: figure out metadata
	entry := pubsub.BulkMessageEntry{
		EntryID:  entryID.String(),
		Event:    event.Data,
		Metadata: map[string]string{},
	}

	r.bulkEntries <- entry
	return nil
}

// Close stops the flush handler.
func (r *bulkReceiver) Close() {
	r.flushHandlerStopCh <- struct{}{}
}

// startFlushHandler will flush the bulk entries channel to the bulk handler
// whenever there are enough entries in the channel, or when maxAwaitDuration has passed.
// This should be run in a goroutine.
func (r *bulkReceiver) startFlushHandler() {
	ticker := time.NewTicker(r.maxAwaitDuration)
	for {
		select {
		case entry := <-r.bulkEntries:
			r.bulk = append(r.bulk, entry)
			if len(r.bulk) == r.bulkSize {
				err := r.flush(context.TODO())
				if err != nil {
					r.logger.Warnf("Error flushing bulk messages: %v", err)
				}
			}
		case <-ticker.C:
			if len(r.bulk) > 0 {
				err := r.flush(context.TODO())
				if err != nil {
					r.logger.Warnf("Error flushing bulk messages: %v", err)
				}
			}
		case <-r.flushHandlerStopCh:
			close(r.bulkEntries)
			return
		}
	}
}

// flush flushes the buffer to the given pubsub.BulkHandler.
// Post-condition:
//
//	error == nil: buffer has been flushed successfully, buffer has been replaced with a new buffer.
//	error != nil: some or no events have been flushed, buffer contains only events that failed to flush.
func (r *bulkReceiver) flush(ctx context.Context) error {
	bulkMessage := pubsub.BulkMessage{
		Topic:    r.topic,
		Entries:  r.bulk,
		Metadata: map[string]string{},
	}
	res, err := r.handler(ctx, &bulkMessage)
	r.logger.Debugf("Invoked handler, error: %v, result: %+v", err, res)

	var offset int
	if err != nil {
		offset = r.getFailureOffset(ctx, res, err)
		r.logger.Debugf("Failure offset: %d", offset)
		if offset < 0 {
			return fmt.Errorf("failed to flush buffer: %w", err)
		}

		// Remove the successful events from the buffers.
		r.bulk = r.bulk[offset:]
		r.persistRecords = r.persistRecords[offset:]

		// Record the last successful persist record.
		r.updateFlushed(offset - 1)

		return err
	}

	r.logger.Debugf("Flushed everything, resetting buffer, offset: %d", offset)
	r.logger.Debugf("persistRecords before reset: %+v", r.persistRecords)

	// Record the last successful persist record.
	r.updateFlushed(offset - 1)

	// Reset the buffers.
	r.bulk = make([]pubsub.BulkMessageEntry, 0, r.bulkSize)
	r.persistRecords = make([]*persistRecord, 0, r.bulkSize)

	return nil
}

// getFailureOffset returns the offset of the first event that failed to flush.
func (r *bulkReceiver) getFailureOffset(ctx context.Context, entries []pubsub.BulkSubscribeResponseEntry, err error) int {
	if entries == nil {
		// No events were processed successfully.
		return 0
	}

	// Find the first event that failed to process.
	for i, entry := range entries {
		if entry.Error != nil {
			// The event at index i failed to process.
			return i
		}
	}

	// This should never be called, but if it is, return the length of the bulk buffer.
	r.logger.Debugf("getFailureOffset called with no errors, returning len(r.bulk) = %d", len(r.bulk))
	return len(r.bulk)
}

// updateFlushed updates r.flushed to an element from r.persistRecords at index i.
func (r *bulkReceiver) updateFlushed(i int) {
	if i >= 0 && i < len(r.persistRecords) {
		r.flushed = r.persistRecords[i]
	}
}
