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

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/Azure/azure-event-hubs-go/v3/persist"
	"github.com/dapr/components-contrib/pubsub"
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

	bulkSize       int
	bulk           []pubsub.BulkMessageEntry
	topic          string
	persistRecords []*persistRecord
	flushed        *persistRecord
}

// NewBulkReceiver creates an object that can be used as both a `persist.CheckpointPersister` and an Event Hubs Event Handler `batchWriter.HandleEvent`.
func NewBulkReceiver(persister persist.CheckpointPersister, bulkSize int, topic string, handler pubsub.BulkHandler) (*bulkReceiver, error) {
	return &bulkReceiver{
		persister:      persister,
		handler:        handler,
		bulkSize:       bulkSize,
		topic:          topic,
		bulk:           make([]pubsub.BulkMessageEntry, 0, bulkSize),
		persistRecords: make([]*persistRecord, 0, bulkSize),
	}, nil
}

// Read reads the last checkpoint.
func (r *bulkReceiver) Read(namespace, name, consumerGroup, partitionID string) (persist.Checkpoint, error) {
	return r.persister.Read(namespace, name, consumerGroup, partitionID)
}

// Write will write the last checkpoint of the last event flushed and record persist records for future use.
func (r *bulkReceiver) Write(namespace, name, consumerGroup, partitionID string, checkpoint persist.Checkpoint) error {
	var err error
	if r.flushed != nil {
		pr := r.flushed
		err = r.persister.Write(pr.namespace, pr.name, pr.consumerGroup, pr.partitionID, pr.checkpoint)
		if err != nil {
			r.flushed = nil
		}
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

// HandleEvent is a bulk event handler for the event hub receiver.
// If the length of the bulk buffer has reached the max bulkSize, the buffer will be flushed before appending the new event.
// If flush fails and it hasn't made space in the buffer, the flush error will be returned to the caller.
func (r *bulkReceiver) HandleEvent(ctx context.Context, event *eventhub.Event) error {
	if len(r.bulk) >= r.bulkSize {
		err := r.Flush(ctx)
		// If no events were flushed, return the error.
		if err != nil && len(r.bulk) >= r.bulkSize {
			return err
		}
	}
	// Append the event to the buffer if we have room for it.

	// Create a new pubsub BulkMessageEntry from the event.
	entryID, err := uuid.NewRandom()
	if err != nil {
		return err
	}

	// TODO: figure out metadata
	data := pubsub.BulkMessageEntry{
		EntryID:  entryID.String(),
		Event:    event.Data,
		Metadata: map[string]string{},
	}
	r.bulk = append(r.bulk, data)
	return nil
}

// Flush flushes the buffer to the given pubsub.BulkHandler.
// Post-condition:
//
//	error == nil: buffer has been flushed successfully, buffer has been replaced with a new buffer.
//	error != nil: some or no events have been flushed, buffer contains only events that failed to flush.
func (r *bulkReceiver) Flush(ctx context.Context) error {
	bulkMessage := pubsub.BulkMessage{
		Topic:    r.topic,
		Entries:  r.bulk,
		Metadata: map[string]string{},
	}
	res, err := r.handler(ctx, &bulkMessage)

	var offset int
	if err != nil {
		offset = r.getFailureOffset(ctx, res, err)
		r.bulk = r.bulk[offset:]
		r.persistRecords = r.persistRecords[offset:]
		return err
	}

	// r.flushed is used to track the last entry that was successfully processed.
	if offset > 0 {
		r.flushed = r.persistRecords[offset-1]
	}

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
	return len(r.bulk)
}
