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

type bulkReceiver struct {
	persister persist.CheckpointPersister

	bulkSize int
	bulk     []string
	topic    string
	handler  pubsub.BulkHandler
}

// NewBulkReceiver creates a new bulkReceiver instance.
func NewBulkReceiver(persister persist.CheckpointPersister, bulkSize int, topic string, handler pubsub.BulkHandler) (*bulkReceiver, error) {
	return &bulkReceiver{
		persister: persister,
		bulkSize:  bulkSize,
		bulk:      make([]string, 0, bulkSize),
		topic:     topic,
		handler:   handler,
	}, nil
}

// HandleEvent is an event handler for the event hub receiver.
func (r *bulkReceiver) HandleEvent(ctx context.Context, event *eventhub.Event) error {
	if len(r.bulk) >= r.bulkSize {
		err := r.flush(ctx)
		// If no events were flushed, return the error
		if err != nil && len(r.bulk) >= r.bulkSize {
			return err
		}
	}
	r.bulk = append(r.bulk, string(event.Data))
	return nil
}

// flush flushes the current buffer to the handler.
// If error is nil, buffer has been flushed successfully, buffer has been replaced with a new buffer.
// If error is not nil, buffer has been partially flushed, buffer contains events that failed to flush.
func (r *bulkReceiver) flush(ctx context.Context) error {
	entries := make([]pubsub.BulkMessageEntry, len(r.bulk))
	for i, s := range r.bulk {
		entryID, err := uuid.NewRandom()
		if err != nil {
			return err
		}

		// TODO: figure out metadata
		entries[i] = pubsub.BulkMessageEntry{
			EntryID:  entryID.String(),
			Event:    []byte(s),
			Metadata: map[string]string{},
		}
	}
	// TODO: figure out metadata
	bulkMessage := &pubsub.BulkMessage{
		Topic:    r.topic,
		Entries:  entries,
		Metadata: map[string]string{},
	}
	res, err := r.handler(ctx, bulkMessage)
	if err != nil {
		offset := r.handleError(ctx, res, err)
		r.bulk = r.bulk[offset:]
		return err
	}

	r.bulk = make([]string, 0, r.bulkSize)
	return nil
}

func (r *bulkReceiver) handleError(ctx context.Context, res []pubsub.BulkSubscribeResponseEntry, err error) int {
	if res == nil {
		// the entire bulk failed to be processed
		return 0
	}
	return 0
}
