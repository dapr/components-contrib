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

package pubsub

import (
	"context"
	"sync"
	"time"
)

const (
	bulkPublishKeepOrderKey            string = "bulkPublishKeepOrder"
	bulkSubscribeMaxCountKey           string = "bulkSubscribeMaxCount"
	bulkSubscribeMaxAwaitDurationMsKey string = "bulkSubscribeMaxAwaitDurationMs"

	defaultMaxBulkCount           int = 100
	defaultMaxBulkAwaitDurationMs int = 5 * 1000
)

// bulkPublishSerial publishes messages in serial order.
// This is slower, but ensures that messages are published in the same order as specified in the request.
func (p *DefaultBulkMessager) bulkPublishSerial(req *BulkPublishRequest) (BulkPublishResponse, error) {
	statuses := make([]BulkPublishResponseEntry, 0, len(req.Entries))

	for _, entry := range req.Entries {
		statuses = append(statuses, p.bulkPublishSingleEntry(req, entry))
	}

	return BulkPublishResponse{Statuses: statuses}, nil
}

// bulkPublishParallel publishes messages in parallel.
// This is faster, but does not guarantee that messages are published in the same order as specified in the request.
func (p *DefaultBulkMessager) bulkPublishParallel(req *BulkPublishRequest) (BulkPublishResponse, error) {
	statuses := make([]BulkPublishResponseEntry, 0, len(req.Entries))
	var wg sync.WaitGroup

	statusChan := make(chan BulkPublishResponseEntry, len(req.Entries))

	for _, entry := range req.Entries {
		wg.Add(1)

		go func(entry BulkMessageEntry) {
			defer wg.Done()
			statusChan <- p.bulkPublishSingleEntry(req, entry)
		}(entry)
	}

	wg.Wait()
	close(statusChan)

	for status := range statusChan {
		statuses = append(statuses, status)
	}

	return BulkPublishResponse{Statuses: statuses}, nil
}

// bulkPublishSingleEntry publishes a single message from the bulk request.
func (p *DefaultBulkMessager) bulkPublishSingleEntry(req *BulkPublishRequest, entry BulkMessageEntry) BulkPublishResponseEntry {
	pr := PublishRequest{
		Data:        entry.Event,
		PubsubName:  req.PubsubName,
		Topic:       req.Topic,
		Metadata:    entry.Metadata,
		ContentType: &entry.ContentType,
	}

	if err := p.p.Publish(&pr); err != nil {
		return BulkPublishResponseEntry{
			EntryID: entry.EntryID,
			Status:  PublishFailed,
			Error:   err,
		}
	}

	return BulkPublishResponseEntry{
		EntryID: entry.EntryID,
		Status:  PublishSucceeded,
	}
}

// msgWithCallback is a wrapper around a message that includes a callback function
// that is called when the message is processed.
type msgWithCallback struct {
	msg BulkMessageEntry
	cb  func(error)
}

// flushMessages writes messages to a BulkHandler and clears the messages slice.
func flushMessages(ctx context.Context, req SubscribeRequest, messages *[]BulkMessageEntry, msgCbMap *map[string]func(error), handler BulkHandler) {
	if len(*messages) > 0 {
		// TODO: should we log errors?
		responses, err := handler(ctx, &BulkMessage{
			Topic:    req.Topic,
			Metadata: map[string]string{},
			Entries:  *messages,
		})

		if err != nil {
			if responses != nil {
				// invoke callbacks for each message
				for _, r := range responses {
					if cb, ok := (*msgCbMap)[r.EntryID]; ok {
						cb(r.Error)
					}
				}
			} else {
				// all messages failed
				for _, cb := range *msgCbMap {
					cb(err)
				}
			}
		} else {
			// no error has occurred
			for _, cb := range *msgCbMap {
				cb(nil)
			}
		}

		*messages = []BulkMessageEntry{}
		*msgCbMap = make(map[string]func(error))
	}
}

// processBulkMessages reads messages from msgChan and publishes them to a BulkHandler.
// It buffers messages in memory and publishes them in bulk.
func processBulkMessages(ctx context.Context, req SubscribeRequest, msgCbChan <-chan msgWithCallback, cfg BulkSubscribeConfig, handler BulkHandler) {
	var messages []BulkMessageEntry
	msgCbMap := make(map[string]func(error))

	ticker := time.NewTicker(time.Duration(cfg.MaxBulkAwaitDurationMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			flushMessages(ctx, req, &messages, &msgCbMap, handler)
			return
		case msgCb := <-msgCbChan:
			messages = append(messages, msgCb.msg)
			msgCbMap[msgCb.msg.EntryID] = msgCb.cb
			if len(messages) >= cfg.MaxBulkCount {
				flushMessages(ctx, req, &messages, &msgCbMap, handler)
			}
		case <-ticker.C:
			flushMessages(ctx, req, &messages, &msgCbMap, handler)
		}
	}
}
