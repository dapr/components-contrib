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

import "sync"

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

// bulkPublishSerial publishes messages in serial order.
// This is slower, but ensures that messages are published in the order as specified in the request.
func (p *DefaultBulkMessager) bulkPublishSerial(req *BulkPublishRequest) (BulkPublishResponse, error) {
	var statuses []BulkPublishResponseEntry

	for _, entry := range req.Entries {
		statuses = append(statuses, p.bulkPublishSingleEntry(req, entry))
	}

	return BulkPublishResponse{Statuses: statuses}, nil
}

// bulkPublishParallel publishes messages in parallel.
// This is faster, but does not guarantee that messages are published in the order as specified in the request.
func (p *DefaultBulkMessager) bulkPublishParallel(req *BulkPublishRequest) (BulkPublishResponse, error) {
	var statuses []BulkPublishResponseEntry
	var wg sync.WaitGroup

	for _, entry := range req.Entries {
		wg.Add(1)

		go func(entry BulkMessageEntry) {
			defer wg.Done()
			statuses = append(statuses, p.bulkPublishSingleEntry(req, entry))
		}(entry)
	}

	return BulkPublishResponse{Statuses: statuses}, nil
}
