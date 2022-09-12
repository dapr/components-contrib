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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBulkPublish_DefaultBulkMessager(t *testing.T) {
	bulkMessager := &DefaultBulkMessager{
		p: &MockPubSub{},
	}

	t.Run("publishes all messages in a request", func(t *testing.T) {
		req := &BulkPublishRequest{
			Entries: []BulkMessageEntry{
				{
					EntryID:     "78a48b5c-ff5a-4275-9bef-4a3bb8eefc3b",
					Event:       []byte("event1"),
					ContentType: "application/octet-stream",
					Metadata:    map[string]string{},
				},
				{
					EntryID:     "d64669e2-fab6-4452-a933-8de44e26ca02",
					Event:       []byte("event2"),
					ContentType: "application/octet-stream",
					Metadata:    map[string]string{},
				},
			},
			PubsubName: "pubsub",
			Topic:      "topic",
			Metadata:   map[string]string{},
		}
		res, err := bulkMessager.BulkPublish(req)

		assert.NoError(t, err)
		assert.Len(t, res.Statuses, len(req.Entries))

		var responseEntryIds []string
		for _, status := range res.Statuses {
			responseEntryIds = append(responseEntryIds, status.EntryID)
		}

		for _, entry := range req.Entries {
			assert.Contains(t, responseEntryIds, entry.EntryID)
		}
	})
}
