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

func TestNewBulkPublishResponse(t *testing.T) {
	messages := []BulkMessageEntry{
		{
			EntryId: "1",
			Event:   []byte("event 1"),
			Metadata: map[string]string{
				"ttlInSeconds": "22",
			},
			ContentType: "text/plain",
		},
		{
			EntryId: "2",
			Event:   []byte("event 2"),
			Metadata: map[string]string{
				"ttlInSeconds": "11",
			},
			ContentType: "text/plain",
		},
	}
	t.Run("populate success", func(t *testing.T) {
		res := NewBulkPublishResponse(messages, PublishSucceeded, nil)
		assert.NotEmpty(t, res, "expected res to be populated")
		assert.Equal(t, 2, len(res.Statuses), "expected two statuses")
		expectedRes := BulkPublishResponse{
			Statuses: []BulkPublishResponseEntry{
				{
					EntryId: "1",
					Status:  PublishSucceeded,
				},
				{
					EntryId: "2",
					Status:  PublishSucceeded,
				},
			},
		}
		assert.ElementsMatch(t, expectedRes.Statuses, res.Statuses, "expected output to match")
	})
	t.Run("populate failure", func(t *testing.T) {
		res := NewBulkPublishResponse(messages, PublishFailed, assert.AnError)
		assert.NotEmpty(t, res, "expected res to be populated")
		assert.Equal(t, 2, len(res.Statuses), "expected two statuses")
		expectedRes := BulkPublishResponse{
			Statuses: []BulkPublishResponseEntry{
				{
					EntryId: "1",
					Status:  PublishFailed,
					Error:   assert.AnError,
				},
				{
					EntryId: "2",
					Status:  PublishFailed,
					Error:   assert.AnError,
				},
			},
		}
		assert.ElementsMatch(t, expectedRes.Statuses, res.Statuses, "expected output to match")
	})
}
