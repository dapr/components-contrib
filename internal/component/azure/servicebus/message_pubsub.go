/*
Copyright 2021 The Dapr Authors
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

package servicebus

import (
	"encoding/base64"
	"net/http"
	"strconv"

	azservicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/google/uuid"

	"github.com/dapr/components-contrib/pubsub"
)

// NewPubsubMessageFromASBMessage returns a pubsub.NewMessage from a message received from ASB.
func NewPubsubMessageFromASBMessage(asbMsg *azservicebus.ReceivedMessage, topic string) (*pubsub.NewMessage, error) {
	pubsubMsg := &pubsub.NewMessage{
		Topic: topic,
		Data:  asbMsg.Body,
	}

	pubsubMsg.Metadata = addMessageAttributesToMetadata(pubsubMsg.Metadata, asbMsg)

	return pubsubMsg, nil
}

// NewBulkMessageEntryFromASBMessage returns a pubsub.NewMessageEntry from a bulk message received from ASB.
func NewBulkMessageEntryFromASBMessage(asbMsg *azservicebus.ReceivedMessage) (pubsub.BulkMessageEntry, error) {
	entryId, err := uuid.NewRandom() //nolint:stylecheck
	if err != nil {
		return pubsub.BulkMessageEntry{}, err
	}

	bulkMsgEntry := pubsub.BulkMessageEntry{
		EntryId: entryId.String(),
		Event:   asbMsg.Body,
	}

	bulkMsgEntry.Metadata = addMessageAttributesToMetadata(bulkMsgEntry.Metadata, asbMsg)

	return bulkMsgEntry, nil
}

func addMessageAttributesToMetadata(metadata map[string]string, asbMsg *azservicebus.ReceivedMessage) map[string]string {
	if metadata == nil {
		metadata = map[string]string{}
	}

	if asbMsg.MessageID != "" {
		metadata["metadata."+MessageKeyMessageID] = asbMsg.MessageID
	}
	if asbMsg.SessionID != nil {
		metadata["metadata."+MessageKeySessionID] = *asbMsg.SessionID
	}
	if asbMsg.CorrelationID != nil && *asbMsg.CorrelationID != "" {
		metadata["metadata."+MessageKeyCorrelationID] = *asbMsg.CorrelationID
	}
	if asbMsg.Subject != nil && *asbMsg.Subject != "" {
		metadata["metadata."+MessageKeyLabel] = *asbMsg.Subject
	}
	if asbMsg.ReplyTo != nil && *asbMsg.ReplyTo != "" {
		metadata["metadata."+MessageKeyReplyTo] = *asbMsg.ReplyTo
	}
	if asbMsg.To != nil && *asbMsg.To != "" {
		metadata["metadata."+MessageKeyTo] = *asbMsg.To
	}
	if asbMsg.ContentType != nil && *asbMsg.ContentType != "" {
		metadata["metadata."+MessageKeyContentType] = *asbMsg.ContentType
	}
	if asbMsg.LockToken != [16]byte{} {
		metadata["metadata."+MessageKeyLockToken] = base64.StdEncoding.EncodeToString(asbMsg.LockToken[:])
	}

	// Always set delivery count.
	metadata["metadata."+MessageKeyDeliveryCount] = strconv.FormatInt(int64(asbMsg.DeliveryCount), 10)

	if asbMsg.EnqueuedTime != nil {
		// Preserve RFC2616 time format.
		metadata["metadata."+MessageKeyEnqueuedTimeUtc] = asbMsg.EnqueuedTime.UTC().Format(http.TimeFormat)
	}
	if asbMsg.SequenceNumber != nil {
		metadata["metadata."+MessageKeySequenceNumber] = strconv.FormatInt(*asbMsg.SequenceNumber, 10)
	}
	if asbMsg.ScheduledEnqueueTime != nil {
		// Preserve RFC2616 time format.
		metadata["metadata."+MessageKeyScheduledEnqueueTimeUtc] = asbMsg.ScheduledEnqueueTime.UTC().Format(http.TimeFormat)
	}
	if asbMsg.PartitionKey != nil {
		metadata["metadata."+MessageKeyPartitionKey] = *asbMsg.PartitionKey
	}
	if asbMsg.LockedUntil != nil {
		// Preserve RFC2616 time format.
		metadata["metadata."+MessageKeyLockedUntilUtc] = asbMsg.LockedUntil.UTC().Format(http.TimeFormat)
	}

	return metadata
}

// UpdateASBBatchMessageWithBulkPublishRequest updates the batch message with messages from the bulk publish request.
func UpdateASBBatchMessageWithBulkPublishRequest(asbMsgBatch *azservicebus.MessageBatch, req *pubsub.BulkPublishRequest) error {
	// Add entries from bulk request to batch.
	for _, entry := range req.Entries {
		asbMsg, err := NewASBMessageFromBulkMessageEntry(entry)
		if err != nil {
			return err
		}

		err = asbMsgBatch.AddMessage(asbMsg, nil)
		if err != nil {
			return err
		}
	}

	return nil
}
