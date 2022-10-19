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

	impl "github.com/dapr/components-contrib/internal/component/azure/servicebus"
	"github.com/dapr/components-contrib/pubsub"
)

func NewPubsubMessageFromASBMessage(asbMsg *azservicebus.ReceivedMessage, topic string) (*pubsub.NewMessage, error) {
	pubsubMsg := &pubsub.NewMessage{
		Topic: topic,
		Data:  asbMsg.Body,
	}

	pubsubMsg.Metadata = addMessageAttributesToMetadata(pubsubMsg.Metadata, asbMsg)

	return pubsubMsg, nil
}

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

	addToMetadata := func(metadata map[string]string, key, value string) {
		metadata["metadata."+key] = value
	}

	if asbMsg.MessageID != "" {
		addToMetadata(metadata, impl.MessageKeyMessageID, asbMsg.MessageID)
	}
	if asbMsg.SessionID != nil {
		addToMetadata(metadata, impl.MessageKeySessionID, *asbMsg.SessionID)
	}
	if asbMsg.CorrelationID != nil && *asbMsg.CorrelationID != "" {
		addToMetadata(metadata, impl.MessageKeyCorrelationID, *asbMsg.CorrelationID)
	}
	if asbMsg.Subject != nil && *asbMsg.Subject != "" {
		addToMetadata(metadata, impl.MessageKeyLabel, *asbMsg.Subject)
	}
	if asbMsg.ReplyTo != nil && *asbMsg.ReplyTo != "" {
		addToMetadata(metadata, impl.MessageKeyReplyTo, *asbMsg.ReplyTo)
	}
	if asbMsg.To != nil && *asbMsg.To != "" {
		addToMetadata(metadata, impl.MessageKeyTo, *asbMsg.To)
	}
	if asbMsg.ContentType != nil && *asbMsg.ContentType != "" {
		addToMetadata(metadata, impl.MessageKeyContentType, *asbMsg.ContentType)
	}
	if asbMsg.LockToken != [16]byte{} {
		addToMetadata(metadata, impl.MessageKeyLockToken, base64.StdEncoding.EncodeToString(asbMsg.LockToken[:]))
	}

	// Always set delivery count.
	addToMetadata(metadata, impl.MessageKeyDeliveryCount, strconv.FormatInt(int64(asbMsg.DeliveryCount), 10))

	if asbMsg.EnqueuedTime != nil {
		// Preserve RFC2616 time format.
		addToMetadata(metadata, impl.MessageKeyEnqueuedTimeUtc, asbMsg.EnqueuedTime.UTC().Format(http.TimeFormat))
	}
	if asbMsg.SequenceNumber != nil {
		addToMetadata(metadata, impl.MessageKeySequenceNumber, strconv.FormatInt(*asbMsg.SequenceNumber, 10))
	}
	if asbMsg.ScheduledEnqueueTime != nil {
		// Preserve RFC2616 time format.
		addToMetadata(metadata, impl.MessageKeyScheduledEnqueueTimeUtc, asbMsg.ScheduledEnqueueTime.UTC().Format(http.TimeFormat))
	}
	if asbMsg.PartitionKey != nil {
		addToMetadata(metadata, impl.MessageKeyPartitionKey, *asbMsg.PartitionKey)
	}
	if asbMsg.LockedUntil != nil {
		// Preserve RFC2616 time format.
		addToMetadata(metadata, impl.MessageKeyLockedUntilUtc, asbMsg.LockedUntil.UTC().Format(http.TimeFormat))
	}

	return metadata
}

// UpdateASBBatchMessageWithBulkPublishRequest updates the batch message with messages from the bulk publish request.
func UpdateASBBatchMessageWithBulkPublishRequest(asbMsgBatch *azservicebus.MessageBatch, req *pubsub.BulkPublishRequest) error {
	// Add entries from bulk request to batch.
	for _, entry := range req.Entries {
		asbMsg, err := impl.NewASBMessageFromBulkMessageEntry(entry)
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
