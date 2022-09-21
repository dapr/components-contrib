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
	"fmt"
	"net/http"
	"strconv"
	"time"

	azservicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
)

const (
	// MessageIDMetadataKey defines the metadata key for the message id.
	MessageIDMetadataKey = "MessageId" // read, write.

	// CorrelationIDMetadataKey defines the metadata key for the correlation id.
	CorrelationIDMetadataKey = "CorrelationId" // read, write.

	// SessionIDMetadataKey defines the metadata key for the session id.
	SessionIDMetadataKey = "SessionId" // read, write.

	// LabelMetadataKey defines the metadata key for the label.
	LabelMetadataKey = "Label" // read, write.

	// ReplyToMetadataKey defines the metadata key for the reply to value.
	ReplyToMetadataKey = "ReplyTo" // read, write.

	// ToMetadataKey defines the metadata key for the to value.
	ToMetadataKey = "To" // read, write.

	// PartitionKeyMetadataKey defines the metadata key for the partition key.
	PartitionKeyMetadataKey = "PartitionKey" // read, write.

	// ContentTypeMetadataKey defines the metadata key for the content type.
	ContentTypeMetadataKey = "ContentType" // read, write.

	// DeliveryCountMetadataKey defines the metadata key for the delivery count.
	DeliveryCountMetadataKey = "DeliveryCount" // read.

	// LockedUntilUtcMetadataKey defines the metadata key for the locked until utc value.
	LockedUntilUtcMetadataKey = "LockedUntilUtc" // read.

	// LockTokenMetadataKey defines the metadata key for the lock token.
	LockTokenMetadataKey = "LockToken" // read.

	// EnqueuedTimeUtcMetadataKey defines the metadata key for the enqueued time utc value.
	EnqueuedTimeUtcMetadataKey = "EnqueuedTimeUtc" // read.

	// SequenceNumberMetadataKey defines the metadata key for the sequence number.
	SequenceNumberMetadataKey = "SequenceNumber" // read.

	// ScheduledEnqueueTimeUtcMetadataKey defines the metadata key for the scheduled enqueue time utc value.
	ScheduledEnqueueTimeUtcMetadataKey = "ScheduledEnqueueTimeUtc" // read, write.

	// ReplyToSessionID defines the metadata key for the reply to session id.
	ReplyToSessionID = "ReplyToSessionId" // read, write.
)

func NewPubsubMessageFromASBMessage(asbMsg *azservicebus.ReceivedMessage, topic string) (*pubsub.NewMessage, error) {
	pubsubMsg := &pubsub.NewMessage{
		Topic: topic,
	}

	pubsubMsg.Data = asbMsg.Body

	addToMetadata := func(msg *pubsub.NewMessage, key, value string) {
		if msg.Metadata == nil {
			msg.Metadata = make(map[string]string)
		}

		msg.Metadata[fmt.Sprintf("metadata.%s", key)] = value
	}

	if asbMsg.MessageID != "" {
		addToMetadata(pubsubMsg, MessageIDMetadataKey, asbMsg.MessageID)
	}
	if asbMsg.SessionID != nil {
		addToMetadata(pubsubMsg, SessionIDMetadataKey, *asbMsg.SessionID)
	}
	if asbMsg.CorrelationID != nil && *asbMsg.CorrelationID != "" {
		addToMetadata(pubsubMsg, CorrelationIDMetadataKey, *asbMsg.CorrelationID)
	}
	if asbMsg.Subject != nil && *asbMsg.Subject != "" {
		addToMetadata(pubsubMsg, LabelMetadataKey, *asbMsg.Subject)
	}
	if asbMsg.ReplyTo != nil && *asbMsg.ReplyTo != "" {
		addToMetadata(pubsubMsg, ReplyToMetadataKey, *asbMsg.ReplyTo)
	}
	if asbMsg.To != nil && *asbMsg.To != "" {
		addToMetadata(pubsubMsg, ToMetadataKey, *asbMsg.To)
	}
	if asbMsg.ContentType != nil && *asbMsg.ContentType != "" {
		addToMetadata(pubsubMsg, ContentTypeMetadataKey, *asbMsg.ContentType)
	}
	if asbMsg.LockToken != [16]byte{} {
		addToMetadata(pubsubMsg, LockTokenMetadataKey, base64.StdEncoding.EncodeToString(asbMsg.LockToken[:]))
	}

	// Always set delivery count.
	addToMetadata(pubsubMsg, DeliveryCountMetadataKey, strconv.FormatInt(int64(asbMsg.DeliveryCount), 10))

	if asbMsg.EnqueuedTime != nil {
		// Preserve RFC2616 time format.
		addToMetadata(pubsubMsg, EnqueuedTimeUtcMetadataKey, asbMsg.EnqueuedTime.UTC().Format(http.TimeFormat))
	}
	if asbMsg.SequenceNumber != nil {
		addToMetadata(pubsubMsg, SequenceNumberMetadataKey, strconv.FormatInt(*asbMsg.SequenceNumber, 10))
	}
	if asbMsg.ScheduledEnqueueTime != nil {
		// Preserve RFC2616 time format.
		addToMetadata(pubsubMsg, ScheduledEnqueueTimeUtcMetadataKey, asbMsg.ScheduledEnqueueTime.UTC().Format(http.TimeFormat))
	}
	if asbMsg.PartitionKey != nil {
		addToMetadata(pubsubMsg, PartitionKeyMetadataKey, *asbMsg.PartitionKey)
	}
	if asbMsg.LockedUntil != nil {
		// Preserve RFC2616 time format.
		addToMetadata(pubsubMsg, LockedUntilUtcMetadataKey, asbMsg.LockedUntil.UTC().Format(http.TimeFormat))
	}

	return pubsubMsg, nil
}

// NewASBMessageFromPubsubRequest builds a new Azure Service Bus message from a PublishRequest.
func NewASBMessageFromPubsubRequest(req *pubsub.PublishRequest) (*azservicebus.Message, error) {
	asbMsg := &azservicebus.Message{
		Body: req.Data,
	}

	err := addMetadataToMessage(asbMsg, req.Metadata)
	return asbMsg, err
}

// NewASBMessageFromBulkMessageEntry builds a new Azure Service Bus message from a BulkMessageEntry.
func NewASBMessageFromBulkMessageEntry(entry pubsub.BulkMessageEntry) (*azservicebus.Message, error) {
	asbMsg := &azservicebus.Message{
		Body:        entry.Event,
		ContentType: &entry.ContentType,
	}

	err := addMetadataToMessage(asbMsg, entry.Metadata)
	return asbMsg, err
}

func addMetadataToMessage(asbMsg *azservicebus.Message, metadata map[string]string) error {
	// Common properties.
	ttl, ok, _ := contribMetadata.TryGetTTL(metadata)
	if ok {
		asbMsg.TimeToLive = &ttl
	}

	// Azure Service Bus specific properties.
	// reference: https://docs.microsoft.com/en-us/rest/api/servicebus/message-headers-and-properties#message-headers
	msgID, ok, _ := tryGetString(metadata, MessageIDMetadataKey)
	if ok {
		asbMsg.MessageID = &msgID
	}

	correlationID, ok, _ := tryGetString(metadata, CorrelationIDMetadataKey)
	if ok {
		asbMsg.CorrelationID = &correlationID
	}

	sessionID, okSessionID, _ := tryGetString(metadata, SessionIDMetadataKey)
	if okSessionID {
		asbMsg.SessionID = &sessionID
	}

	label, ok, _ := tryGetString(metadata, LabelMetadataKey)
	if ok {
		asbMsg.Subject = &label
	}

	replyTo, ok, _ := tryGetString(metadata, ReplyToMetadataKey)
	if ok {
		asbMsg.ReplyTo = &replyTo
	}

	to, ok, _ := tryGetString(metadata, ToMetadataKey)
	if ok {
		asbMsg.To = &to
	}

	partitionKey, ok, _ := tryGetString(metadata, PartitionKeyMetadataKey)
	if ok {
		if okSessionID && partitionKey != sessionID {
			return fmt.Errorf("session id %s and partition key %s should be equal when both present", sessionID, partitionKey)
		}

		asbMsg.PartitionKey = &partitionKey
	}

	contentType, ok, _ := tryGetString(metadata, ContentTypeMetadataKey)
	if ok {
		asbMsg.ContentType = &contentType
	}

	scheduledEnqueueTime, ok, _ := tryGetScheduledEnqueueTime(metadata)
	if ok {
		asbMsg.ScheduledEnqueueTime = scheduledEnqueueTime
	}

	return nil
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

func tryGetString(props map[string]string, key string) (string, bool, error) {
	if val, ok := props[key]; ok && val != "" {
		return val, true, nil
	}

	return "", false, nil
}

func tryGetScheduledEnqueueTime(props map[string]string) (*time.Time, bool, error) {
	if val, ok := props[ScheduledEnqueueTimeUtcMetadataKey]; ok && val != "" {
		timeVal, err := time.Parse(http.TimeFormat, val)
		if err != nil {
			return nil, false, err
		}

		return &timeVal, true, nil
	}

	return nil, false, nil
}
