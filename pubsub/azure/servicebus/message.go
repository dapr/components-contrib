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
	"fmt"
	"net/http"
	"strconv"
	"time"

	azservicebus "github.com/Azure/azure-service-bus-go"

	contrib_metadata "github.com/dapr/components-contrib/metadata"
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

func NewPubsubMessageFromASBMessage(asbMsg *azservicebus.Message, topic string) (*pubsub.NewMessage, error) {
	pubsubMsg := &pubsub.NewMessage{
		Data:  asbMsg.Data,
		Topic: topic,
	}

	addToMetadata := func(msg *pubsub.NewMessage, key, value string) {
		if msg.Metadata == nil {
			msg.Metadata = make(map[string]string)
		}

		msg.Metadata[fmt.Sprintf("metadata.%s", key)] = value
	}

	if asbMsg.ID != "" {
		addToMetadata(pubsubMsg, MessageIDMetadataKey, asbMsg.ID)
	}
	if asbMsg.SessionID != nil {
		addToMetadata(pubsubMsg, SessionIDMetadataKey, *asbMsg.SessionID)
	}
	if asbMsg.CorrelationID != "" {
		addToMetadata(pubsubMsg, CorrelationIDMetadataKey, asbMsg.CorrelationID)
	}
	if asbMsg.Label != "" {
		addToMetadata(pubsubMsg, LabelMetadataKey, asbMsg.Label)
	}
	if asbMsg.ReplyTo != "" {
		addToMetadata(pubsubMsg, ReplyToMetadataKey, asbMsg.ReplyTo)
	}
	if asbMsg.To != "" {
		addToMetadata(pubsubMsg, ToMetadataKey, asbMsg.To)
	}
	if asbMsg.ContentType != "" {
		addToMetadata(pubsubMsg, ContentTypeMetadataKey, asbMsg.ContentType)
	}
	if asbMsg.LockToken != nil {
		addToMetadata(pubsubMsg, LockTokenMetadataKey, asbMsg.LockToken.String())
	}

	// Always set delivery count.
	addToMetadata(pubsubMsg, DeliveryCountMetadataKey, strconv.FormatInt(int64(asbMsg.DeliveryCount), 10))

	//nolint:golint,nestif
	if asbMsg.SystemProperties != nil {
		systemProps := asbMsg.SystemProperties
		if systemProps.EnqueuedTime != nil {
			// Preserve RFC2616 time format.
			addToMetadata(pubsubMsg, EnqueuedTimeUtcMetadataKey, systemProps.EnqueuedTime.UTC().Format(http.TimeFormat))
		}
		if systemProps.SequenceNumber != nil {
			addToMetadata(pubsubMsg, SequenceNumberMetadataKey, strconv.FormatInt(*systemProps.SequenceNumber, 10))
		}
		if systemProps.ScheduledEnqueueTime != nil {
			// Preserve RFC2616 time format.
			addToMetadata(pubsubMsg, ScheduledEnqueueTimeUtcMetadataKey, systemProps.ScheduledEnqueueTime.UTC().Format(http.TimeFormat))
		}
		if systemProps.PartitionKey != nil {
			addToMetadata(pubsubMsg, PartitionKeyMetadataKey, *systemProps.PartitionKey)
		}
		if systemProps.LockedUntil != nil {
			// Preserve RFC2616 time format.
			addToMetadata(pubsubMsg, LockedUntilUtcMetadataKey, systemProps.LockedUntil.UTC().Format(http.TimeFormat))
		}
	}

	return pubsubMsg, nil
}

// NewASBMessageFromPubsubRequest builds a new Azure Service Bus message from a PublishRequest.
func NewASBMessageFromPubsubRequest(req *pubsub.PublishRequest) (*azservicebus.Message, error) {
	asbMsg := azservicebus.NewMessage(req.Data)

	// Common properties.
	ttl, hasTTL, _ := contrib_metadata.TryGetTTL(req.Metadata)
	if hasTTL {
		asbMsg.TTL = &ttl
	}

	// Azure Service Bus specific properties.
	// reference: https://docs.microsoft.com/en-us/rest/api/servicebus/message-headers-and-properties#message-headers
	msgID, hasMsgID, _ := tryGetMessageID(req.Metadata)
	if hasMsgID {
		asbMsg.ID = msgID
	}

	correlationID, hasCorrelationID, _ := tryGetCorrelationID(req.Metadata)
	if hasCorrelationID {
		asbMsg.CorrelationID = correlationID
	}

	sessionID, hasSessionID, _ := tryGetSessionID(req.Metadata)
	if hasSessionID {
		asbMsg.SessionID = &sessionID
	}

	label, hasLabel, _ := tryGetLabel(req.Metadata)
	if hasLabel {
		asbMsg.Label = label
	}

	replyTo, hasReplyTo, _ := tryGetReplyTo(req.Metadata)
	if hasReplyTo {
		asbMsg.ReplyTo = replyTo
	}

	to, hasTo, _ := tryGetTo(req.Metadata)
	if hasTo {
		asbMsg.To = to
	}

	partitionKey, hasPartitionKey, _ := tryGetPartitionKey(req.Metadata)
	if hasPartitionKey {
		if hasSessionID {
			if partitionKey != sessionID {
				return nil, fmt.Errorf("session id %s and partition key %s should be equal when both present", sessionID, partitionKey)
			}
		}

		if asbMsg.SystemProperties == nil {
			asbMsg.SystemProperties = &azservicebus.SystemProperties{}
		}

		asbMsg.SystemProperties.PartitionKey = &partitionKey
	}

	contentType, hasContentType, _ := tryGetContentType(req.Metadata)
	if hasContentType {
		asbMsg.ContentType = contentType
	}

	scheduledEnqueueTime, hasScheduledEnqueueTime, _ := tryGetScheduledEnqueueTime(req.Metadata)
	if hasScheduledEnqueueTime {
		if asbMsg.SystemProperties == nil {
			asbMsg.SystemProperties = &azservicebus.SystemProperties{}
		}

		asbMsg.SystemProperties.ScheduledEnqueueTime = scheduledEnqueueTime
	}

	return asbMsg, nil
}

func tryGetMessageID(props map[string]string) (string, bool, error) {
	if val, ok := props[MessageIDMetadataKey]; ok && val != "" {
		return val, true, nil
	}

	return "", false, nil
}

func tryGetCorrelationID(props map[string]string) (string, bool, error) {
	if val, ok := props[CorrelationIDMetadataKey]; ok && val != "" {
		return val, true, nil
	}

	return "", false, nil
}

func tryGetSessionID(props map[string]string) (string, bool, error) {
	if val, ok := props[SessionIDMetadataKey]; ok && val != "" {
		return val, true, nil
	}

	return "", false, nil
}

func tryGetLabel(props map[string]string) (string, bool, error) {
	if val, ok := props[LabelMetadataKey]; ok && val != "" {
		return val, true, nil
	}

	return "", false, nil
}

func tryGetReplyTo(props map[string]string) (string, bool, error) {
	if val, ok := props[ReplyToMetadataKey]; ok && val != "" {
		return val, true, nil
	}

	return "", false, nil
}

func tryGetTo(props map[string]string) (string, bool, error) {
	if val, ok := props[ToMetadataKey]; ok && val != "" {
		return val, true, nil
	}

	return "", false, nil
}

func tryGetPartitionKey(props map[string]string) (string, bool, error) {
	if val, ok := props[PartitionKeyMetadataKey]; ok && val != "" {
		return val, true, nil
	}

	return "", false, nil
}

func tryGetContentType(props map[string]string) (string, bool, error) {
	if val, ok := props[ContentTypeMetadataKey]; ok && val != "" {
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
