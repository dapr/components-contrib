// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

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

type MessageWithMetadata interface {
	GetData() []byte
	GetMetadata() map[string]string
}

type busMessage struct {
	data     []byte
	metadata map[string]string
}

func (m *busMessage) GetData() []byte {
	return m.data
}

func (m *busMessage) GetMetadata() map[string]string {
	return m.metadata
}

func NewMessageWithMetadataFromASBMessage(asbMsg *azservicebus.Message) (MessageWithMetadata, error) {
	msg := &busMessage{
		data: asbMsg.Data,
	}

	addToMetadata := func(msg *busMessage, key, value string) {
		if msg.metadata == nil {
			msg.metadata = make(map[string]string)
		}

		msg.metadata[fmt.Sprintf("metadata.%s", key)] = value
	}

	if asbMsg.ID != "" {
		addToMetadata(msg, MessageIDMetadataKey, asbMsg.ID)
	}
	if asbMsg.SessionID != nil {
		addToMetadata(msg, SessionIDMetadataKey, *asbMsg.SessionID)
	}
	if asbMsg.CorrelationID != "" {
		addToMetadata(msg, CorrelationIDMetadataKey, asbMsg.CorrelationID)
	}
	if asbMsg.Label != "" {
		addToMetadata(msg, LabelMetadataKey, asbMsg.Label)
	}
	if asbMsg.ReplyTo != "" {
		addToMetadata(msg, ReplyToMetadataKey, asbMsg.ReplyTo)
	}
	if asbMsg.To != "" {
		addToMetadata(msg, ToMetadataKey, asbMsg.To)
	}
	if asbMsg.ContentType != "" {
		addToMetadata(msg, ContentTypeMetadataKey, asbMsg.ContentType)
	}
	if asbMsg.LockToken != nil {
		addToMetadata(msg, LockTokenMetadataKey, asbMsg.LockToken.String())
	}

	// Always set delivery count.
	addToMetadata(msg, DeliveryCountMetadataKey, strconv.FormatInt(int64(asbMsg.DeliveryCount), 10))

	//nolint:golint,nestif
	if asbMsg.SystemProperties != nil {
		systemProps := asbMsg.SystemProperties
		if systemProps.EnqueuedTime != nil {
			// Preserve RFC2616 time format.
			addToMetadata(msg, EnqueuedTimeUtcMetadataKey, systemProps.EnqueuedTime.UTC().Format(http.TimeFormat))
		}
		if systemProps.SequenceNumber != nil {
			addToMetadata(msg, SequenceNumberMetadataKey, strconv.FormatInt(*systemProps.SequenceNumber, 10))
		}
		if systemProps.ScheduledEnqueueTime != nil {
			// Preserve RFC2616 time format.
			addToMetadata(msg, ScheduledEnqueueTimeUtcMetadataKey, systemProps.ScheduledEnqueueTime.UTC().Format(http.TimeFormat))
		}
		if systemProps.PartitionKey != nil {
			addToMetadata(msg, PartitionKeyMetadataKey, *systemProps.PartitionKey)
		}
		if systemProps.LockedUntil != nil {
			// Preserve RFC2616 time format.
			addToMetadata(msg, LockedUntilUtcMetadataKey, systemProps.LockedUntil.UTC().Format(http.TimeFormat))
		}
	}

	return msg, nil
}

func NewPubsubMessageFromASBMessage(asbMsg *azservicebus.Message, topic string) (*pubsub.NewMessage, error) {
	m, err := NewMessageWithMetadataFromASBMessage(asbMsg)
	if err != nil {
		return nil, err
	}
	return &pubsub.NewMessage{
		Data:     m.GetData(),
		Topic:    topic,
		Metadata: m.GetMetadata(),
	}, nil
}

// NewASBMessageFromMessageWithMetadata builds a new Azure Service Bus message from a MessageWithMetadata compliant structure.
func NewASBMessageFromMessageWithMetadata(req MessageWithMetadata) (*azservicebus.Message, error) {
	asbMsg := azservicebus.NewMessage(req.GetData())
	metadata := req.GetMetadata()

	// Common properties.
	ttl, hasTTL, _ := contrib_metadata.TryGetTTL(metadata)
	if hasTTL {
		asbMsg.TTL = &ttl
	}

	// Azure Service Bus specific properties.
	// reference: https://docs.microsoft.com/en-us/rest/api/servicebus/message-headers-and-properties#message-headers
	msgID, hasMsgID, _ := tryGetMessageID(metadata)
	if hasMsgID {
		asbMsg.ID = msgID
	}

	correlationID, hasCorrelationID, _ := tryGetCorrelationID(metadata)
	if hasCorrelationID {
		asbMsg.CorrelationID = correlationID
	}

	sessionID, hasSessionID, _ := tryGetSessionID(metadata)
	if hasSessionID {
		asbMsg.SessionID = &sessionID
	}

	label, hasLabel, _ := tryGetLabel(metadata)
	if hasLabel {
		asbMsg.Label = label
	}

	replyTo, hasReplyTo, _ := tryGetReplyTo(metadata)
	if hasReplyTo {
		asbMsg.ReplyTo = replyTo
	}

	to, hasTo, _ := tryGetTo(metadata)
	if hasTo {
		asbMsg.To = to
	}

	partitionKey, hasPartitionKey, _ := tryGetPartitionKey(metadata)
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

	contentType, hasContentType, _ := tryGetContentType(metadata)
	if hasContentType {
		asbMsg.ContentType = contentType
	}

	scheduledEnqueueTime, hasScheduledEnqueueTime, _ := tryGetScheduledEnqueueTime(metadata)
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
