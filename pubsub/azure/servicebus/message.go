package servicebus

import (
	"fmt"

	azservicebus "github.com/Azure/azure-service-bus-go"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
)

const (
	// MessageIdMetadataKey defines the metadata key for the message id.
	MessageIdMetadataKey = "MessageId"

	// CorrelationIdMetadataKey defines the metadata key for the correlation id.
	CorrelationIdMetadataKey = "CorrelationId"

	// SessionIdMetadataKey defines the metadata key for the session id.
	SessionIdMetadataKey = "SessionId"

	// LabelMetadataKey defines the metadata key for the label.
	LabelMetadataKey = "Label"

	// ReplyToMetadataKey defines the metadata key for the reply to value.
	ReplyToMetadataKey = "ReplyTo"

	// ToMetadataKey defines the metadata key for the to value.
	ToMetadataKey = "To"

	// PartitionKeyMetadataKey defines the metadata key for the partition key.
	PartitionKeyMetadataKey = "PartitionKey"

	// ContentTypeMetadataKey defines the metadata key for the content type.
	ContentTypeMetadataKey = "Content-Type"
)

// NewMessageFromRequest builds a new Azure Service Bus message from a PublishRequest.
func NewMessageFromRequest(req *pubsub.PublishRequest) (*azservicebus.Message, error) {
	msg := azservicebus.NewMessage(req.Data)

	// Common properties.
	ttl, hasTTL, _ := contrib_metadata.TryGetTTL(req.Metadata)
	if hasTTL {
		msg.TTL = &ttl
	}

	// Azure Service Bus specific properties.
	// reference: https://docs.microsoft.com/en-us/rest/api/servicebus/message-headers-and-properties#message-headers
	msgId, hasMsgId, _ := tryGetMessageId(req.Metadata)
	if hasMsgId {
		msg.ID = msgId
	}

	correlationId, hasCorrelationId, _ := tryGetCorrelationId(req.Metadata)
	if hasCorrelationId {
		msg.CorrelationID = correlationId
	}

	sessionId, hasSessionId, _ := tryGetSessionId(req.Metadata)
	if hasSessionId {
		msg.SessionID = &sessionId
	}

	label, hasLabel, _ := tryGetLabel(req.Metadata)
	if hasLabel {
		msg.Label = label
	}

	replyTo, hasReplyTo, _ := tryGetReplyTo(req.Metadata)
	if hasReplyTo {
		msg.ReplyTo = replyTo
	}

	to, hasTo, _ := tryGetTo(req.Metadata)
	if hasTo {
		msg.To = to
	}

	partitionKey, hasPartitionKey, _ := tryGetPartitionKey(req.Metadata)
	if hasPartitionKey {
		if hasSessionId {
			if partitionKey != sessionId {
				return nil, fmt.Errorf("session id %s and partition key %s should be equal when both present", sessionId, partitionKey)
			}
		}

		if msg.SystemProperties == nil {
			msg.SystemProperties = &azservicebus.SystemProperties{}
		}

		msg.SystemProperties.PartitionKey = &partitionKey
	}

	contentType, hasContentType, _ := tryGetContentType(req.Metadata)
	if hasContentType {
		msg.ContentType = contentType
	}

	return msg, nil
}

func tryGetMessageId(props map[string]string) (string, bool, error) {
	if val, ok := props[MessageIdMetadataKey]; ok && val != "" {
		return val, true, nil
	}

	return "", false, nil
}

func tryGetCorrelationId(props map[string]string) (string, bool, error) {
	if val, ok := props[CorrelationIdMetadataKey]; ok && val != "" {
		return val, true, nil
	}

	return "", false, nil
}

func tryGetSessionId(props map[string]string) (string, bool, error) {
	if val, ok := props[SessionIdMetadataKey]; ok && val != "" {
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
