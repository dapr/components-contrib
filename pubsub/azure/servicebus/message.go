package servicebus

import (
	"fmt"

	azservicebus "github.com/Azure/azure-service-bus-go"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
)

const (
	// MessageIDMetadataKey defines the metadata key for the message id.
	MessageIDMetadataKey = "MessageId"

	// CorrelationIDMetadataKey defines the metadata key for the correlation id.
	CorrelationIDMetadataKey = "CorrelationId"

	// SessionIDMetadataKey defines the metadata key for the session id.
	SessionIDMetadataKey = "SessionId"

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
	msgID, hasMsgID, _ := tryGetMessageID(req.Metadata)
	if hasMsgID {
		msg.ID = msgID
	}

	correlationID, hasCorrelationID, _ := tryGetCorrelationID(req.Metadata)
	if hasCorrelationID {
		msg.CorrelationID = correlationID
	}

	sessionID, hasSessionID, _ := tryGetSessionID(req.Metadata)
	if hasSessionID {
		msg.SessionID = &sessionID
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
		if hasSessionID {
			if partitionKey != sessionID {
				return nil, fmt.Errorf("session id %s and partition key %s should be equal when both present", sessionID, partitionKey)
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
