package rabbitmq

import (
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	MetadataKeyMessageID     = "messageID"
	MetadataKeyCorrelationID = "correlationID"
	MetadataKeyContentType   = "contentType"
	MetadataKeyType          = "type"
	MetadataKeyPriority      = "priority"
	MetadataKeyTTL           = "ttl"
)

// TryGetProperty finds a property value using case-insensitive matching
func TryGetProperty(props map[string]string, key string) (string, bool) {
	// First try exact match
	if val, ok := props[key]; ok && val != "" {
		return val, true
	}

	// Then try case-insensitive match
	for k, v := range props {
		if v != "" && strings.EqualFold(key, k) {
			return v, true
		}
	}

	return "", false
}

// ApplyMetadataToPublishing applies common metadata fields to an AMQP publishing
func ApplyMetadataToPublishing(metadata map[string]string, publishing *amqp.Publishing) {
	if contentType, ok := TryGetProperty(metadata, MetadataKeyContentType); ok {
		publishing.ContentType = contentType
	}

	if messageID, ok := TryGetProperty(metadata, MetadataKeyMessageID); ok {
		publishing.MessageId = messageID
	}

	if correlationID, ok := TryGetProperty(metadata, MetadataKeyCorrelationID); ok {
		publishing.CorrelationId = correlationID
	}

	if aType, ok := TryGetProperty(metadata, MetadataKeyType); ok {
		publishing.Type = aType
	}
}
