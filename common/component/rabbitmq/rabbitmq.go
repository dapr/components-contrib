package rabbitmq

import (
	"strings"
)

// tryGetProperty checks for a property value using various key formats: original, camelCase, and case-insensitive
func tryGetProperty(props map[string]string, keys ...string) (string, bool) {
	// First try exact match for all provided keys
	for _, key := range keys {
		if val, ok := props[key]; ok && val != "" {
			return val, true
		}
	}

	// Then try case-insensitive match if no exact matches were found
	for k, v := range props {
		if v != "" {
			lowerK := strings.ToLower(k)
			for _, key := range keys {
				if strings.ToLower(key) == lowerK {
					return v, true
				}
			}
		}
	}

	return "", false
}

func TryGetMessageID(props map[string]string) (string, bool) {
	return tryGetProperty(props, "messageId", "messageID", "MessageId", "MessageID")
}

func TryGetCorrelationID(props map[string]string) (string, bool) {
	return tryGetProperty(props, "correlationId", "correlationID", "CorrelationId", "CorrelationID")
}

func TryGetContentType(props map[string]string) (string, bool) {
	return tryGetProperty(props, "contentType", "ContentType")
}

func TryGetType(props map[string]string) (string, bool) {
	return tryGetProperty(props, "type", "Type")
}
