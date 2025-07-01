package rabbitmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTryGetProperty(t *testing.T) {
	tests := []struct {
		name     string
		props    map[string]string
		key      string
		expected string
		found    bool
	}{
		{
			name:     "exact match",
			props:    map[string]string{"messageID": "test-id"},
			key:      "messageID",
			expected: "test-id",
			found:    true,
		},
		{
			name:     "case insensitive match",
			props:    map[string]string{"messageid": "test-id"},
			key:      "messageID",
			expected: "test-id",
			found:    true,
		},
		{
			name:     "uppercase match",
			props:    map[string]string{"MESSAGEID": "test-id"},
			key:      "messageID",
			expected: "test-id",
			found:    true,
		},
		{
			name:     "not found",
			props:    map[string]string{"otherKey": "value"},
			key:      "messageID",
			expected: "",
			found:    false,
		},
		{
			name:     "empty value",
			props:    map[string]string{"messageID": ""},
			key:      "messageID",
			expected: "",
			found:    false,
		},
		{
			name:     "whitespace value",
			props:    map[string]string{"messageID": "   "},
			key:      "messageID",
			expected: "   ",
			found:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, found := TryGetProperty(tt.props, tt.key)
			assert.Equal(t, tt.expected, value)
			assert.Equal(t, tt.found, found)
		})
	}
}
