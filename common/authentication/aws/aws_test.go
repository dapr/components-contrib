package aws

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewEnvironmentSettings(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
	}{
		{
			name: "valid metadata",
			metadata: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := NewEnvironmentSettings(tt.metadata)
			assert.NoError(t, err)
			assert.NotNil(t, result)
		})
	}
}
