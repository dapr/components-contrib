package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/internal/config"
)

func TestPrefixedBy(t *testing.T) {
	tests := map[string]struct {
		prefix   string
		input    interface{}
		expected interface{}
		err      string
	}{
		"map of string to string": {
			prefix: "test",
			input: map[string]string{
				"":        "",
				"ignore":  "don't include me",
				"testOne": "include me",
				"testTwo": "and me",
			},
			expected: map[string]string{
				"one": "include me",
				"two": "and me",
			},
		},
		"map of string to interface{}": {
			prefix: "test",
			input: map[string]interface{}{
				"":        "",
				"ignore":  "don't include me",
				"testOne": "include me",
				"testTwo": "and me",
			},
			expected: map[string]interface{}{
				"one": "include me",
				"two": "and me",
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			actual, err := config.PrefixedBy(tc.input, tc.prefix)
			if tc.err != "" {
				if assert.Error(t, err) {
					assert.Equal(t, tc.err, err.Error())
				}
			} else {
				assert.Equal(t, tc.expected, actual, "unexpected output")
			}
		})
	}
}
