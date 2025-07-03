/*
Copyright 2025 The Dapr Authors
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

package conversation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeFinishReason(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		// Stop reasons
		{"stop", "stop", FinishReasonStop},
		{"end_turn", "end_turn", FinishReasonStop},
		{"eos_token", "eos_token", FinishReasonStop},
		{"stop_sequence", "stop_sequence", FinishReasonStop},
		{"finish", "finish", FinishReasonStop},
		{"leading/trailing spaces", "  stop  ", FinishReasonStop},
		{"mixed case", "sToP", FinishReasonStop},

		// Length reasons
		{"length", "length", FinishReasonLength},
		{"max_tokens", "max_tokens", FinishReasonLength},
		{"max_output_tokens", "max_output_tokens", FinishReasonLength},

		// Tool calling reasons
		{"tool_calls", "tool_calls", FinishReasonToolCalls},
		{"tool_use", "tool_use", FinishReasonToolCalls},
		{"function_call", "function_call", FinishReasonToolCalls},

		// Content filter reasons
		{"safety", "safety", FinishReasonContentFilter},
		{"content_filter", "content_filter", FinishReasonContentFilter},
		{"content_filtered", "content_filtered", FinishReasonContentFilter},
		{"prohibited_content", "prohibited_content", FinishReasonContentFilter},
		{"spii", "spii", FinishReasonContentFilter},

		// Error reasons
		{"error", "error", FinishReasonError},
		{"insufficient_system_resource", "insufficient_system_resource", FinishReasonError},
		{"recitation", "recitation", FinishReasonError},

		// Unknown reasons
		{"empty string", "", FinishReasonUnknown},
		{"finish_reason_unspecified", "finish_reason_unspecified", FinishReasonUnknown},
		{"blocked_reason_unspecified", "blocked_reason_unspecified", FinishReasonUnknown},
		{"other", "other", FinishReasonUnknown},

		// Unmapped reason
		{"unmapped reason", "custom_reason", "custom_reason"},
		{"another unmapped", "some_other_value", "some_other_value"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := NormalizeFinishReason(tc.input)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestGetEnvKey(t *testing.T) {
	t.Run("returns first valid env key", func(t *testing.T) {
		t.Setenv("KEY_B", "value_b")
		defer t.Setenv("KEY_B", "")

		key := GetEnvKey("KEY_A", "KEY_B", "KEY_C")
		assert.Equal(t, "KEY_B", key)
	})

	t.Run("returns second valid env key", func(t *testing.T) {
		t.Setenv("KEY_C", "value_c")
		defer t.Setenv("KEY_C", "")

		key := GetEnvKey("KEY_A", "KEY_B", "KEY_C")
		assert.Equal(t, "KEY_C", key)
	})

	t.Run("returns empty string if no keys are set", func(t *testing.T) {
		key := GetEnvKey("KEY_X", "KEY_Y", "KEY_Z")
		assert.Equal(t, "", key)
	})

	t.Run("returns empty string for empty input", func(t *testing.T) {
		key := GetEnvKey()
		assert.Equal(t, "", key)
	})

	t.Run("handles multiple set keys", func(t *testing.T) {
		t.Setenv("KEY_1", "value_1")
		t.Setenv("KEY_2", "value_2")
		defer t.Setenv("KEY_1", "")
		defer t.Setenv("KEY_2", "")

		key := GetEnvKey("KEY_1", "KEY_2")
		assert.Equal(t, "KEY_1", key, "should return the first one found")
	})
}
