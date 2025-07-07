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

func TestDefaultFinishReason(t *testing.T) {
	t.Run("returns tool_calls when tool calls are present", func(t *testing.T) {
		parts := []ContentPart{
			ToolCallContentPart{ID: "1", Function: ToolCallFunction{Name: "get_weather"}},
		}
		reason := DefaultFinishReason(parts)
		assert.Equal(t, "tool_calls", reason)
	})

	t.Run("returns stop when no tool calls are present", func(t *testing.T) {
		parts := []ContentPart{
			TextContentPart{Text: "Hello"},
		}
		reason := DefaultFinishReason(parts)
		assert.Equal(t, "stop", reason)
	})

	t.Run("returns stop for empty parts", func(t *testing.T) {
		parts := []ContentPart{}
		reason := DefaultFinishReason(parts)
		assert.Equal(t, "stop", reason)
	})
}
