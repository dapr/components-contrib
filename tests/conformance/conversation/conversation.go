/*
Copyright 2024 The Dapr Authors
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
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/conformance/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestConfig struct {
	utils.CommonConfig
}

func NewTestConfig(componentName string) TestConfig {
	tc := TestConfig{
		utils.CommonConfig{
			ComponentType: "conversation",
			ComponentName: componentName,
		},
	}

	return tc
}

func ConformanceTests(t *testing.T, props map[string]string, conv conversation.Conversation, component string) {
	t.Run("init", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()

		err := conv.Init(ctx, conversation.Metadata{
			Base: metadata.Base{
				Properties: props,
			},
		})
		require.NoError(t, err)
	})

	if t.Failed() {
		t.Fatal("initialization failed")
	}

	t.Run("converse", func(t *testing.T) {
		t.Run("get a non-empty response without errors", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "what is the time?",
					},
				},
			}
			resp, err := conv.Converse(ctx, req)

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)
			assert.NotEmpty(t, resp.Outputs[0].Result)
		})

		t.Run("returns usage information", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "Say hello",
					},
				},
			}
			resp, err := conv.Converse(ctx, req)

			require.NoError(t, err)
			require.NotNil(t, resp.Usage, "Usage information should be present")
			assert.GreaterOrEqual(t, resp.Usage.PromptTokens, int32(0), "Should have non-negative prompt tokens")
			assert.GreaterOrEqual(t, resp.Usage.CompletionTokens, int32(0), "Should have non-negative completion tokens")
			assert.Equal(t, resp.Usage.TotalTokens, resp.Usage.PromptTokens+resp.Usage.CompletionTokens, "Total should equal sum of prompt and completion tokens")
		})
	})

	t.Run("streaming", func(t *testing.T) {
		// Check if component supports streaming
		streamer, ok := conv.(conversation.StreamingConversation)
		if !ok {
			t.Skipf("Component %s does not implement StreamingConversation interface, skipping streaming tests", component)
			return
		}

		// Create a simple request for streaming tests
		req := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Message: "Say hello world",
				},
			},
		}

		// First try to stream and see if it's supported or disabled
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()

		streamFunc := func(ctx context.Context, chunk []byte) error {
			return nil // Simple stream function for testing
		}

		resp, err := streamer.ConverseStream(ctx, req, streamFunc)

		// Check if streaming is disabled (like HuggingFace)
		if err != nil && (strings.Contains(err.Error(), "streaming is not supported") ||
			strings.Contains(err.Error(), "streaming not supported") ||
			errors.Is(err, errors.New("streaming is not supported by this model or provider"))) {
			t.Run("streaming disabled returns proper error", func(t *testing.T) {
				// This is expected for components like HuggingFace
				require.Error(t, err, "Should return error when streaming is disabled")
				assert.Nil(t, resp, "Response should be nil when streaming is disabled")
				t.Logf("Component %s has streaming disabled (expected): %v", component, err)
			})
			return // Skip other streaming tests if streaming is disabled
		}

		// If we get here, streaming should be working
		t.Run("stream response without errors", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			var chunks []string
			var totalContent string

			streamFunc := func(ctx context.Context, chunk []byte) error {
				content := string(chunk)
				chunks = append(chunks, content)
				totalContent += content
				return nil
			}

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "Say hello world",
					},
				},
			}

			resp, err := streamer.ConverseStream(ctx, req, streamFunc)
			require.NoError(t, err)
			require.NotNil(t, resp)

			// Verify we got streaming chunks (at least some content)
			assert.NotEmpty(t, totalContent, "Should have received streaming content")
		})

		t.Run("streaming returns usage information", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			streamFunc := func(ctx context.Context, chunk []byte) error {
				return nil // Ignore chunks for this test
			}

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "Count to three",
					},
				},
			}

			resp, err := streamer.ConverseStream(ctx, req, streamFunc)
			require.NoError(t, err)
			require.NotNil(t, resp)

			// Verify usage information is present in streaming response
			require.NotNil(t, resp.Usage, "Usage information should be present in streaming response")
			assert.GreaterOrEqual(t, resp.Usage.PromptTokens, int32(0), "Should have non-negative prompt tokens")
			assert.GreaterOrEqual(t, resp.Usage.CompletionTokens, int32(0), "Should have non-negative completion tokens")
			assert.Equal(t, resp.Usage.TotalTokens, resp.Usage.PromptTokens+resp.Usage.CompletionTokens, "Total should equal sum of prompt and completion tokens")
		})
	})
}
