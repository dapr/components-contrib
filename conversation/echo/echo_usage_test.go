package echo

import (
	"context"
	"testing"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEchoUsageInformation(t *testing.T) {
	e := NewEcho(nil)
	meta := conversation.Metadata{Base: metadata.Base{Properties: map[string]string{}}}
	err := e.Init(context.Background(), meta)
	require.NoError(t, err)

	req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{Message: "Hello world, this is a test message"},
		},
	}

	t.Run("non-streaming mode returns usage", func(t *testing.T) {
		resp, err := e.Converse(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Usage, "Usage information should be present")

		// Verify usage information is reasonable
		assert.Greater(t, resp.Usage.PromptTokens, int32(0), "Should have prompt tokens")
		assert.Greater(t, resp.Usage.CompletionTokens, int32(0), "Should have completion tokens")
		assert.Equal(t, resp.Usage.TotalTokens, resp.Usage.PromptTokens+resp.Usage.CompletionTokens, "Total should equal sum")
	})

	t.Run("streaming mode returns usage", func(t *testing.T) {
		var chunks []string
		var finalResp *conversation.ConversationResponse

		streamFunc := func(ctx context.Context, chunk []byte) error {
			chunks = append(chunks, string(chunk))
			return nil
		}

		// Cast to StreamingConversation interface
		streamer, ok := e.(conversation.StreamingConversation)
		require.True(t, ok, "Echo should implement StreamingConversation interface")

		resp, err := streamer.ConverseStream(context.Background(), req, streamFunc)
		require.NoError(t, err)
		finalResp = resp

		// Verify we got streaming chunks
		assert.Greater(t, len(chunks), 0, "Should receive streaming chunks")

		// Verify usage information is present in final response
		require.NotNil(t, finalResp, "Final response should be present")
		require.NotNil(t, finalResp.Usage, "Usage information should be present in streaming response")

		// Verify usage information is reasonable
		assert.Greater(t, finalResp.Usage.PromptTokens, int32(0), "Should have prompt tokens")
		assert.Greater(t, finalResp.Usage.CompletionTokens, int32(0), "Should have completion tokens")
		assert.Equal(t, finalResp.Usage.TotalTokens, finalResp.Usage.PromptTokens+finalResp.Usage.CompletionTokens, "Total should equal sum")
	})

	t.Run("usage information consistent between modes", func(t *testing.T) {
		// Test with the same input to ensure consistent usage calculation
		testReq := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{Message: "Consistent test message"},
			},
		}

		// Non-streaming
		nonStreamResp, err := e.Converse(context.Background(), testReq)
		require.NoError(t, err)
		require.NotNil(t, nonStreamResp.Usage)

		// Streaming
		streamer, ok := e.(conversation.StreamingConversation)
		require.True(t, ok, "Echo should implement StreamingConversation interface")

		streamResp, err := streamer.ConverseStream(context.Background(), testReq, func(ctx context.Context, chunk []byte) error {
			return nil // Ignore chunks for this test
		})
		require.NoError(t, err)
		require.NotNil(t, streamResp.Usage)

		// Usage should be the same for the same input
		assert.Equal(t, nonStreamResp.Usage.PromptTokens, streamResp.Usage.PromptTokens, "Prompt tokens should be consistent")
		assert.Equal(t, nonStreamResp.Usage.CompletionTokens, streamResp.Usage.CompletionTokens, "Completion tokens should be consistent")
		assert.Equal(t, nonStreamResp.Usage.TotalTokens, streamResp.Usage.TotalTokens, "Total tokens should be consistent")
	})
}
