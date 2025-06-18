package echo

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func TestEchoStreamingInterface(t *testing.T) {
	testLogger := logger.NewLogger("test")
	echoComponent := NewEcho(testLogger)

	// Test that echo component implements StreamingConversation interface
	streamingComponent, ok := echoComponent.(conversation.StreamingConversation)
	assert.True(t, ok, "Echo component should implement StreamingConversation interface")
	assert.NotNil(t, streamingComponent, "StreamingConversation interface should not be nil")
}

func TestEchoStreamingFunctionality(t *testing.T) {
	testLogger := logger.NewLogger("test")
	echoComponent := NewEcho(testLogger)

	// Initialize the echo component
	ctx := t.Context()
	err := echoComponent.Init(ctx, conversation.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{},
		},
	})
	require.NoError(t, err)

	// Test streaming functionality
	streamingComponent, ok := echoComponent.(conversation.StreamingConversation)
	require.True(t, ok, "Echo component should implement StreamingConversation")

	// Prepare test request
	req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Message: "Hello streaming world",
				Role:    "user",
			},
		},
		ConversationContext: "test-context-123",
	}

	// Collect streaming chunks
	var chunks []string
	streamFunc := func(ctx context.Context, chunk []byte) error {
		chunks = append(chunks, string(chunk))
		return nil
	}

	// Execute streaming conversation
	resp, err := streamingComponent.ConverseStream(ctx, req, streamFunc)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify streaming chunks were received
	assert.NotEmpty(t, chunks, "Should receive streaming chunks")

	// Verify chunks combine to form the original message
	fullContent := strings.Join(chunks, "")
	assert.Equal(t, "Hello streaming world", fullContent)

	// Verify response structure
	assert.Len(t, resp.Outputs, 1)
	assert.Equal(t, "Hello streaming world", resp.Outputs[0].Result)
	assert.Equal(t, "test-context-123", resp.ConversationContext)

	// Verify we got multiple chunks (streaming behavior)
	assert.Greater(t, len(chunks), 1, "Should receive multiple chunks for streaming")
}

func TestEchoStreamingWithMultipleInputs(t *testing.T) {
	testLogger := logger.NewLogger("test")
	echoComponent := NewEcho(testLogger)

	// Initialize the echo component
	ctx := t.Context()
	err := echoComponent.Init(ctx, conversation.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{},
		},
	})
	require.NoError(t, err)

	streamingComponent, ok := echoComponent.(conversation.StreamingConversation)
	require.True(t, ok)

	// Test with multiple inputs
	req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Message: "First message",
				Role:    "user",
			},
			{
				Message: "Second message",
				Role:    "assistant",
			},
		},
	}

	var chunks []string
	streamFunc := func(ctx context.Context, chunk []byte) error {
		chunks = append(chunks, string(chunk))
		return nil
	}

	resp, err := streamingComponent.ConverseStream(ctx, req, streamFunc)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify we got chunks for both inputs
	assert.Greater(t, len(chunks), 2, "Should receive chunks for multiple inputs")

	// Verify response has both outputs
	assert.Len(t, resp.Outputs, 2)
	assert.Equal(t, "First message", resp.Outputs[0].Result)
	assert.Equal(t, "Second message", resp.Outputs[1].Result)
}

func TestEchoStreamingContextGeneration(t *testing.T) {
	testLogger := logger.NewLogger("test")
	echoComponent := NewEcho(testLogger)

	ctx := t.Context()
	err := echoComponent.Init(ctx, conversation.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{},
		},
	})
	require.NoError(t, err)

	streamingComponent, ok := echoComponent.(conversation.StreamingConversation)
	require.True(t, ok)

	// Test without providing context ID
	req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Message: "Test message",
				Role:    "user",
			},
		},
		// No ConversationContext provided
	}

	streamFunc := func(ctx context.Context, chunk []byte) error {
		return nil // Just consume chunks
	}

	resp, err := streamingComponent.ConverseStream(ctx, req, streamFunc)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify context ID was generated
	assert.NotEmpty(t, resp.ConversationContext, "Should generate context ID when none provided")
	assert.Contains(t, resp.ConversationContext, "echo-context-", "Generated context should have echo prefix")
}

func TestEchoStreamingErrorHandling(t *testing.T) {
	testLogger := logger.NewLogger("test")
	echoComponent := NewEcho(testLogger)

	ctx := t.Context()
	err := echoComponent.Init(ctx, conversation.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{},
		},
	})
	require.NoError(t, err)

	streamingComponent, ok := echoComponent.(conversation.StreamingConversation)
	require.True(t, ok)

	req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Message: "Test message",
				Role:    "user",
			},
		},
	}

	// Test with stream function that returns an error
	streamFunc := func(ctx context.Context, chunk []byte) error {
		return assert.AnError // Return a test error
	}

	resp, err := streamingComponent.ConverseStream(ctx, req, streamFunc)
	require.Error(t, err, "Should return error when streamFunc fails")
	require.Nil(t, resp, "Response should be nil when streaming fails")
}

func TestEchoStreamingWithCancelledContext(t *testing.T) {
	testLogger := logger.NewLogger("test")
	echoComponent := NewEcho(testLogger)

	ctx := t.Context()
	err := echoComponent.Init(ctx, conversation.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{},
		},
	})
	require.NoError(t, err)

	streamingComponent, ok := echoComponent.(conversation.StreamingConversation)
	require.True(t, ok)

	// Create a context that we can cancel
	streamCtx, cancel := context.WithCancel(ctx)
	cancel() // Cancel immediately

	req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Message: "This should be cancelled",
				Role:    "user",
			},
		},
	}

	streamFunc := func(ctx context.Context, chunk []byte) error {
		return nil
	}

	resp, err := streamingComponent.ConverseStream(streamCtx, req, streamFunc)
	require.Error(t, err, "Should return error when context is cancelled")
	require.Nil(t, resp, "Response should be nil when context is cancelled")
}
