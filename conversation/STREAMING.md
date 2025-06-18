# Conversation Streaming Support

This document describes the streaming support implementation for Dapr conversation components.

## Overview

Conversation components can now support real-time streaming responses by implementing the optional `StreamingConversation` interface. This enables applications to receive conversation responses as they are generated, rather than waiting for the complete response.

## StreamingConversation Interface

```go
type StreamingConversation interface {
    // ConverseStream enables streaming conversation using LangChain Go's WithStreamingFunc.
    // The streamFunc will be called for each chunk of content as it's generated.
    ConverseStream(ctx context.Context, req *ConversationRequest, streamFunc func(ctx context.Context, chunk []byte) error) (*ConversationResponse, error)
}
```

## Implementation Pattern

All LangChain Go-based components follow the same implementation pattern:

```go
func (c *Component) ConverseStream(ctx context.Context, req *ConversationRequest, streamFunc func(ctx context.Context, chunk []byte) error) (*ConversationResponse, error) {
    // Convert inputs to LangChain messages (reuse existing logic)
    messages := make([]llms.MessageContent, 0, len(req.Inputs))
    for _, input := range req.Inputs {
        role := ConvertLangchainRole(input.Role)
        messages = append(messages, llms.MessageContent{
            Role: role,
            Parts: []llms.ContentPart{llms.TextPart(input.Message)},
        })
    }

    // Build options with streaming enabled
    opts := []llms.CallOption{
        llms.WithStreamingFunc(streamFunc), // Enable streaming
    }

    // Add temperature and other options (reuse existing logic)
    if req.Temperature > 0 {
        opts = append(opts, LangchainTemperature(req.Temperature))
    }

    // Call LangChain Go with streaming enabled
    resp, err := c.llm.GenerateContent(ctx, messages, opts...)
    if err != nil {
        return nil, err
    }

    // Return response structure (similar to non-streaming)
    outputs := make([]ConversationResult, 0, len(resp.Choices))
    for i := range resp.Choices {
        outputs = append(outputs, ConversationResult{
            Result:     resp.Choices[i].Content,
            Parameters: req.Parameters,
        })
    }

    return &ConversationResponse{
        ConversationContext: req.ConversationContext,
        Outputs:            outputs,
    }, nil
}
```

## Supported Components

The following components implement streaming support:

### LangChain Go Components
- **OpenAI** (`conversation/openai`) - GPT models with streaming
- **Anthropic** (`conversation/anthropic`) - Claude models with streaming  
- **Google AI** (`conversation/googleai`) - Gemini models with streaming
- **Ollama** (`conversation/ollama`) - Local LLM streaming
- **Mistral** (`conversation/mistral`) - Mistral AI streaming
- **AWS Bedrock** (`conversation/aws/bedrock`) - AWS Bedrock model streaming
- **DeepSeek** (`conversation/deepseek`) - DeepSeek model streaming

### Test Components
- **Echo** (`conversation/echo`) - Test component with word-by-word streaming

### Non-Streaming Components
- **HuggingFace** (`conversation/huggingface`) - HuggingFace model does not supports it even if using langchaingo

## Usage Example

```go
// Check if component supports streaming
if streamingComponent, ok := component.(StreamingConversation); ok {
    // Use streaming
    streamFunc := func(ctx context.Context, chunk []byte) error {
        fmt.Print(string(chunk)) // Print chunks as they arrive
        return nil
    }
    
    resp, err := streamingComponent.ConverseStream(ctx, req, streamFunc)
} else {
    // Fallback to non-streaming
    resp, err := component.Converse(ctx, req)
}
```

## Key Benefits

1. **Real-time Response**: Users see content as it's generated
2. **Better UX**: Perceived faster response times
3. **Backward Compatible**: Non-streaming API remains unchanged
4. **Consistent Interface**: All LangChain Go components use the same pattern
5. **Easy Integration**: Simple interface for Dapr runtime integration

## Testing

Each component includes tests to verify:
- Interface implementation
- Streaming functionality
- Multiple input handling
- Context generation
- Error handling
- Cancellation support

Run tests with:
```bash
go test ./conversation/... -v
```

## Integration with Dapr Runtime

The Dapr runtime can detect streaming support and use it for the conversation API:

```go
// In Dapr runtime
if streamingComponent, ok := component.(conversation.StreamingConversation); ok {
    // Use streaming pipeline with PII scrubbing and middleware
    return streamingComponent.ConverseStream(ctx, req, streamingPipeline.ProcessChunk)
} else {
    // Use regular conversation
    return component.Converse(ctx, req)
}
```

This enables the Dapr conversation API to provide streaming responses while maintaining full backward compatibility.

## Token Usage Information

Both streaming and non-streaming conversation APIs support token usage reporting. Usage information is automatically extracted from LLM provider responses and included in the conversation response.

### Usage Information Structure

```go
type UsageInfo struct {
    PromptTokens     int32 `json:"prompt_tokens"`
    CompletionTokens int32 `json:"completion_tokens"`
    TotalTokens      int32 `json:"total_tokens"`
}
```

### How Usage Information is Extracted

The conversation components automatically extract token usage from LangChain Go responses using multiple patterns:

1. **LangChain Go Standard Format** (used by OpenAI, Anthropic, etc.):
   ```go
   GenerationInfo: map[string]any{
       "PromptTokens":     10,
       "CompletionTokens": 5,
       "TotalTokens":      15,
   }
   ```

2. **Provider-specific formats** (Google AI, direct API responses, etc.)

### Usage in Components

Components automatically include usage information in responses:

```go
func (c *Component) Converse(ctx context.Context, req *conversation.ConversationRequest) (*conversation.ConversationResponse, error) {
    // ... LangChain Go call ...
    resp, err := c.llm.GenerateContent(ctx, messages, opts...)
    if err != nil {
        return nil, err
    }

    // Extract usage information automatically
    usage := conversation.ExtractUsageFromResponse(resp)

    return &conversation.ConversationResponse{
        Outputs: outputs,
        Usage:   usage, // Usage information included
    }, nil
}
```

### Usage in Streaming Mode

For streaming conversations, usage information is sent in the completion message:

```protobuf
message ConversationStreamComplete {
    optional string contextID = 1;
    optional ConversationUsage usage = 2; // Usage stats here
}
```

### Supported Providers

Token usage is supported by most LLM providers through LangChain Go:

- ✅ **OpenAI**: Full usage reporting (prompt, completion, total tokens)
- ✅ **Anthropic**: Usage reporting when available  
- ✅ **Google AI/Vertex AI**: Usage metadata extraction
- ✅ **Echo**: Simulated usage for testing (character-based estimation)
- ⚠️ **Other providers**: Usage availability depends on LangChain Go implementation

### Usage Example

```go
resp, err := component.Converse(ctx, req)
if err != nil {
    return err
}

if resp.Usage != nil {
    fmt.Printf("Token usage - Prompt: %d, Completion: %d, Total: %d\n",
        resp.Usage.PromptTokens, resp.Usage.CompletionTokens, resp.Usage.TotalTokens)
}
``` 