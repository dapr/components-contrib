# Anthropic Conversation Component

This component provides conversation capabilities using Anthropic's Claude models through the langchaingo library.

## Overview

The Anthropic conversation component enables natural language interactions with Claude models, including support for:
- Text-based conversations
- Multi-turn tool calling
- Content parts (text, tool calls, tool results)
- Streaming responses (text only - tool call streaming disabled due to langchaingo limitations)
- Usage tracking (token consumption)

## Configuration

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: anthropic
spec:
  type: conversation.anthropic
  version: v1
  metadata:
  - name: key
    value: "your-anthropic-api-key"
  - name: model
    value: "claude-3-haiku-20240307"  # Optional, defaults to claude-sonnet-4-20250514
  - name: cacheTTL
    value: "10m"  # Optional, enables response caching
  - name: endpoint  
    value: "https://api.anthropic.com"  # Optional, custom endpoint
```

### Configuration Parameters

| Parameter | Required | Description | Default |
|-----------|----------|-------------|---------|
| `key` | Yes | Anthropic API key (or set `ANTHROPIC_API_KEY` env var) | - |
| `model` | No | Claude model to use | `claude-sonnet-4-20250514` |
| `cacheTTL` | No | Cache TTL for response caching (e.g., "10m", "1h") | - |
| `endpoint` | No | Custom API endpoint | - |

### Available Models

- `claude-sonnet-4-20250514` (default)
- `claude-3-5-sonnet-20241022`
- `claude-3-haiku-20240307`
- `claude-3-opus-20240229`

## Usage

### Basic Text Conversation

```json
{
  "inputs": [
    {
      "role": "user",
      "parts": [
        {
          "type": "text",
          "text": "Hello, how are you?"
        }
      ]
    }
  ]
}
```

### Tool Calling

```json
{
  "tools": [
    {
      "type": "function",
      "function": {
        "name": "get_weather",
        "description": "Get current weather for a location",
        "parameters": {
          "type": "object",
          "properties": {
            "location": {
              "type": "string",
              "description": "City name"
            }
          },
          "required": ["location"]
        }
      }
    }
  ],
  "inputs": [
    {
      "role": "user",
      "parts": [
        {
          "type": "text",
          "text": "What's the weather like in San Francisco?"
        }
      ]
    }
  ]
}
```

### Response with Tool Call

```json
{
  "outputs": [
    {
      "parts": [
        {
          "type": "tool_call",
          "id": "toolu_01A5b3c4D5e6F7g8H9i0J1k2",
          "type": "function",
          "function": {
            "name": "get_weather",
            "arguments": "{\"location\":\"San Francisco\"}"
          }
        }
      ],
      "finish_reason": "tool_calls"
    }
  ],
  "usage": {
    "promptTokens": 45,
    "completionTokens": 12,
    "totalTokens": 57
  }
}
```

## Multi-Turn Tool Calling

⚠️ **Important**: Anthropic has strict requirements for multi-turn tool calling conversations. The conversation structure must follow a specific pattern for tool calls to work correctly.

### Required Conversation Flow

For multi-turn tool calling, the conversation history must be structured exactly as follows:

1. **User message** with tool definitions in the `tools` field
2. **Assistant message** with tool call response
3. **Tool result message** with the tool execution result
4. **Follow-up user message**

### Example Multi-Turn Conversation

**Step 1: Initial request with tools**
```json
{
  "tools": [
    {
      "type": "function",
      "function": {
        "name": "get_weather",
        "description": "Get current weather for a location",
        "parameters": {
          "type": "object",
          "properties": {
            "location": {"type": "string", "description": "City name"}
          },
          "required": ["location"]
        }
      }
    }
  ],
  "inputs": [
    {
      "role": "user", 
      "parts": [
        {"type": "text", "text": "What's the weather in Boston?"}
      ]
    }
  ]
}
```

**Step 2: Provide tool results and ask follow-up**
```json
{
  "tools": [
    {
      "type": "function",
      "function": {
        "name": "get_weather",
        "description": "Get current weather for a location",
        "parameters": {
          "type": "object",
          "properties": {
            "location": {"type": "string", "description": "City name"}
          },
          "required": ["location"]
        }
      }
    }
  ],
  "inputs": [
    {
      "role": "user",
      "parts": [
        {"type": "text", "text": "What's the weather in Boston?"}
      ]
    },
    {
      "role": "assistant",
      "parts": [
        {
          "type": "tool_call",
          "id": "toolu_01A5b3c4D5e6F7g8H9i0J1k2",
          "type": "function", 
          "function": {
            "name": "get_weather",
            "arguments": "{\"location\":\"Boston\"}"
          }
        }
      ]
    },
    {
      "role": "tool",
      "parts": [
        {
          "type": "tool_result",
          "tool_call_id": "toolu_01A5b3c4D5e6F7g8H9i0J1k2",
          "name": "get_weather",
          "content": "Sunny, 72°F in Boston"
        }
      ]
    },
    {
      "role": "user",
      "parts": [
        {"type": "text", "text": "What should I wear for this weather?"}
      ]
    }
  ]
}
```

## Streaming

The Anthropic component supports streaming for text responses:

```go
if streamingConv, ok := conv.(conversation.StreamingConversation); ok {
    resp, err := streamingConv.ConverseStream(ctx, req, func(ctx context.Context, chunk []byte) error {
        fmt.Printf("Chunk: %s", string(chunk))
        return nil
    })
}
```

⚠️ **Note**: Tool call streaming is disabled for Anthropic due to langchaingo limitations. Tool calls are returned as complete responses.

## Provider-Specific Behavior

### Tool Calling Characteristics
- **Tool calling**: Anthropic's tool calling is on par with OpenAI; it reliably calls tools when appropriate
- **High quality**: When tools are called, the quality and accuracy are excellent
- **Unique IDs**: Tool call IDs follow format `toolu_01XXXXXXXXXXXXXXXXXXXX`
- **Parallel calls**: Supports multiple tool calls in a single response

### Streaming Behavior
- **Text streaming**: Works well for text-only responses
- **Tool call streaming**: Disabled - tool calls are returned as complete responses
- **Mixed content**: When both text and tool calls are present, only complete responses are returned

### Error Handling
- **"tool_use_id not found"**: Indicates conversation structure doesn't match Anthropic's requirements
- **Authentication errors**: Check API key validity and account credits
- **Rate limiting**: Anthropic has usage limits based on your plan

## Troubleshooting

### Common Issues

1. **Tool calling not working**: 
   - Ensure tools are provided in the `tools` field, not as content parts
   - Check that tool descriptions are clear and specific
   - Anthropic is conservative - try more explicit requests

2. **"tool_use_id not found" errors**: 
   - Verify conversation structure matches the exact pattern above
   - Ensure tool call IDs match between assistant and tool result messages
   - Check that tool results reference the correct `tool_call_id`

3. **API authentication errors**: 
   - Verify your Anthropic API key is correct and active
   - Check that your account has sufficient credits
   - Ensure the key has the required permissions

4. **Model not found errors**: 
   - Verify the model name is correctly spelled
   - Check that the model is available in your region
   - Some models may require special access

5. **Streaming issues**:
   - Text streaming works normally
   - Tool call streaming is disabled by design
   - Mixed content responses are returned complete

### Debug Logging

Enable debug logging to inspect the exact messages being sent to Anthropic:

```go
// Add debug logging in your implementation
logger.Debugf("Sending request to Anthropic: %+v", request)
```

### Environment Variables

The component will automatically use the `ANTHROPIC_API_KEY` environment variable if no `key` is provided in the metadata.

## Usage Tracking

The component provides detailed usage information in responses:

```json
{
  "usage": {
    "promptTokens": 45,      // Input tokens consumed
    "completionTokens": 23,  // Output tokens generated  
    "totalTokens": 68        // Total tokens used
  }
}
```

This helps track API consumption and costs.

## References

- [Anthropic API Documentation](https://docs.anthropic.com/)
- [langchaingo Anthropic Example](https://raw.githubusercontent.com/tmc/langchaingo/refs/heads/main/examples/anthropic-tool-call-example/anthropic-tool-call-example.go)
- [Claude Models Documentation](https://docs.anthropic.com/claude/docs/models-overview)
- [Dapr Conversation Component Spec](../../docs/specs/conversation/)

## Implementation Notes

This component is built on top of the langchaingo library's Anthropic provider. Key considerations:

- **Tool call streaming disabled**: Due to limitations in langchaingo's streaming implementation with tool calls
- **Conservative tool calling**: Anthropic models are more conservative about calling tools than some other providers
- **Usage tracking**: Provides detailed token usage information for cost tracking
- **Caching support**: Optional response caching to reduce API calls and costs

For developers working on this component: always test multi-turn tool calling scenarios and refer to the conformance tests for correct usage patterns. 