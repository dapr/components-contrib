# Anthropic Conversation Component

This component provides conversation capabilities using Anthropic's Claude models through the langchaingo library.

## Overview

The Anthropic conversation component enables natural language interactions with Claude models, including support for:
- Text-based conversations
- Multi-turn tool calling
- Content parts (text, tool calls, tool results)
- Streaming responses

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
    value: "claude-3-haiku-20240307"  # Optional, defaults to claude-3-haiku-20240307
```

### Configuration Parameters

| Parameter | Required | Description | Default |
|-----------|----------|-------------|---------|
| `key` | Yes | Anthropic API key | - |
| `model` | No | Claude model to use | `claude-sonnet-4-20250514` |

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
  "inputs": [
    {
      "role": "user",
      "parts": [
        {
          "type": "text",
          "text": "What's the weather like in San Francisco?"
        },
        {
          "type": "tool_definitions",
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
          ]
        }
      ]
    }
  ]
}
```

## Multi-Turn Tool Calling Requirements

⚠️ **Important**: Anthropic has strict requirements for multi-turn tool calling conversations. The conversation structure must follow a specific pattern for tool calls to work correctly.

### Required Conversation Flow

For multi-turn tool calling, the conversation history must be structured exactly as follows:

1. **User message** with tool definitions
2. **Assistant message** with tool call (created directly from the original response)
3. **Tool result message** with the tool execution result
4. **Follow-up user message**

### Critical Implementation Details

#### Assistant Message Structure

When creating the assistant message that contains tool calls, it's **critical** to preserve the original structure from the LLM response. Do NOT convert tool calls to content parts and then back to langchaingo format, as this loses essential structure that Anthropic's API requires.

**✅ Correct Pattern** (from [langchaingo example](https://raw.githubusercontent.com/tmc/langchaingo/refs/heads/main/examples/anthropic-tool-call-example/anthropic-tool-call-example.go)):

```go
// Create assistant message directly from original response
assistantResponse := llms.MessageContent{
    Role: llms.ChatMessageTypeAI,
    Parts: []llms.ContentPart{
        llms.ToolCall{
            ID:   toolCall.ID,
            Type: toolCall.Type,
            FunctionCall: &llms.FunctionCall{
                Name:      toolCall.FunctionCall.Name,
                Arguments: toolCall.FunctionCall.Arguments,
            },
        },
    },
}
```

**❌ Incorrect Pattern** (causes "tool_use_id not found" errors):

```go
// Don't recreate from converted content parts - this breaks the structure
assistantResponse := llms.MessageContent{
    Role: llms.ChatMessageTypeAI,
    Parts: convertedContentParts, // This loses critical structure
}
```

#### Error: "unexpected tool_use_id found in tool_result blocks"

If you encounter this error:
```
messages.2.content.0: unexpected `tool_use_id` found in `tool_result` blocks: toolu_xxx. 
Each `tool_result` block must have a corresponding `tool_use` block in the previous message.
```

This means:
1. The assistant message (messages[1]) doesn't contain the expected `tool_use` block
2. The conversation structure is not following Anthropic's required pattern
3. The tool call structure was likely corrupted during conversion

#### Debugging Tool Call Issues

To debug tool calling issues:

1. **Check the conversation structure**: Ensure messages follow the exact pattern above
2. **Verify tool call IDs**: The `tool_result` must reference the exact `tool_call_id` from the assistant message
3. **Inspect message JSON**: Enable debug logging to see the exact JSON being sent to Anthropic
4. **Compare with langchaingo example**: Use the official langchaingo Anthropic example as reference

### Example Multi-Turn Conversation

```json
[
  {
    "role": "user",
    "parts": [
      {"type": "text", "text": "What's the weather in Boston?"},
      {"type": "tool_definitions", "tools": [...]}
    ]
  },
  {
    "role": "assistant", 
    "parts": [
      {
        "type": "tool_call",
        "tool_call": {
          "id": "toolu_01xxx",
          "type": "function",
          "function": {
            "name": "get_weather",
            "arguments": "{\"location\":\"Boston\"}"
          }
        }
      }
    ]
  },
  {
    "role": "tool",
    "parts": [
      {
        "type": "tool_result",
        "tool_call_id": "toolu_01xxx",
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
```

## Troubleshooting

### Common Issues

1. **Tool calling not working**: Ensure tool definitions are provided in the first user message
2. **"tool_use_id not found" errors**: Check conversation structure and tool call preservation
3. **API authentication errors**: Verify your Anthropic API key is correct and has sufficient credits
4. **Model not found errors**: Ensure the specified model name is valid and available

### Debug Logging

Enable debug logging to inspect the exact messages being sent to Anthropic:

```go
// Add debug logging in your implementation
fmt.Printf("DEBUG: Sending messages to Anthropic: %+v\n", messages)
```

## References

- [Anthropic API Documentation](https://docs.anthropic.com/)
- [langchaingo Anthropic Example](https://raw.githubusercontent.com/tmc/langchaingo/refs/heads/main/examples/anthropic-tool-call-example/anthropic-tool-call-example.go)
- [Claude Models Documentation](https://docs.anthropic.com/claude/docs/models-overview)

## Implementation Notes

This component is built on top of the langchaingo library's Anthropic provider. The key insight for successful multi-turn tool calling is to preserve the original message structure from the LLM response rather than converting through intermediate formats.

For developers working on this component: always test multi-turn tool calling scenarios and refer to the official langchaingo example for the correct conversation patterns. 