# Mistral Conversation Component

This component provides access to Mistral AI models through Dapr's conversation API with enhanced tool calling support.

## Key Features

- **Full tool calling support** with parallel tool execution
- **Automatic tool result conversion** for Mistral API compatibility
- **Multi-turn tool calling** with custom workarounds for API limitations
- **Sequential tool processing** to handle complex multi-tool scenarios

## Configuration

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: mistral
spec:
  type: conversation.mistral
  version: v1
  metadata:
    - name: key
      value: "your_mistral_api_key_here"  # or use MISTRAL_API_KEY env var
    - name: model
      value: "open-mistral-7b"  # default
    - name: cacheTTL
      value: "10m"  # optional
```

## Supported Models

- **open-mistral-7b** (default) - Fast and efficient
- **mistral-small-latest** - Balanced performance
- **mistral-medium-latest** - Enhanced capabilities  
- **mistral-large-latest** - Most capable
- **codestral-latest** - Code generation optimized

## Tool Calling

### Basic Tool Definition
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
  ]
}
```

### Tool Result Processing
Tool results are automatically converted to text format for Mistral API compatibility:

```
Input: ToolResultContentPart{Name: "get_weather", Content: "Sunny, 72°F"}
Output: "The get_weather function returned: Sunny, 72°F. Please use this information to respond to the user."
```

## Multi-turn Tool Calling Enhancement

Mistral has API limitations with tool call history. This component implements automatic workarounds:

### Sequential Tool Processing
When multiple tools are requested, they're processed sequentially with progress updates:
```
User: "Get weather, time, and exchange rate for Tokyo"
Assistant: [Calls get_weather] + "I'll call the remaining 2 tool(s) after processing this result."
```

### Fresh Context Pattern
For multi-turn conversations with tool history, the component automatically creates fresh context:
```
Internal: "Based on previous tool execution results: Weather: sunny, 22°C. User's original request was: Get weather and time for Tokyo"
Assistant: Provides comprehensive response using all tool results
```

## Tool Calling Characteristics

- **Tool calling**: Reliable tool calling on par with other major providers
- **Parallel tools**: Processed sequentially due to API limitations (transparent to users)
- **Multi-turn support**: ✅ Enhanced with custom workarounds
- **Error handling**: Automatic conversion of tool errors to text format

## Authentication

Set your API key via environment variable:
```bash
export MISTRAL_API_KEY="your_api_key_here"
```

Or configure in component metadata using the `key` field.

## Testing Multi-turn Tool Calling

Verify the enhanced multi-turn tool calling works:
```bash
source tests/config/conversation/.env && \
MISTRAL_API_KEY=$MISTRAL_API_KEY \
go test -v -tags conftests \
-run "TestConversationConformance/mistral/content_parts/sophisticated_multi-turn_multi-tool" \
./tests/conformance/ -timeout 10m
```

## Troubleshooting

**Tool calls not generated**: Improve tool descriptions and ensure required parameters are specified.

**Rate limiting**: Implement exponential backoff and respect API rate limits.

**Multi-turn issues**: The component automatically handles Mistral's tool call history limitations.

## Limitations

1. **Sequential tool processing**: Multiple tools are called sequentially rather than in parallel
2. **Context window**: Limited by model's context window size  
3. **Rate limits**: Subject to Mistral API rate limits

---

For more details on the conversation component interface, see the [main conversation documentation](../README.md). 