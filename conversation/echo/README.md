# Echo Conversation Provider

The Echo conversation provider is a **test double** designed for reliable testing of conversation components in Dapr. It provides predictable, deterministic responses while mimicking the structure and behavior of real LLM providers.


## 🏗️ Architecture Overview

### Hybrid Input Processing

Echo uses a **hybrid approach** for handling multiple conversation inputs:

```go
// 1. For ECHOING (response generation):
echoMessage := lastUserMessage  // Predictable echoing behavior

// 2. For TOOL MATCHING (functionality):
toolMatchingMessage := buildToolMatchingContext(allUserMessages)  // Enhanced context
```

This design provides:
- **Predictable responses** for testing reliability
- **Enhanced tool calling** with context understanding (direct naming for any tool, keyword matching for common categories)

### Core Components

1. **Message Processing**: Extracts and processes conversation inputs
2. **Tool Matching Engine**: Tool selection via direct naming (any tool) or keyword matching (8 categories)
3. **Parameter Generation**: Schema-aware argument generation for tools
4. **Streaming Support**: Chunk-based response streaming
5. **Usage Tracking**: Token counting and usage metrics

## 📋 Behavior Specification

### Basic Echo Behavior

#### Single Input
```go
Input:  "Hello, world!"
Output: "Hello, world!"  // Exact echo
```

#### Multiple Inputs
```go
Inputs: [
    {Message: "First message", Role: "user"},
    {Message: "Assistant response", Role: "assistant"},
    {Message: "Final message", Role: "user"}
]
Output: "Final message"  // Always echoes the LAST user message
```

### Tool Calling Behavior

#### Tool Detection
Echo uses multiple strategies to detect tool calls from natural language:

```go
// Direct tool name matching (works with ANY tool)
"call send_email" → calls send_email tool
"use my_custom_function" → calls my_custom_function tool

// Keyword-based matching (limited to 8 predefined categories)
"send an email" → calls send_email tool
"check the weather" → calls get_weather tool

// Case-agnostic matching (all naming conventions supported)
"Send Email", "send_email", "sendEmail" → all match send_email tool
```

#### Parameter Generation
Echo generates realistic parameters based on tool schemas:

```go
// String parameters
"location": "San Francisco"

// Number parameters  
"temperature": 42
"count": 42

// Boolean parameters
"enabled": true

// Object parameters
"address": {"street": "123 Main St", "city": "San Francisco"}
```

#### Parallel Tool Calling
Echo can call multiple tools simultaneously:

```go
Input: "Get weather and time for San Francisco and calculate 10+5"
Tools Called: [get_weather, get_time, calculate]  // All in parallel
```

#### Tool Call ID Generation
Echo generates unique, deterministic tool call IDs using a timestamp-based approach:

```go
// Format: call_echo_{nanosecond_timestamp + index}
// Examples:
"call_echo_1703123456789012345"  // First tool call
"call_echo_1703123456789012346"  // Second tool call (same request)
"call_echo_1703123456789012347"  // Third tool call (same request)
```

**Key features:**
- **Unique IDs**: Each tool call gets a unique identifier, even in parallel calls
- **Deterministic**: Same execution order produces same IDs (for testing)
- **OpenAI-compatible**: Follows similar format to real LLM providers
- **Streaming support**: Works identically for both regular and streaming requests

### Response Structure

Echo perfectly mimics real LLM response structure:

```go
type ConversationResponse struct {
    Outputs: []ConversationOutput  // Always exactly 1 output
    Usage: *Usage                  // Token counting
    ConversationContext: string    // Preserves context
}

type ConversationOutput struct {
    Result: string                 // Echo text or empty if tool calls
    ToolCalls: []ToolCall         // Function calls (if any)
    FinishReason: string          // "stop" or "tool_calls"
}
```

### FinishReason Values

Echo uses identical FinishReason values to OpenAI:

- `"stop"`: Normal conversation response
- `"tool_calls"`: Response contains tool calls

### Token Counting

Echo provides realistic token estimation:

```go
// Input tokens: ~1 token per 4 characters (minimum 1)
inputTokens := max(1, len(message)/4)

// Output tokens: Same calculation for response
outputTokens := max(1, len(response)/4)

// Total tokens: input + output
totalTokens := inputTokens + outputTokens
```

## 🔧 Tool Calling Features

### Dynamic Tool Support

Echo can **execute any tool schema** provided by users and has **flexible tool detection**:

```go
// Echo can process ANY tool schema
tools := []Tool{
    {Name: "custom_function", ...},
    {Name: "another_tool", ...},
    {Name: "third_tool", ...},
}
```

**Tool Detection Methods:**
- **Direct naming**: "call my_custom_tool" → calls `my_custom_tool` ✅ Works with ANY tool
- **Explicit phrases**: "use send_email" → calls `send_email` ✅ Works with ANY tool  
- **Keyword matching**: "check the weather" → calls `get_weather` ⚠️ Limited to 8 categories

**Keyword categories:** email, weather, time, calendar, user, database, file, calculate

### Case-Agnostic Tool Matching

Echo handles all naming conventions:

- **snake_case**: `send_email`
- **camelCase**: `sendEmail`  
- **PascalCase**: `SendEmail`
- **kebab-case**: `send-email`
- **space-separated**: `send email`

```go
// All of these match a tool named "sendEmail":
"please sendEmail to the team"
"please send_email to the team"  
"please send-email to the team"
"please send email to the team"
```

### Schema-Aware Parameter Generation

Echo analyzes tool parameter schemas and generates appropriate values:

```json
{
  "type": "object",
  "properties": {
    "email": {"type": "string", "format": "email"},
    "subject": {"type": "string"},
    "priority": {"type": "integer", "minimum": 1, "maximum": 5},
    "send_immediately": {"type": "boolean"}
  }
}
```

Generated parameters:
```json
{
  "email": "test@example.com",
  "subject": "Test email subject", 
  "priority": 42,
  "send_immediately": true
}
```

## 🏃‍♂️ Streaming Support

### Streaming Behavior

Echo supports streaming with realistic word-based chunk generation:

```go
message := "Hello, world!"
chunks := ["Hello", " world!"]  // Word-based chunking
```

**Note**: Echo currently only streams text content. Tool calls are returned as complete structures in the final response, not streamed incrementally. This matches the behavior of most real LLM providers where tool calls need to be complete and valid JSON.



## 🔄 Comparison with Real LLMs

### Similarities (Structural Compatibility)

| Aspect | Echo | OpenAI | Status |
|--------|------|--------|---------|
| Response Structure | ✅ | ✅ | Identical |
| Tool Calling Format | ✅ | ✅ | Identical |
| FinishReason Values | ✅ | ✅ | Identical |
| Streaming Protocol | ✅ | ✅ | Identical |
| Usage Tracking | ✅ | ✅ | Identical |

### Differences (Behavioral)

| Aspect | Echo | OpenAI | Purpose |
|--------|------|--------|---------|
| Response Logic | Echoes last user message | Contextual AI response | Predictable testing |
| Input Processing | Last message + tool context | Full conversation analysis | Test reliability |
| Tool Selection | Pattern matching | AI reasoning | Deterministic results |
| Parameter Generation | Schema-based defaults | Context-aware values | Consistent testing |

## 🚀 Usage Examples

### Basic Echo

```go
req := &ConversationRequest{
    Inputs: []ConversationInput{
        {Message: "Hello Echo!", Role: "user"},
    },
}

resp, err := echo.Converse(ctx, req)
// resp.Outputs[0].Result == "Hello Echo!"
```

### Tool Calling

```go
tools := []Tool{
    {
        Type: "function",
        Function: ToolFunction{
            Name: "get_weather",
            Description: "Get weather information",
            Parameters: map[string]any{
                "type": "object",
                "properties": map[string]any{
                    "location": {"type": "string"},
                },
            },
        },
    },
}

req := &ConversationRequest{
    Inputs: []ConversationInput{
        {
            Message: "What's the weather in San Francisco?",
            Role: "user",
            Tools: tools,
        },
    },
}

resp, err := echo.Converse(ctx, req)
// resp.Outputs[0].ToolCalls[0].Function.Name == "get_weather"
// resp.Outputs[0].FinishReason == "tool_calls"
```

### Streaming

```go
var chunks []string
streamFunc := func(ctx context.Context, chunk []byte) error {
    chunks = append(chunks, string(chunk))
    return nil
}

resp, err := echo.ConverseStream(ctx, req, streamFunc)
// chunks contains multiple parts of the response
// strings.Join(chunks, "") == full response
```

## 🔧 Configuration

Echo requires minimal configuration:

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: echo-conversation
spec:
  type: conversation.echo
  version: v1
  metadata: []  # No configuration required
```

## 🔍 Debugging and Troubleshooting

### Common Issues

1. **Tool not detected**: Check tool name variations and keywords
2. **Unexpected parameters**: Review tool schema definition
3. **Wrong echo response**: Verify you're checking the last user message
4. **Token count mismatch**: Remember 1 token ≈ 4 characters

## 🤝 Contributing

When modifying Echo, remember:

1. **Maintain predictability**: Same input must produce same output
2. **Preserve compatibility**: Don't break existing test expectations  
3. **Document changes**: Update this README with behavior changes
5. **Consider tool calling**: Ensure features work with function calling
