# HuggingFace Conversation Component

This component provides access to HuggingFace models through Dapr's conversation API using an OpenAI compatibility layer.

## Overview

Due to issues with the native HuggingFace implementation in langchaingo, this component uses HuggingFace's OpenAI-compatible API endpoints through the OpenAI SDK. This approach provides better reliability and compatibility while maintaining access to the full range of HuggingFace models.

## How It Works

### Dynamic Endpoint Generation
The component automatically generates the correct API endpoint based on the model name:
- **Template**: `https://router.huggingface.co/hf-inference/models/{{model}}/v1`
- **Example**: For model `microsoft/DialoGPT-medium`, the endpoint becomes:
  `https://router.huggingface.co/hf-inference/models/microsoft/DialoGPT-medium/v1`

### Model Support
Any HuggingFace model that supports the OpenAI-compatible API can be used. Popular options include:
- `microsoft/DialoGPT-medium` - Conversational AI
- `deepseek-ai/DeepSeek-R1-Distill-Qwen-32B` - Advanced reasoning model
- `meta-llama/Llama-2-7b-chat-hf` - Meta's Llama 2 chat model
- `mistralai/Mistral-7B-Instruct-v0.1` - Mistral instruction-following model

## Configuration

### Basic Configuration
```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: huggingface
spec:
  type: conversation.huggingface
  version: v1
  metadata:
    - name: key
      value: "hf_your_api_key_here"
    - name: model
      value: "microsoft/DialoGPT-medium"
```

### Advanced Configuration with Custom Endpoint
```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: huggingface-custom
spec:
  type: conversation.huggingface
  version: v1
  metadata:
    - name: key
      value: "hf_your_api_key_here"
    - name: model
      value: "custom-org/custom-model"
    - name: endpoint
      value: "https://custom-endpoint.com/v1"
    - name: cacheTTL
      value: "10m"
```

## Authentication

1. Get your HuggingFace API key from: https://huggingface.co/settings/tokens
2. Set the `key` parameter in your component configuration

## Parameters

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `key` | Yes | HuggingFace API key | `hf_abc123...` |
| `model` | No | HuggingFace model name | `microsoft/DialoGPT-medium` |
| `endpoint` | No | Custom OpenAI-compatible endpoint | `https://custom.api.com/v1` |
| `cacheTTL` | No | Cache time-to-live | `10m` |

## Usage Example

```go
import (
    "context"
    "github.com/dapr/go-sdk/client"
)

func main() {
    ctx := context.Background()
    c, _ := client.NewClient()
    defer c.Close()

    resp, err := c.InvokeMethod(ctx, "conversation-app", "converse", map[string]interface{}{
        "inputs": []map[string]interface{}{
            {
                "message": "Hello! How can you help me today?",
                "role": "user",
            },
        },
    })
}
```

## Troubleshooting

### Common Issues

1. **402 Payment Required**: Your HuggingFace account needs to have credits or a subscription
2. **401 Unauthorized**: Check that your API key is correct and has the necessary permissions
3. **404 Model Not Found**: Verify the model name is correct and supports OpenAI-compatible API

### Debug Tips

- Check HuggingFace model documentation for OpenAI API compatibility
- Verify your API key has access to the specific model
- Use the HuggingFace Inference API documentation for endpoint details

## Technical Details

### Implementation
- Uses `github.com/tmc/langchaingo/llms/openai` SDK
- Implements the standard Dapr conversation interface
- Supports all OpenAI SDK features (streaming, function calling, etc.)

### Endpoint Template
The component uses string replacement to generate endpoints:
```go
endpoint := strings.Replace("https://router.huggingface.co/hf-inference/models/{{model}}/v1", "{{model}}", model, 1)
```

This allows any HuggingFace model to work without hardcoding specific endpoints.

## Migration from Native HuggingFace SDK

If you were previously using a native HuggingFace implementation, this component provides:
- ✅ Better reliability and error handling
- ✅ Consistent API interface with other conversation components
- ✅ Support for all OpenAI-compatible features
- ✅ Automatic endpoint generation
- ✅ No breaking changes to the Dapr conversation API 