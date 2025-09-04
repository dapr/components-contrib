# Conversation Component Conformance Tests

This directory contains conformance tests for all conversation components, including all langchaingo-based models.

## Available Components

- **echo** - Simple echo component for testing (no configuration needed)
- **openai** - OpenAI GPT models (also supports Azure OpenAI)
- **anthropic** - Anthropic Claude models  
- **googleai** - Google Gemini models
- **mistral** - Mistral AI models
- **huggingface** - HuggingFace models (using OpenAI compatibility layer)
- **ollama** - Local Ollama models
- **bedrock** - AWS Bedrock models

## Running the Tests

To run the conformance tests:

```bash
# Run all conformance tests (will skip components without API keys)
go test -tags conftests ./tests/conformance -run TestConversationConformance -timeout 30s

# Run specific component test
go test -tags conftests ./tests/conformance -run TestConversationConformance/openai -timeout 30s

# Run with verbose output
go test -tags conftests ./tests/conformance -run TestConversationConformance -v -timeout 30s
```

## Environment Variables

The tests will automatically skip components for which the required environment variables are not set. You can either set environment variables directly or use a `.env` file.

### Using a .env file (Recommended)

1. Copy the template file:

```bash
cp env.template .env
```

2. Edit the `.env` file and add your API keys
3. Run the tests - they will automatically load the environment variables from the `.env` file

### Setting Environment Variables Directly

Alternatively, you can set the following environment variables to run the respective tests:

#### Model Configuration (Optional)

You can override the default models used by each component by setting these environment variables:

```bash
export OPENAI_MODEL="gpt-5-nano"           # Default: gpt-5-nano
export AZURE_OPENAI_MODEL="gpt-4.1-nano"   # Default: gpt-4.1-nano
export ANTHROPIC_MODEL="claude-3-5-sonnet-20240620"  # Default: claude-3-5-sonnet-20240620
export GOOGLEAI_MODEL="gemini-1.5-flash"   # Default: gemini-1.5-flash
export MISTRAL_MODEL="open-mistral-7b"     # Default: open-mistral-7b
export HUGGINGFACE_MODEL="deepseek-ai/DeepSeek-R1-Distill-Qwen-32B"  # Default: deepseek-ai/DeepSeek-R1-Distill-Qwen-32B
export OLLAMA_MODEL="llama3.2:latest"      # Default: llama3.2:latest
```

#### OpenAI

```bash
export OPENAI_API_KEY="your_openai_api_key"
```

Get your API key from: https://platform.openai.com/api-keys

### Azure OpenAI

```bash
export AZURE_OPENAI_API_KEY="your_openai_api_key"
export AZURE_OPENAI_ENDPOINT="your_azureopenai_endpoint_here"
export AZURE_OPENAI_API_VERSION="your_azreopenai_api_version_here"
```

Get your configuration values from: https://ai.azure.com/

```bash
export ANTHROPIC_API_KEY="your_anthropic_api_key"
```

Get your API key from: <https://console.anthropic.com/>

#### Google AI

```bash
export GOOGLE_AI_API_KEY="your_google_ai_api_key"
```

Get your API key from: <https://aistudio.google.com/app/apikey>

#### Mistral

```bash
export MISTRAL_API_KEY="your_mistral_api_key"
```

Get your API key from: <https://console.mistral.ai/>

#### HuggingFace

```bash
export HUGGINGFACE_API_KEY="your_huggingface_api_key"
```

Get your API key from: <https://huggingface.co/settings/tokens>

#### AWS Bedrock

```bash
export AWS_ACCESS_KEY_ID="your_aws_access_key"
export AWS_SECRET_ACCESS_KEY="your_aws_secret_key"
export AWS_REGION="us-east-1"  # Optional, defaults to us-east-1
```

Get your credentials from AWS Console

#### Ollama

```bash
export OLLAMA_ENABLED="1"
```

Requires a local Ollama server running with the `llama3.2:latest` model available.

## Test Configuration

Each component has its own configuration file in this directory:

- `echo/echo.yml` - Echo component configuration
- `openai/openai.yml` - OpenAI configuration with gpt-5-nano model
- `anthropic/anthropic.yml` - Anthropic configuration with Claude 3 Haiku
- `googleai/googleai.yml` - Google AI configuration with Gemini 1.5 Flash
- `mistral/mistral.yml` - Mistral configuration with open-mistral-7b
- `huggingface/huggingface.yml` - HuggingFace configuration (uses OpenAI compatibility layer)
- `ollama/ollama.yml` - Ollama configuration with llama3.2:latest
- `bedrock/bedrock.yml` - AWS Bedrock configuration with Claude 3 Haiku

The configurations use cost-effective models where possible to minimize testing costs.

## HuggingFace OpenAI Compatibility Layer

The HuggingFace component uses a workaround due to issues with the native HuggingFace implementation in langchaingo. Instead of using the HuggingFace SDK directly, it uses the OpenAI SDK with HuggingFace's OpenAI-compatible API endpoints.

### How it works

- **Model Selection**: Any HuggingFace model can be used by specifying its full name (e.g., `deepseek-ai/DeepSeek-R1-Distill-Qwen-32B`)
- **Dynamic Endpoints**: The endpoint URL is automatically generated based on the model name using the template: `https://router.huggingface.co/hf-inference/models/{{model}}/v1`
- **Custom Endpoints**: You can override the endpoint by specifying a custom `endpoint` parameter
- **Authentication**: Uses the same HuggingFace API key authentication

### Example Configuration

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
    # Optional: custom endpoint (auto-generated if not specified)
    - name: endpoint
      value: "https://router.huggingface.co/hf-inference/models/microsoft/DialoGPT-medium/v1"
```

This approach provides better reliability and compatibility while maintaining access to the full range of HuggingFace models.

## Notes

- The tests will automatically skip components when required environment variables are not set
- Cost-effective models are used by default to minimize API costs
- HuggingFace uses the OpenAI compatibility layer as a workaround due to langchaingo API issues
- Ollama requires a local server and must be explicitly enabled
- OpenAI component is tested for OpenAI and Azure
- All tests include proper initialization and basic conversation functionality testing 