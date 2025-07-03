# Conversation Component Conformance Tests

This directory contains conformance tests for all conversation components, including comprehensive real provider tests that validate tool calling and streaming functionality against actual LLM providers.

> **Note**: Real provider tests require external API keys and make billable API calls. They are **not part of the regular CI pipeline** and are intended for local development and validation.

## 🎯 Purpose

These tests validate that:
- **Tool calling** works correctly with real LLM providers
- **Streaming responses** function properly
- **Multi-step conversations** handle tool results correctly
- **Parallel tool calling** executes as expected
- **Content quality** meets standards across providers
- **Usage information** is properly tracked
- **Error handling** works across provider implementations
- **Content parts** system functions correctly
- **Multi-turn conversations** maintain context properly

## 📋 Available Components

- **echo** - Simple echo component for testing (no configuration needed)
- **openai** - OpenAI GPT models
- **anthropic** - Anthropic Claude models  
- **googleai** - Google Gemini models
- **mistral** - Mistral AI models
- **huggingface** - HuggingFace models (using OpenAI compatibility layer)
- **deepseek** - DeepSeek models (using OpenAI compatibility layer)
- **ollama** - Local Ollama models
- **bedrock** - AWS Bedrock models

## 🏗️ Environment Setup

### Method 1: Using .env File (Recommended)

1. **Create environment file** (both locations are gitignored for security):
```bash
# Navigate to this directory
cd tests/config/conversation/

# Option A: Direct .env file (gitignored via **/.env pattern)
cp env.template .env

# Option B: Use .local folder for extra organization
mkdir -p .local
cp env.template .local/.env
```

2. **Edit your `.env` file** and add your API keys:
```bash
# OpenAI API Key - Get from https://platform.openai.com/api-keys
OPENAI_API_KEY=your_openai_api_key_here

# Anthropic API Key - Get from https://console.anthropic.com/
ANTHROPIC_API_KEY=your_anthropic_api_key_here

# Google AI API Key - Get from https://aistudio.google.com/app/apikey
GOOGLE_AI_API_KEY=your_google_ai_api_key_here

# Mistral API Key - Get from https://console.mistral.ai/
MISTRAL_API_KEY=your_mistral_api_key_here

# HuggingFace API Key - Get from https://huggingface.co/settings/tokens
HUGGINGFACE_API_KEY=your_huggingface_api_key_here

# Deepseek API Key - Get from https://platform.deepseek.com/api_keys
DEEPSEEK_API_KEY=your_deepseek_api_key_here

# AWS Credentials for Bedrock - Get from AWS Console
AWS_ACCESS_KEY_ID=your_aws_access_key_here
AWS_SECRET_ACCESS_KEY=your_aws_secret_key_here
AWS_REGION=us-east-1

# Ollama - Set to 1 if you have a local Ollama server running
# OLLAMA_ENABLED=1
```

3. **Verify security**:
```bash
# Both .env files and .local/ folder are gitignored for security
# Verify your credentials won't be committed: git status
```

> **🔒 Security Note**: `.env` files are automatically gitignored via the `**/.env` pattern, and the `.local/` folder is also gitignored. Both approaches prevent accidentally committing sensitive credentials to version control.

### Method 2: Direct Environment Variables

Alternatively, export environment variables directly in your shell before running tests.

## 🚀 Running Tests

### Run All Provider Tests

```bash
# Run all conformance tests (skips components without API keys)
go test -v -tags=conftests ./tests/conformance -run=TestConversationConformance -timeout=15m

# Run with shorter timeout for faster feedback
go test -v -tags=conftests ./tests/conformance -run=TestConversationConformance -timeout=5m
```

### Run Specific Provider Tests

```bash
# OpenAI only
go test -v -tags=conftests ./tests/conformance -run=TestConversationConformance/openai -timeout=5m

# Google AI only  
go test -v -tags=conftests ./tests/conformance -run=TestConversationConformance/googleai -timeout=5m

# Anthropic only
go test -v -tags=conftests ./tests/conformance -run=TestConversationConformance/anthropic -timeout=5m

# Mistral only
go test -v -tags=conftests ./tests/conformance -run=TestConversationConformance/mistral -timeout=5m

# HuggingFace only
go test -v -tags=conftests ./tests/conformance -run=TestConversationConformance/huggingface -timeout=5m

# DeepSeek only
go test -v -tags=conftests ./tests/conformance -run=TestConversationConformance/deepseek -timeout=5m

# AWS Bedrock only
go test -v -tags=conftests ./tests/conformance -run=TestConversationConformance/bedrock -timeout=5m

# Echo provider only (local, no API keys needed)
go test -v -tags=conftests ./tests/conformance -run=TestConversationConformance/echo -timeout=2m
```

### Run Specific Test Scenarios

```bash
# Test only streaming functionality
go test -v -tags=conftests ./tests/conformance -run=TestConversationConformance/.*/streaming -timeout=5m

# Test only tool calling
go test -v -tags=conftests ./tests/conformance -run=TestConversationConformance/.*/tool_calling -timeout=5m

# Test only parallel tool calling
go test -v -tags=conftests ./tests/conformance -run=TestConversationConformance/.*/parallel -timeout=5m

# Test usage information tracking
go test -v -tags=conftests ./tests/conformance -run=TestConversationConformance/.*/usage -timeout=3m

# Test multi-turn conversations
go test -v -tags=conftests ./tests/conformance -run=TestConversationConformance/.*/multi-turn -timeout=5m
```

## 🧪 Test Features Covered

The conformance tests validate:

- **✅ Basic functionality** - Component initialization and basic conversation
- **✅ Basic tool calling** - Single tool invocation with proper argument generation
- **✅ Multiple tools** - Multiple tools provided in one request
- **✅ Multi-step flows** - Tool calls → tool results → final response integration
- **✅ Parallel tool calling** - Multiple tools called simultaneously with unique IDs
- **✅ Streaming** - Real-time response streaming with chunk validation
- **✅ Streaming + tool calling** - Tool calling combined with streaming responses
- **✅ Content validation** - Streaming content quality, consistency, and completeness
- **✅ Usage tracking** - Token usage information (prompt, completion, total)
- **✅ Error handling** - Missing API keys, rate limiting, authentication failures
- **✅ Content parts** - Modern content part system with tool calls and results
- **✅ Multi-turn conversations** - Conversation history and context management
- **✅ Finish reasons** - Proper completion reason reporting

## 📊 Provider-Specific Behavior

### ✅ OpenAI
- **✅ Excellent tool calling**: Reliable tool selection and execution
- **✅ Granular streaming**: Many small chunks (word-by-word)  
- **✅ Unique tool call IDs**: Properly formatted identifiers
- **✅ Multi-step flows**: Handles conversation history well
- **✅ Usage tracking**: Accurate token counting
- **✅ Parallel tools**: Supports multiple simultaneous tool calls

### ✅ Google AI (OpenAI Compatibility Layer)
- **✅ Excellent tool calling**: Uses OpenAI compatibility endpoint
- **✅ Efficient streaming**: Fewer, larger chunks (sentence-based)
- **⚠️ Simple tool call IDs**: Basic numeric IDs (langchaingo limitation)
- **✅ Multi-step flows**: Works correctly via compatibility layer
- **✅ Usage tracking**: Provides token usage information

### ⚠️ Anthropic
- **⚠️ Conservative tool calling**: Often chooses not to call tools initially
- **✅ High quality responses**: When tools are called, quality is excellent
- **✅ Streaming**: Works well for text responses
- **⚠️ Streaming + tools**: Skipped due to known langchaingo limitations
- **✅ Multi-step flows**: Good conversation handling
- **✅ Unique tool call IDs**: Properly formatted identifiers
- **✅ Usage tracking**: Standardized usage extraction (fixed in 2025)

### ✅ Mistral
- **✅ Good tool calling**: Reliable tool execution
- **✅ Fresh context approach**: Converts tool results to text for multi-turn
- **✅ Streaming**: Works well for text responses  
- **⚠️ Streaming + tools**: Skipped due to API matching requirements
- **✅ Usage tracking**: Provides usage information
- **✅ Parallel tools**: Supports multiple tool calls
- **✅ Test coverage**: Significantly improved (59.3% coverage in 2025)

### ⚠️ HuggingFace (OpenAI Compatibility Layer)
- **✅ Basic tool calling**: Works with supported models
- **❌ No streaming**: Most models don't support streaming
- **⚠️ Timeout issues**: Can be slow/unreliable
- **✅ Multi-step flows**: Handles conversation history
- **⚠️ Limited parallel tools**: Often calls tools sequentially

### ⚠️ DeepSeek  
- **✅ Good tool calling**: When working, tool calls are accurate
- **✅ Streaming**: Supports streaming responses
- **⚠️ Timeout issues**: Can be slow or timeout on complex requests
- **✅ Usage tracking**: Provides token usage
- **⚠️ Rate limiting**: Strict rate limits

### ✅ AWS Bedrock
- **✅ Enterprise-grade**: Reliable and scalable
- **✅ Tool calling**: Supports function calling
- **✅ Streaming**: Good streaming support
- **✅ Usage tracking**: Detailed usage metrics
- **⚠️ Setup complexity**: Requires AWS credentials and proper IAM

### ⚡ Echo (Test Provider)
- **✅ Perfect for development**: Fast, local, deterministic
- **✅ All features**: Supports all test scenarios
- **✅ No API costs**: Local mock implementation
- **✅ Debugging**: Great for testing test logic itself

## 🔧 Component Configuration

Each component has its own configuration file in the respective subdirectory:
- `echo/echo.yml` - Echo component (no special configuration needed)
- `openai/openai.yml` - OpenAI configuration
- `anthropic/anthropic.yml` - Anthropic configuration
- `googleai/googleai.yml` - Google AI configuration
- `mistral/mistral.yml` - Mistral configuration
- `huggingface/huggingface.yml` - HuggingFace configuration
- `deepseek/deepseek.yml` - DeepSeek configuration
- `ollama/ollama.yml` - Ollama configuration
- `bedrock/bedrock.yml` - AWS Bedrock configuration

## 🛠️ HuggingFace OpenAI Compatibility Layer

The HuggingFace component uses the OpenAI SDK with HuggingFace's OpenAI-compatible API endpoints due to issues with the native HuggingFace implementation in langchaingo.

### How it works:
- **Model Selection**: Any HuggingFace model can be used by specifying its full name (e.g., `deepseek-ai/DeepSeek-R1-Distill-Qwen-32B`)
- **Dynamic Endpoints**: The endpoint URL is automatically generated using the template: `https://router.huggingface.co/hf-inference/models/{{model}}/v1`
- **Custom Endpoints**: You can override the endpoint by specifying a custom `endpoint` parameter
- **Authentication**: Uses the same HuggingFace API key authentication

### Example Configuration:
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

## 📱 Example Test Output

### Successful Test Run
```bash
=== RUN   TestConversationConformance/openai/content_parts/tool_calling_with_content_parts
    conversation.go:298: Component generated 1 tool calls across 1 outputs
=== RUN   TestConversationConformance/openai/content_parts/parallel_tool_calling_with_content_parts  
    conversation.go:373: Component generated 2 tool calls for parallel request
    conversation.go:390: Verified 2 parallel tool calls have unique IDs
=== RUN   TestConversationConformance/openai/content_parts/multi-turn_tool_calling
    conversation.go:447: Step 1: Making initial request with tool definitions
    conversation.go:496: Step 1 completed: AI generated tool call with ID: call_abc123
    conversation.go:595: Step 2 completed: Multi-turn tool calling successful
```

### Test Skipping (Missing API Key)
```bash
=== RUN   TestConversationConformance/openai
--- SKIP: TestConversationConformance/openai (0.00s)
    conversation_test.go:148: Skipping OpenAI conformance test: OPENAI_API_KEY environment variable not set
```

## ⚠️ Important Considerations

### API Costs and Usage
- **Real API calls**: These tests make actual requests to LLM providers
- **API quotas**: Will consume your API credits/quotas
- **Rate limiting**: May hit provider rate limits during extensive testing
- **Billing**: Costs will appear on your provider accounts

### Test Duration
- **Longer execution**: Real provider tests take 2-10 minutes per provider
- **Network dependency**: Requires stable internet connection
- **Provider latency**: Response times vary by provider and region
- **Timeout considerations**: Some providers (DeepSeek, HuggingFace) may timeout

### Security
- **API key protection**: Never commit API keys to version control
- **Local testing only**: Not suitable for CI/CD without secure secret management
- **Key rotation**: Regularly rotate API keys as security best practice
- **Environment isolation**: Use separate API keys for testing vs production

## 🔧 Troubleshooting

### Missing API Keys
**Symptoms**: Tests skip with "API key not found" message
**Solutions**:
- Verify `.env` file exists in `tests/config/conversation/` or `tests/config/conversation/.local/`
- Check API key names match exactly the template (use `env.template` as reference)
- Ensure API keys are valid and active
- Tests automatically check both `.local/.env` and `.env` locations

### Authentication Errors
**Symptoms**: "authentication failed" or "invalid API key" errors
**Solutions**:
- Verify API keys are copied correctly (no extra spaces)
- Check API key permissions and capabilities
- Ensure API keys support required features (tool calling, streaming)

### Rate Limiting
**Symptoms**: "rate limit exceeded" or "429" errors
**Solutions**:
- Reduce test frequency or add delays
- Use different API keys for different test runs
- Check provider-specific rate limits and quotas

### Timeout Errors
**Symptoms**: Tests fail with timeout errors
**Solutions**:
- Increase timeout values: `-timeout=15m`
- Check network connectivity
- Verify provider service status
- Test individual providers to isolate issues

## 🛠️ Recent Improvements (2025)

### Fixed Issues
- **✅ Usage Type Mismatch**: Fixed `uint64` vs `int32` comparison in usage tracking tests
- **✅ Streaming Expectations**: Made streaming requirements more realistic for tool calling scenarios  
- **✅ Provider Coverage**: Extended testing to support 9 providers (was 3)
- **✅ Test Reliability**: Improved test stability and timeout handling
- **✅ Anthropic Standardization**: Moved from custom usage extraction to standardized approach
- **✅ Mistral Test Coverage**: Improved from 31.1% to 59.3% coverage

### Enhanced Features
- **✅ Content Parts System**: Full validation of modern content part interfaces
- **✅ Multi-Provider Support**: Comprehensive testing across all major LLM providers
- **✅ Tool Call Validation**: Improved validation of tool call IDs and argument formats
- **✅ Streaming Intelligence**: Smart streaming validation that understands provider differences
- **✅ Usage Extraction**: Standardized usage extraction across all providers

## 🔄 Integration with Regular Conformance Tests

These real provider tests complement the regular conversation conformance tests:

- **Regular tests**: Use Echo provider for fast, deterministic testing
- **Real provider tests**: Validate actual LLM provider integration
- **Both needed**: Echo for development speed, real providers for production validation

## 🎯 When to Run These Tests

### Development Scenarios
- **Before releasing** conversation component changes
- **Testing new tool calling features** against real providers
- **Validating streaming improvements** with actual LLM APIs
- **Debugging provider-specific issues** in production
- **Verifying new provider integrations**

### Not Recommended For
- **Regular CI/CD pipelines** (use Echo provider instead)
- **Unit testing** (too slow and expensive)
- **Frequent development cycles** (API costs add up)
- **Automated testing** without proper API key management

## 🤝 Contributing

When modifying these tests:

1. **Maintain provider neutrality**: Tests should work across providers
2. **Document provider differences**: Note expected behavioral variations
3. **Update timeouts appropriately**: Real providers need longer timeouts
4. **Consider API costs**: Minimize unnecessary API calls in tests
5. **Test with multiple providers**: Ensure changes work across all supported providers
6. **Update documentation**: Keep this README current with changes

## 📚 Related Documentation

- [Main Conformance Tests README](../../conformance/README.md)
- [Echo Provider Documentation](../../../conversation/echo/README.md)
- [Conversation Component Interface](../../../conversation/README.md)
- [Environment Template](./env.template)

---

*These conformance tests ensure that Dapr's conversation components work correctly with actual LLM providers in production scenarios. Use them for validation and debugging, but rely on Echo provider tests for regular development.* 