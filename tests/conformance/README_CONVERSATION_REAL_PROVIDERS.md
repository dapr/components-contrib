# Conversation Real Provider Tests

The conversation component includes special **real provider tests** that validate tool calling and streaming functionality against actual LLM providers. These tests require API keys and make real API calls.

> **Note**: These tests are **not part of the regular CI pipeline** and are intended for local development and validation. They require external API keys and make billable API calls.

## 🎯 Purpose

These tests validate that:
- **Tool calling** works correctly with real LLM providers
- **Streaming responses** function properly
- **Multi-step conversations** handle tool results correctly
- **Parallel tool calling** executes as expected
- **Content quality** meets standards across providers
- **Usage information** is properly tracked
- **Error handling** works across provider implementations

## 🏗️ Environment Setup

### 1. Create Environment File

Create a `.env` file in the `tests/config/conversation/` directory:

```bash
# Navigate to the conversation config directory
cd tests/config/conversation/

# Copy the template and customize
cp env.template .env
```

### 2. Add API Keys

Edit `.env` and add your API keys for the providers you want to test:

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

### 3. Secure the File

Ensure the environment file is not committed to version control:

```bash
# The .env file is already gitignored in tests/config/conversation/
# Verify with: git status
```

## 🚀 Running Tests

### Run All Real Provider Tests

```bash
go test -v -tags=conftests ./tests/conformance -run=TestConversationConformance -timeout=15m
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
```

### Run Fast Local Tests

```bash
# Echo provider only (local, no API keys needed)
go test -v -tags=conftests ./tests/conformance -run=TestConversationConformance/echo -timeout=2m
```

## 🧪 Test Features Covered

The real provider tests validate:

- **✅ Basic tool calling**: Single tool invocation with proper argument generation
- **✅ Multiple tools**: Multiple tools provided in one request
- **✅ Multi-step flows**: Tool calls → tool results → final response integration
- **✅ Parallel tool calling**: Multiple tools called simultaneously with unique IDs
- **✅ Streaming**: Real-time response streaming with chunk validation
- **✅ Streaming + tool calling**: Tool calling combined with streaming responses
- **✅ Content validation**: Streaming content quality, consistency, and completeness
- **✅ Usage tracking**: Token usage information (prompt, completion, total)
- **✅ Error handling**: Missing API keys, rate limiting, authentication failures
- **✅ Content parts**: Modern content part system with tool calls and results
- **✅ Multi-turn conversations**: Conversation history and context management

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

### ✅ Mistral
- **✅ Good tool calling**: Reliable tool execution
- **✅ Fresh context approach**: Converts tool results to text for multi-turn
- **✅ Streaming**: Works well for text responses  
- **⚠️ Streaming + tools**: Skipped due to API matching requirements
- **✅ Usage tracking**: Provides usage information
- **✅ Parallel tools**: Supports multiple tool calls

### ⚠️ HuggingFace
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

### Streaming Test Success

```bash
=== RUN   TestConversationConformance/openai/content_parts/sophisticated_streaming_multi-turn_multi-tool
    conversation.go:1022: 🔄 TURN 1: User asks for multiple pieces of information (Streaming)
    conversation.go:1099: ℹ️ Turn 1 generated tool calls without text - streaming chunks optional (provider-dependent)
    conversation.go:1167: 📦 Turn 2 Chunk: Here's the information for your trip to Tokyo:
    conversation.go:1216: 🎯 SUCCESS: Provider streaming response incorporates data from multiple tool calls!
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

### Provider Availability
- **Service downtime**: Tests may fail during provider maintenance
- **New models**: Provider model names may change over time
- **Feature updates**: Provider capabilities evolve (tool calling, streaming)
- **Rate limits**: Each provider has different rate limiting policies

## 🔧 Troubleshooting

### Missing API Keys
**Symptoms**: Tests skip with "API key not found" message
**Solutions**:
- Verify `.env` file exists in `tests/config/conversation/`
- Check API key names match exactly the template
- Ensure API keys are valid and active

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

### Tool Calling Issues
**Symptoms**: Tools not called when expected
**Solutions**:
- Review provider-specific behavior (some are more conservative)
- Check test prompt clarity and tool descriptions
- Verify tool schemas are correctly formatted

### Streaming Issues
**Symptoms**: Streaming tests fail or no chunks received
**Solutions**:
- Check if provider supports streaming
- Verify streaming is enabled in provider configuration
- Note that tool call generation often doesn't stream (this is normal)

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

## 🛠️ Recent Improvements

### Fixed Issues (2025)
- **✅ Usage Type Mismatch**: Fixed `uint64` vs `int32` comparison in usage tracking tests
- **✅ Streaming Expectations**: Made streaming requirements more realistic for tool calling scenarios  
- **✅ Provider Coverage**: Extended testing to support 9 providers (was 3)
- **✅ Test Reliability**: Improved test stability and timeout handling

### Enhanced Features
- **✅ Content Parts System**: Full validation of modern content part interfaces
- **✅ Multi-Provider Support**: Comprehensive testing across all major LLM providers
- **✅ Tool Call Validation**: Improved validation of tool call IDs and argument formats
- **✅ Streaming Intelligence**: Smart streaming validation that understands provider differences

## 🤝 Contributing

When modifying these tests:

1. **Maintain provider neutrality**: Tests should work across providers
2. **Document provider differences**: Note expected behavioral variations
3. **Update timeouts appropriately**: Real providers need longer timeouts
4. **Consider API costs**: Minimize unnecessary API calls in tests
5. **Test with multiple providers**: Ensure changes work across all supported providers
6. **Update documentation**: Keep this README current with changes

## 📚 Related Documentation

- [Main Conformance Tests README](./README.md)
- [Echo Provider Documentation](../../conversation/echo/README.md)
- [Conversation Component Interface](../../conversation/README.md)
- [Environment Template](../config/conversation/env.template)

---

*These real provider tests ensure that Dapr's conversation components work correctly with actual LLM providers in production scenarios. Use them for validation and debugging, but rely on Echo provider tests for regular development.* 