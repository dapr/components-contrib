# Conversation Real Provider Tests

The conversation component includes special **real provider tests** that validate tool calling and streaming functionality against actual LLM providers (OpenAI, Anthropic, Google AI). These tests require API keys and make real API calls.

> **Note**: These tests are **not part of the regular CI pipeline** and are intended for local development and validation. They require external API keys and make billable API calls.

## 🎯 Purpose

These tests validate that:
- **Tool calling** works correctly with real LLM providers
- **Streaming responses** function properly
- **Multi-step conversations** handle tool results correctly
- **Parallel tool calling** executes as expected
- **Content quality** meets standards across providers

## 🏗️ Environment Setup

### 1. Create Environment File

Create a `.local/.env` file in the repository root directory:

```bash
# Create the directory if it doesn't exist
mkdir -p .local

# Create the environment file
touch .local/.env
```

### 2. Add API Keys

Add your API keys to `.local/.env` in the following format:

```bash
# OpenAI API Key (required for OpenAI tests)
OPENAI_API_KEY=sk-your-openai-api-key-here

# Anthropic API Key (required for Anthropic tests)
ANTHROPIC_API_KEY=your-anthropic-api-key-here

# Google AI API Key (required for Google AI tests)
GOOGLE_AI_API_KEY=your-google-ai-api-key-here
```

### 3. Secure the File

Ensure the environment file is not committed to version control:

```bash
# The .local/ directory should already be in .gitignore
echo ".local/" >> .gitignore
```

## 🚀 Running Tests

### Run All Real Provider Tests

```bash
go test -v -tags=conftests ./tests/conformance -run=TestRealProvidersToolCalling -timeout=10m
```

### Run Specific Provider Tests

```bash
# OpenAI only
go test -v -tags=conftests ./tests/conformance -run=TestRealProvidersToolCalling/openai -timeout=5m

# Google AI only  
go test -v -tags=conftests ./tests/conformance -run=TestRealProvidersToolCalling/googleai -timeout=5m

# Anthropic only
go test -v -tags=conftests ./tests/conformance -run=TestRealProvidersToolCalling/anthropic -timeout=5m
```

### Run Specific Test Scenarios

```bash
# Test only streaming functionality
go test -v -tags=conftests ./tests/conformance -run=TestRealProvidersToolCalling/.*/streaming -timeout=5m

# Test only tool calling
go test -v -tags=conftests ./tests/conformance -run=TestRealProvidersToolCalling/.*/tool_calling -timeout=5m

# Test only parallel tool calling
go test -v -tags=conftests ./tests/conformance -run=TestRealProvidersToolCalling/.*/parallel -timeout=5m

# Test streaming content validation
go test -v -tags=conftests ./tests/conformance -run=TestRealProvidersToolCalling/.*/streaming_content_validation -timeout=3m
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
- **✅ Error handling**: Missing API keys, rate limiting, authentication failures

## 📊 Provider-Specific Behavior

### OpenAI
- ✅ **Excellent tool calling**: Reliable tool selection and execution
- ✅ **Granular streaming**: Many small chunks (word-by-word)
- ✅ **Unique tool call IDs**: Properly formatted identifiers
- ✅ **Multi-step flows**: Handles conversation history well

### Google AI (OpenAI Compatibility Layer)
- ✅ **Excellent tool calling**: Uses OpenAI compatibility endpoint
- ✅ **Efficient streaming**: Fewer, larger chunks (sentence-based)
- ⚠️ **Empty tool call IDs**: Limitation of langchaingo implementation
- ✅ **Multi-step flows**: Works correctly via compatibility layer

### Anthropic
- ⚠️ **Conservative tool calling**: Often chooses not to call tools
- ⚠️ **Streaming issues**: Some streaming errors with langchaingo
- ✅ **High quality responses**: When tools are called, quality is excellent
- ⚠️ **Multi-step limitations**: More conservative approach

## 📱 Example Test Output

### Successful Test Run

```bash
=== RUN   TestRealProvidersToolCalling/openai/basic_tool_calling
    conversation_real_providers_test.go:221: ✅ openai called 1 tool(s)
    conversation_real_providers_test.go:234: Tool call: get_weather (ID: call_abc123) with args: {"location":"San Francisco"}
    conversation_real_providers_test.go:246: Usage: 66 total tokens (50 prompt + 16 completion)

=== RUN   TestRealProvidersToolCalling/googleai/streaming_content_validation  
    conversation_real_providers_test.go:513: 📡 googleai received 2 streaming chunks
    conversation_real_providers_test.go:524: Chunk 1: "Artificial intelligence (AI) is a field of computer science..."
    conversation_real_providers_test.go:546: ✅ Streamed content matches final result perfectly

=== RUN   TestRealProvidersToolCalling/openai/parallel_tool_calling
    conversation_real_providers_test.go:463: ✅ openai successfully performed parallel tool calling (3 tools)
    conversation_real_providers_test.go:474: Tool call: get_weather (ID: call_abc123)
    conversation_real_providers_test.go:474: Tool call: get_time (ID: call_def456)
    conversation_real_providers_test.go:474: Tool call: calculate (ID: call_ghi789)
```

### Test Skipping (Missing API Key)

```bash
=== RUN   TestRealProvidersToolCalling/openai
--- SKIP: TestRealProvidersToolCalling/openai (0.00s)
    conversation_real_providers_test.go:148: OpenAI API key not found in .local/.env
```

## ⚠️ Important Considerations

### API Costs and Usage
- **Real API calls**: These tests make actual requests to LLM providers
- **API quotas**: Will consume your API credits/quotas
- **Rate limiting**: May hit provider rate limits during extensive testing
- **Billing**: Costs will appear on your provider accounts

### Test Duration
- **Longer execution**: Real provider tests take 2-5 minutes per provider
- **Network dependency**: Requires stable internet connection
- **Provider latency**: Response times vary by provider and region

### Security
- **API key protection**: Never commit API keys to version control
- **Local testing only**: Not suitable for CI/CD without secure secret management
- **Key rotation**: Regularly rotate API keys as security best practice

### Provider Availability
- **Service downtime**: Tests may fail during provider maintenance
- **New models**: Provider model names may change over time
- **Feature updates**: Provider capabilities evolve (tool calling, streaming)

## 🔧 Troubleshooting

### Missing API Keys
**Symptoms**: Tests skip with "API key not found" message
**Solutions**:
- Verify `.local/.env` file exists in repository root
- Check API key names match exactly: `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GOOGLE_AI_API_KEY`
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
- Increase timeout values: `-timeout=10m`
- Check network connectivity
- Verify provider service status

### Tool Calling Issues
**Symptoms**: Tools not called when expected
**Solutions**:
- Review provider-specific behavior (Anthropic is more conservative)
- Check test prompt clarity and tool descriptions
- Verify tool schemas are correctly formatted

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
5. **Test with multiple providers**: Ensure changes work across OpenAI, Anthropic, Google AI

## 📚 Related Documentation

- [Main Conformance Tests README](./README.md)
- [Echo Provider Documentation](../../conversation/echo/README.md)
- [Conversation Component Interface](../../conversation/README.md)
- [Tool Calling Specification](../../docs/tool-calling.md)

---

*These real provider tests ensure that Dapr's conversation components work correctly with actual LLM providers in production scenarios. Use them for validation and debugging, but rely on Echo provider tests for regular development.* 