# LangChain Go Kit Integration Tests

This directory contains integration tests for all conversation components that use the langchaingo library.

## Overview

The `langchaingokit_integration_test.go` file provides comprehensive testing for all langchaingo-based conversation models with support for both environment variables and `.env` file configuration.

## Supported Models

The tests cover the following langchaingo-based conversation models:

1. **OpenAI** - Requires `OPENAI_API_KEY`
2. **Anthropic** - Requires `ANTHROPIC_API_KEY`
3. **Google AI** - Requires `GOOGLE_AI_API_KEY`
4. **Mistral** - Requires `MISTRAL_API_KEY`
5. **Hugging Face** - Requires `HUGGINGFACE_API_KEY` (uses OpenAI SDK as workaround)
6. **AWS Bedrock** - Requires `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
7. **Ollama** - No API key required (requires local Ollama server)

## Environment Variable Configuration

### Option 1: Using .env File (Recommended)

Create a `.env` file in this directory with your API keys:

```bash
# Copy the template and edit with your keys
cp env.template .env
```

Example `.env` file content:
```env
# OpenAI API Key
OPENAI_API_KEY=sk-your-openai-api-key-here

# Anthropic API Key  
ANTHROPIC_API_KEY=sk-ant-your-anthropic-api-key-here

# Google AI API Key
GOOGLE_AI_API_KEY=your-google-ai-api-key-here

# Mistral API Key
MISTRAL_API_KEY=your-mistral-api-key-here

# Hugging Face API Key
HUGGINGFACE_API_KEY=hf-your-huggingface-api-key-here

# AWS Credentials for Bedrock
AWS_ACCESS_KEY_ID=your-aws-access-key-id
AWS_SECRET_ACCESS_KEY=your-aws-secret-access-key
AWS_REGION=us-east-1
```

**Note**: The `.env` file is git-ignored for security. Never commit API keys to version control.

### Option 2: Export Environment Variables

```bash
export OPENAI_API_KEY="your-openai-key"
export ANTHROPIC_API_KEY="your-anthropic-key"
export GOOGLE_AI_API_KEY="your-google-ai-key"
# ... set other keys as needed
```

## Running the Tests

### From Repository Root

```bash
# Test with .env file (uses custom .env loader)
go test -v ./conversation/langchaingokit/tests -run TestLangchaingoConverseWithEnvFile

# Test with existing environment variables
go test -v ./conversation/langchaingokit/tests -run TestLangchaingoConverse

# Run all tests
go test -v ./conversation/langchaingokit/tests

# Test only initialization
go test -v ./conversation/langchaingokit/tests -run TestLangchaingoInit

# Test only metadata
go test -v ./conversation/langchaingokit/tests -run TestLangchaingoMetadata
```

### From This Directory

```bash
cd conversation/langchaingokit/tests

# Test with .env file
go test -v -run TestLangchaingoConverseWithEnvFile

# Test specific model with .env file
go test -v -run TestLangchaingoConverseWithEnvFile/OpenAI

# Run all tests
go test -v
```

### With Metadata Build Tag

To enable metadata testing (GetComponentMetadata functionality):

```bash
go test -v -tags=metadata ./conversation/langchaingokit/tests
```

## Test Coverage

The integration tests include:

1. **Converse Test with .env File** (`TestLangchaingoConverseWithEnvFile`)
   - Uses custom `.env` file loader to load API keys from `.env` file
   - Overrides any existing environment variables
   - Tests actual conversation functionality
   - Skips models where API keys are not available in .env file

2. **Converse Test** (`TestLangchaingoConverse`)
   - Tests actual conversation functionality using existing environment variables
   - Requires valid API keys in environment
   - Skips models where API keys are not available

3. **Init Test** (`TestLangchaingoInit`)
   - Tests component initialization
   - Uses dummy API keys to test initialization logic
   - Validates that components don't crash during init

4. **Metadata Test** (`TestLangchaingoMetadata`)
   - Tests component metadata exposure
   - Requires the `metadata` build tag
   - Validates that common metadata fields are present

## Helper Functions

### LoadEnvVars Function

The `LoadEnvVars` helper function:

- Uses a custom simple `.env` file parser (no external dependencies)
- Overrides any existing environment variables
- Returns a structured `ConversationEnvVars` object
- Gracefully handles missing `.env` files with warnings
- Based on the same pattern as other component tests

**Features of the custom .env loader:**
- Parses `KEY=value` pairs
- Supports quoted values (`"value"` or `'value'`)
- Skips comments (lines starting with `#`)
- Skips empty lines
- Trims whitespace around keys and values

```go
// Usage example
envVars := LoadEnvVars(".env")
if envVars.OpenAIAPIKey != "" {
    // Use OpenAI API key
}
```

## Skipped Tests

Some tests may be skipped for the following reasons:
- API key not provided (test will show as "SKIP" with reason)
- Ollama: Requires local server running
- Metadata tests: Require `metadata` build tag

## Getting API Keys

1. **OpenAI**: https://platform.openai.com/api-keys
2. **Anthropic**: https://console.anthropic.com/
3. **Google AI**: https://aistudio.google.com/app/apikey
4. **Mistral**: https://console.mistral.ai/
5. **Hugging Face**: https://huggingface.co/settings/tokens
6. **AWS Bedrock**: AWS Console -> IAM -> Users -> Security credentials

## Example Output

### With .env File
```
=== RUN   TestLangchaingoConverseWithEnvFile
=== RUN   TestLangchaingoConverseWithEnvFile/OpenAI
    langchaingokit_integration_test.go:296: OpenAI response: SUCCESS
=== RUN   TestLangchaingoConverseWithEnvFile/Anthropic
    langchaingokit_integration_test.go:296: Anthropic response: SUCCESS
...
```

### Without API Keys
```
=== RUN   TestLangchaingoConverseWithEnvFile/OpenAI
    langchaingokit_integration_test.go:262: Skipping OpenAI: OPENAI_API_KEY not set in .env file
=== RUN   TestLangchaingoConverseWithEnvFile/Anthropic
    langchaingokit_integration_test.go:262: Skipping Anthropic: ANTHROPIC_API_KEY not set in .env file
...
```

## Security Notes

- ✅ `.env` files are automatically git-ignored
- ✅ Template file provided instead of actual `.env` file
- ✅ Clear documentation about not committing API keys
- ✅ Graceful error handling prevents secrets exposure
- ✅ **No external dependencies** - uses custom simple .env parser 