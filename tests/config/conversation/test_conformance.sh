#!/bin/bash

# Simple script to test conversation conformance tests
# This script will run the conformance tests and show which ones are skipped vs. run

echo "Running conversation conformance tests..."
echo "Tests will be skipped if the required API keys are not set as environment variables."
echo ""

# Run the conformance tests with 30s timeout
cd "$(dirname "$0")/../../.."
go test -tags conftests ./tests/conformance -run TestConversationConformance -v -timeout 30s

echo ""
echo "To run tests with actual API keys, you have two options:"
echo ""
echo "Option 1: Use a .env file (recommended):"
echo "  cp env.template .env"
echo "  # Edit .env file and add your API keys"
echo "  ./test_conformance.sh"
echo ""
echo "Option 2: Set environment variables directly:"
echo "  export OPENAI_API_KEY=\"your_openai_api_key\""
echo "  export AZURE_OPENAI_API_KEY=\"your_azureopenai_api_key\""
echo "  export AZURE_OPENAI_ENDPOINT=\"your_azureopenai_endpoint\""
echo "  export AZURE_OPENAI_API_VERSION=\"your_azureopenai_api_version\""
echo "  export ANTHROPIC_API_KEY=\"your_anthropic_api_key\""
echo "  export GOOGLE_AI_API_KEY=\"your_google_ai_api_key\""
echo "  export MISTRAL_API_KEY=\"your_mistral_api_key\""
echo "  export HUGGINGFACE_API_KEY=\"your_huggingface_api_key\""
echo "  export AWS_ACCESS_KEY_ID=\"your_aws_access_key\""
echo "  export AWS_SECRET_ACCESS_KEY=\"your_aws_secret_key\""
echo "  export AWS_REGION=\"us-east-1\""
echo "  export OLLAMA_ENABLED=\"1\"  # if you have Ollama running locally"
echo "  ./test_conformance.sh" 