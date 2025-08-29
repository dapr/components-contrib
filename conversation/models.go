/*
Copyright 2024 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package conversation

import (
	"os"
)

// Default models for conversation components
// These can be overridden via environment variables for runtime configuration
const (
	// Environment variable names
	envOpenAIModel      = "DAPR_CONVERSATION_OPENAI_MODEL"
	envAnthropicModel   = "DAPR_CONVERSATION_ANTHROPIC_MODEL"
	envGoogleAIModel    = "DAPR_CONVERSATION_GOOGLEAI_MODEL"
	envMistralModel     = "DAPR_CONVERSATION_MISTRAL_MODEL"
	envHuggingFaceModel = "DAPR_CONVERSATION_HUGGINGFACE_MODEL"
	envOllamaModel      = "DAPR_CONVERSATION_OLLAMA_MODEL"
)

// Default model values (used as fallbacks when env vars are not set)
const (
	defaultOpenAIModel      = "gpt-5-nano"
	defaultAnthropicModel   = "claude-3-5-sonnet-20240620"
	defaultGoogleAIModel    = "gemini-1.5-flash"
	defaultMistralModel     = "open-mistral-7b"
	defaultHuggingFaceModel = "deepseek-ai/DeepSeek-R1-Distill-Qwen-32B"
	defaultOllamaModel      = "llama3.2:latest"
)

// getEnvOrDefault returns the value of an environment variable or a default value
func getEnvOrDefault(envVar, defaultValue string) string {
	if value := os.Getenv(envVar); value != "" {
		return value
	}
	return defaultValue
}

// Default model getters that check environment variables first
var (
	// DefaultOpenAIModel returns the OpenAI model, checking env var first
	DefaultOpenAIModel = getEnvOrDefault(envOpenAIModel, defaultOpenAIModel)

	// DefaultAnthropicModel returns the Anthropic model, checking env var first
	DefaultAnthropicModel = getEnvOrDefault(envAnthropicModel, defaultAnthropicModel)

	// DefaultGoogleAIModel returns the Google AI model, checking env var first
	DefaultGoogleAIModel = getEnvOrDefault(envGoogleAIModel, defaultGoogleAIModel)

	// DefaultMistralModel returns the Mistral model, checking env var first
	DefaultMistralModel = getEnvOrDefault(envMistralModel, defaultMistralModel)

	// DefaultHuggingFaceModel returns the HuggingFace model, checking env var first
	DefaultHuggingFaceModel = getEnvOrDefault(envHuggingFaceModel, defaultHuggingFaceModel)

	// DefaultOllamaModel returns the Ollama model, checking env var first
	DefaultOllamaModel = getEnvOrDefault(envOllamaModel, defaultOllamaModel)
)
