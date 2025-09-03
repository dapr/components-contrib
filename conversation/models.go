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
	defaultOpenAIModel      = "gpt-5-nano" // Enable GPT-5 (Preview) for all clients
	defaultAnthropicModel   = "claude-sonnet-4-20250514"
	defaultGoogleAIModel    = "gemini-2.5-flash-lite"
	defaultMistralModel     = "open-mistral-7b"
	defaultHuggingFaceModel = "deepseek-ai/DeepSeek-R1-Distill-Qwen-32B"
	defaultOllamaModel      = "llama3.2:latest"
)

// getEnvOrDefault returns the value of an environment variable or a default value
func getModelValue(envVar, defaultValue, metadataValue string) string {
	if metadataValue != "" {
		return metadataValue
	}
	if value := os.Getenv(envVar); value != "" {
		return value
	}
	return defaultValue
}

// Example usage for model getters with metadata support:
// Pass metadataValue from your metadata file/struct, or "" if not set.
func GetOpenAIModel(metadataValue string) string {
	return getModelValue(envOpenAIModel, defaultOpenAIModel, metadataValue)
}

func GetAnthropicModel(metadataValue string) string {
	return getModelValue(envAnthropicModel, defaultAnthropicModel, metadataValue)
}

func GetGoogleAIModel(metadataValue string) string {
	return getModelValue(envGoogleAIModel, defaultGoogleAIModel, metadataValue)
}

func GetMistralModel(metadataValue string) string {
	return getModelValue(envMistralModel, defaultMistralModel, metadataValue)
}

func GetHuggingFaceModel(metadataValue string) string {
	return getModelValue(envHuggingFaceModel, defaultHuggingFaceModel, metadataValue)
}

func GetOllamaModel(metadataValue string) string {
	return getModelValue(envOllamaModel, defaultOllamaModel, metadataValue)
}
