/*
Copyright 2025 The Dapr Authors
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
	envOpenAIModel      = "OPENAI_MODEL"
	envAzureOpenAIModel = "AZURE_OPENAI_MODEL"
	envAnthropicModel   = "ANTHROPIC_MODEL"
	envGoogleAIModel    = "GOOGLEAI_MODEL"
	envMistralModel     = "MISTRAL_MODEL"
	envHuggingFaceModel = "HUGGINGFACE_MODEL"
	envOllamaModel      = "OLLAMA_MODEL"
)

// Exported default model constants for consumers of the conversation package.
// These are used as fallbacks when env vars and metadata are not set.
const (
	DefaultOpenAIModel      = "gpt-5-nano"   // Enable GPT-5 (Preview) for all clients
	DefaultAzureOpenAIModel = "gpt-4.1-nano" // Default Azure OpenAI model
	DefaultAnthropicModel   = "claude-sonnet-4-20250514"
	DefaultGoogleAIModel    = "gemini-2.5-flash-lite"
	DefaultMistralModel     = "open-mistral-7b"
	DefaultHuggingFaceModel = "deepseek-ai/DeepSeek-R1-Distill-Qwen-32B"
	DefaultOllamaModel      = "llama3.2:latest"
)

// getModel returns the value of an environment variable or a default value
func getModel(envVar, defaultValue, metadataValue string) string {
	if value := os.Getenv(envVar); value != "" {
		return value
	}
	if metadataValue != "" {
		return metadataValue
	}
	return defaultValue
}

// Example usage for model getters with metadata support:
// Pass metadataValue from your metadata file/struct, or "" if not set.
func GetOpenAIModel(metadataValue string) string {
	return getModel(envOpenAIModel, DefaultOpenAIModel, metadataValue)
}

func GetAzureOpenAIModel(metadataValue string) string {
	return getModel(envAzureOpenAIModel, DefaultAzureOpenAIModel, metadataValue)
}

func GetAnthropicModel(metadataValue string) string {
	return getModel(envAnthropicModel, DefaultAnthropicModel, metadataValue)
}

func GetGoogleAIModel(metadataValue string) string {
	return getModel(envGoogleAIModel, DefaultGoogleAIModel, metadataValue)
}

func GetMistralModel(metadataValue string) string {
	return getModel(envMistralModel, DefaultMistralModel, metadataValue)
}

func GetHuggingFaceModel(metadataValue string) string {
	return getModel(envHuggingFaceModel, DefaultHuggingFaceModel, metadataValue)
}

func GetOllamaModel(metadataValue string) string {
	return getModel(envOllamaModel, DefaultOllamaModel, metadataValue)
}
