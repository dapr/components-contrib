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
	"bufio"
	"context"
	"log"
	"os"
	"path"
	"runtime"
	"strings"
	"testing"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/anthropic"
	"github.com/dapr/components-contrib/conversation/aws/bedrock"
	"github.com/dapr/components-contrib/conversation/googleai"
	"github.com/dapr/components-contrib/conversation/mistral"
	"github.com/dapr/components-contrib/conversation/ollama"
	"github.com/dapr/components-contrib/conversation/openai"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ConversationEnvVars holds the environment variables needed for conversation tests
type ConversationEnvVars struct {
	OpenAIAPIKey      string
	AnthropicAPIKey   string
	GoogleAIAPIKey    string
	MistralAPIKey     string
	HuggingfaceAPIKey string
	AWSAccessKeyID    string
	AWSSecretKey      string
	AWSRegion         string
}

// loadEnvFile loads environment variables from a .env file and sets them in the current environment
// This is a simple implementation that overrides existing environment variables
func loadEnvFile(filepath string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse key=value pairs
		if parts := strings.SplitN(line, "=", 2); len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])

			// Remove surrounding quotes if present
			if len(value) >= 2 {
				if (strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`)) ||
					(strings.HasPrefix(value, `'`) && strings.HasSuffix(value, `'`)) {
					value = value[1 : len(value)-1]
				}
			}

			// Set the environment variable (overriding existing value)
			os.Setenv(key, value)
		}
	}

	return scanner.Err()
}

// LoadEnvVars loads environment variables from a .env file and overrides existing ones
// This will override any existing environment variables with values from the .env file
func LoadEnvVars(relativePath string) ConversationEnvVars {
	_, filename, _, ok := runtime.Caller(1)
	if !ok {
		log.Fatal("Cannot get path to current file")
	}

	filepath := path.Join(path.Dir(filename), relativePath)

	// Load .env file and override existing environment variables
	err := loadEnvFile(filepath)
	if err != nil {
		log.Printf("Warning: Error loading .env file from %s: %v", filepath, err)
		log.Printf("Continuing with existing environment variables...")
	}

	return ConversationEnvVars{
		OpenAIAPIKey:      os.Getenv("OPENAI_API_KEY"),
		AnthropicAPIKey:   os.Getenv("ANTHROPIC_API_KEY"),
		GoogleAIAPIKey:    os.Getenv("GOOGLE_AI_API_KEY"),
		MistralAPIKey:     os.Getenv("MISTRAL_API_KEY"),
		HuggingfaceAPIKey: os.Getenv("HUGGINGFACE_API_KEY"),
		AWSAccessKeyID:    os.Getenv("AWS_ACCESS_KEY_ID"),
		AWSSecretKey:      os.Getenv("AWS_SECRET_ACCESS_KEY"),
		AWSRegion:         os.Getenv("AWS_REGION"),
	}
}

// TestModelProvider defines a test case for each langchaingo-based conversation model
type TestModelProvider struct {
	name           string
	envKey         string
	newFunc        func(logger.Logger) conversation.Conversation
	metadata       map[string]string
	requiresAPIKey bool
	skipReason     string
}

// getTestModels returns all langchaingo-based conversation models for testing
func getTestModels() []TestModelProvider {
	return []TestModelProvider{
		{
			name:           "OpenAI",
			envKey:         "OPENAI_API_KEY",
			newFunc:        func(l logger.Logger) conversation.Conversation { return openai.NewOpenAI(l) },
			requiresAPIKey: true,
			metadata: map[string]string{
				"model": "gpt-4o-mini", // Use a cheaper model for testing
			},
		},
		{
			name:           "Anthropic",
			envKey:         "ANTHROPIC_API_KEY",
			newFunc:        func(l logger.Logger) conversation.Conversation { return anthropic.NewAnthropic(l) },
			requiresAPIKey: true,
			metadata: map[string]string{
				"model": "claude-3-haiku-20240307", // Use a cheaper model for testing
			},
		},
		{
			name:           "GoogleAI",
			envKey:         "GOOGLE_AI_API_KEY",
			newFunc:        func(l logger.Logger) conversation.Conversation { return googleai.NewGoogleAI(l) },
			requiresAPIKey: true,
			metadata: map[string]string{
				"model": "gemini-1.5-flash",
			},
		},
		{
			name:           "Mistral",
			envKey:         "MISTRAL_API_KEY",
			newFunc:        func(l logger.Logger) conversation.Conversation { return mistral.NewMistral(l) },
			requiresAPIKey: true,
			metadata: map[string]string{
				"model": "open-mistral-7b",
			},
		},
		// TODO: Fix. HuggingFace is now broken in langchaingo, or we need to use a different SDK.
		//       URL changed to https://github.com/huggingface/huggingface_hub/issues/3148 - this can be fixed by using a different SDK.
		//       But now it requires `messages` in the json payload that cannot be changed without fixing langchaingo or it returns a 422 error.
		//       This is a workaround to use the HuggingFace API with OpenAI SDK.
		//       The endpoint is the HuggingFace API endpoint for the model (this is not so easy to find out tbh).
		{
			name:   "Huggingface",
			envKey: "HUGGINGFACE_API_KEY",
			// Using OpenAI SDK to use the HuggingFace API as a workaround.
			newFunc:        func(l logger.Logger) conversation.Conversation { return openai.NewOpenAI(l) },
			requiresAPIKey: true,
			metadata: map[string]string{
				"model":    "deepseek-ai/DeepSeek-R1-Distill-Qwen-32B",
				"endpoint": "https://router.huggingface.co/hf-inference/models/deepseek-ai/DeepSeek-R1-Distill-Qwen-32B/v1",
			},
		},
		{
			name:    "Ollama",
			envKey:  "", // Ollama doesn't require an API key
			newFunc: func(l logger.Logger) conversation.Conversation { return ollama.NewOllama(l) },
			metadata: map[string]string{
				"model": "llama3.2:latest",
			},
			requiresAPIKey: false,
			skipReason:     "Requires local Ollama server running",
		},
		{
			name:           "AWS Bedrock",
			envKey:         "AWS_ACCESS_KEY_ID", // Also needs AWS_SECRET_ACCESS_KEY
			newFunc:        func(l logger.Logger) conversation.Conversation { return bedrock.NewAWSBedrock(l) },
			requiresAPIKey: true,
			metadata: map[string]string{
				"region": "us-east-1",
				"model":  "anthropic.claude-3-haiku-20240307-v1:0",
			},
		},
	}
}

// TestLangchaingoConverseWithEnvFile tests the Converse functionality for all langchaingo-based models
// using environment variables loaded from a .env file
func TestLangchaingoConverseWithEnvFile(t *testing.T) {
	// Load environment variables from .env file (will override existing ones)
	envVars := LoadEnvVars(".env")

	logger := logger.NewLogger("langchaingo-test")
	ctx := context.Background()

	testModels := getTestModels()

	for _, testModel := range testModels {
		t.Run(testModel.name, func(t *testing.T) {
			// Skip test if there's a specific skip reason
			if testModel.skipReason != "" {
				t.Skipf("Skipping %s: %s", testModel.name, testModel.skipReason)
				return
			}

			// Check for API key if required
			if testModel.requiresAPIKey {
				var apiKey string

				// Get the appropriate API key based on the model
				switch testModel.name {
				case "OpenAI":
					apiKey = envVars.OpenAIAPIKey
				case "Anthropic":
					apiKey = envVars.AnthropicAPIKey
				case "GoogleAI":
					apiKey = envVars.GoogleAIAPIKey
				case "Mistral":
					apiKey = envVars.MistralAPIKey
				case "Huggingface":
					apiKey = envVars.HuggingfaceAPIKey
				case "AWS Bedrock":
					apiKey = envVars.AWSAccessKeyID
					if envVars.AWSSecretKey == "" {
						t.Skipf("Skipping %s: AWS_SECRET_ACCESS_KEY not set in .env file", testModel.name)
						return
					}
				}

				if apiKey == "" {
					t.Skipf("Skipping %s: %s not set in .env file", testModel.name, testModel.envKey)
					return
				}

				// Add the API key to metadata
				testModel.metadata["key"] = apiKey

				// For AWS Bedrock, also set region if available
				if testModel.name == "AWS Bedrock" && envVars.AWSRegion != "" {
					testModel.metadata["region"] = envVars.AWSRegion
				}
			}

			// Create the conversation component
			conv := testModel.newFunc(logger)
			require.NotNil(t, conv)

			// Initialize the component
			err := conv.Init(ctx, conversation.Metadata{
				Base: metadata.Base{
					Properties: testModel.metadata,
				},
			})
			require.NoError(t, err, "Failed to initialize %s", testModel.name)

			// Test the Converse function
			request := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "Hello! Please respond with just the word 'SUCCESS' and nothing else.",
						Role:    conversation.RoleUser,
					},
				},
				Temperature: 0.1, // Low temperature for consistent responses
			}

			response, err := conv.Converse(ctx, request)
			require.NoError(t, err, "Converse failed for %s", testModel.name)
			require.NotNil(t, response, "Response should not be nil for %s", testModel.name)
			require.NotEmpty(t, response.Outputs, "Response outputs should not be empty for %s", testModel.name)
			assert.NotEmpty(t, response.Outputs[0].Result, "Response result should not be empty for %s", testModel.name)

			// Log the response for debugging
			t.Logf("%s response: %s", testModel.name, response.Outputs[0].Result)

			// Clean up
			err = conv.Close()
			assert.NoError(t, err, "Close failed for %s", testModel.name)
		})
	}
}

// TestLangchaingoConverse tests the Converse functionality for all langchaingo-based models
func TestLangchaingoConverse(t *testing.T) {
	logger := logger.NewLogger("langchaingo-test")
	ctx := context.Background()

	testModels := getTestModels()

	for _, testModel := range testModels {
		t.Run(testModel.name, func(t *testing.T) {
			// Skip test if there's a specific skip reason
			if testModel.skipReason != "" {
				t.Skipf("Skipping %s: %s", testModel.name, testModel.skipReason)
				return
			}

			// Check for API key if required
			if testModel.requiresAPIKey {
				apiKey := os.Getenv(testModel.envKey)
				if apiKey == "" {
					t.Skipf("Skipping %s: %s environment variable not set", testModel.name, testModel.envKey)
					return
				}

				// For AWS Bedrock, also check for secret key
				if testModel.name == "AWS Bedrock" {
					if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
						t.Skipf("Skipping %s: AWS_SECRET_ACCESS_KEY environment variable not set", testModel.name)
						return
					}
				}

				// Add the API key to metadata
				testModel.metadata["key"] = apiKey
			}

			// Create the conversation component
			conv := testModel.newFunc(logger)
			require.NotNil(t, conv)

			// Initialize the component
			err := conv.Init(ctx, conversation.Metadata{
				Base: metadata.Base{
					Properties: testModel.metadata,
				},
			})
			require.NoError(t, err, "Failed to initialize %s", testModel.name)

			// Test the Converse function
			request := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "Hello! Please respond with just the word 'SUCCESS' and nothing else.",
						Role:    conversation.RoleUser,
					},
				},
				Temperature: 0.1, // Low temperature for consistent responses
			}

			response, err := conv.Converse(ctx, request)
			require.NoError(t, err, "Converse failed for %s", testModel.name)
			require.NotNil(t, response, "Response should not be nil for %s", testModel.name)
			require.NotEmpty(t, response.Outputs, "Response outputs should not be empty for %s", testModel.name)
			assert.NotEmpty(t, response.Outputs[0].Result, "Response result should not be empty for %s", testModel.name)

			// Log the response for debugging
			t.Logf("%s response: %s", testModel.name, response.Outputs[0].Result)

			// Clean up
			err = conv.Close()
			assert.NoError(t, err, "Close failed for %s", testModel.name)
		})
	}
}

// TestLangchaingoInit tests the initialization of all langchaingo-based models
func TestLangchaingoInit(t *testing.T) {
	logger := logger.NewLogger("langchaingo-init-test")
	ctx := context.Background()

	testModels := getTestModels()

	for _, testModel := range testModels {
		t.Run(testModel.name+"_Init", func(t *testing.T) {
			// Create minimal metadata (without API key for this test)
			testMetadata := map[string]string{}
			for k, v := range testModel.metadata {
				if k != "key" {
					testMetadata[k] = v
				}
			}

			// Add a dummy key if required to test initialization logic
			if testModel.requiresAPIKey {
				testMetadata["key"] = "dummy-key-for-init-test"
			}

			conv := testModel.newFunc(logger)
			require.NotNil(t, conv)

			// This test just checks that initialization doesn't panic/crash
			// It may fail with authentication errors, but should not crash
			_ = conv.Init(ctx, conversation.Metadata{
				Base: metadata.Base{
					Properties: testMetadata,
				},
			})

			// Test GetComponentMetadata if available (requires metadata build tag)
			if compWithMeta, ok := conv.(interface{ GetComponentMetadata() metadata.MetadataMap }); ok {
				metadataInfo := compWithMeta.GetComponentMetadata()
				assert.NotEmpty(t, metadataInfo, "Component metadata should not be empty for %s", testModel.name)
			}

			// Clean up
			err := conv.Close()
			assert.NoError(t, err, "Close failed for %s", testModel.name)
		})
	}
}

// TestLangchaingoMetadata tests that all langchaingo-based models expose proper metadata
func TestLangchaingoMetadata(t *testing.T) {
	logger := logger.NewLogger("langchaingo-metadata-test")

	testModels := getTestModels()

	for _, testModel := range testModels {
		t.Run(testModel.name+"_Metadata", func(t *testing.T) {
			conv := testModel.newFunc(logger)
			require.NotNil(t, conv)

			// Test GetComponentMetadata if available (requires metadata build tag)
			if compWithMeta, ok := conv.(interface{ GetComponentMetadata() metadata.MetadataMap }); ok {
				metadataInfo := compWithMeta.GetComponentMetadata()
				assert.NotEmpty(t, metadataInfo, "Component metadata should not be empty for %s", testModel.name)

				// All langchaingo-based models should have these common metadata fields
				if testModel.requiresAPIKey {
					// AWS Bedrock uses different field names
					if testModel.name == "AWS Bedrock" {
						_, hasAccessKey := metadataInfo["AccessKey"]
						assert.True(t, hasAccessKey, "%s should have 'AccessKey' metadata field", testModel.name)
					} else {
						_, hasKey := metadataInfo["Key"]
						assert.True(t, hasKey, "%s should have 'Key' metadata field", testModel.name)
					}
				}

				_, hasModel := metadataInfo["Model"]
				assert.True(t, hasModel, "%s should have 'Model' metadata field", testModel.name)

				t.Logf("%s metadata fields: %v", testModel.name, func() []string {
					var fields []string
					for field := range metadataInfo {
						fields = append(fields, field)
					}
					return fields
				}())
			} else {
				t.Skipf("Skipping metadata test for %s: GetComponentMetadata not available (requires metadata build tag)", testModel.name)
			}
		})
	}
}
