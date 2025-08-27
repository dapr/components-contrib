//go:build conftests
// +build conftests

/*
Copyright 2021 The Dapr Authors
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

package conformance

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/anthropic"
	"github.com/dapr/components-contrib/conversation/aws/bedrock"
	"github.com/dapr/components-contrib/conversation/echo"
	"github.com/dapr/components-contrib/conversation/googleai"
	"github.com/dapr/components-contrib/conversation/huggingface"
	"github.com/dapr/components-contrib/conversation/mistral"
	"github.com/dapr/components-contrib/conversation/ollama"
	"github.com/dapr/components-contrib/conversation/openai"
	conf_conversation "github.com/dapr/components-contrib/tests/conformance/conversation"
	"github.com/dapr/components-contrib/tests/conformance/utils"
)

func TestConversationConformance(t *testing.T) {
	const configPath = "../config/conversation/"

	// Try to load environment variables from .env file
	// This will silently continue if the .env file doesn't exist
	utils.LoadEnvVars(configPath + ".env")

	tc, err := NewTestConfiguration(filepath.Join(configPath, "tests.yml"))
	require.NoError(t, err)
	require.NotNil(t, tc)

	tc.TestFn = func(comp *TestComponent) func(t *testing.T) {
		return func(t *testing.T) {
			// Skip tests based on missing environment variables
			if shouldSkipComponent(t, comp.Component) {
				return
			}

			ParseConfigurationMap(t, comp.Config)

			componentConfigPath := convertComponentNameToPath(comp.Component, comp.Profile)
			props, err := loadComponentsAndProperties(t, filepath.Join(configPath, componentConfigPath))
			require.NoErrorf(t, err, "error running conformance test for component %s", comp.Component)

			conv := loadConversationComponent(comp.Component)
			require.NotNil(t, conv, "error running conformance test for component %s", comp.Component)

			conf_conversation.ConformanceTests(t, props, conv, comp.Component)
		}
	}

	tc.Run(t)
}

// shouldSkipComponent checks if a component test should be skipped due to missing environment variables
func shouldSkipComponent(t *testing.T, componentName string) bool {
	switch componentName {
	case "openai.openai":
		if os.Getenv("OPENAI_API_KEY") == "" {
			t.Skipf("Skipping OpenAI conformance test: OPENAI_API_KEY environment variable not set")
			return true
		}
	case "openai.azure":
		if os.Getenv("AZURE_OPENAI_API_KEY") == "" || os.Getenv("AZURE_OPENAI_ENDPOINT") == "" || os.Getenv("AZURE_OPENAI_API_TYPE") == "" || os.Getenv("AZURE_OPENAI_API_VERSION") == "" {
			t.Skipf("Skipping Azure OpenAI conformance test: AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_TYPE, and AZURE_OPENAI_API_VERSION environment variables must be set")
			return true
		}
	case "anthropic":
		if os.Getenv("ANTHROPIC_API_KEY") == "" {
			t.Skipf("Skipping Anthropic conformance test: ANTHROPIC_API_KEY environment variable not set")
			return true
		}
	case "googleai":
		if os.Getenv("GOOGLE_AI_API_KEY") == "" {
			t.Skipf("Skipping GoogleAI conformance test: GOOGLE_AI_API_KEY environment variable not set")
			return true
		}
	case "mistral":
		if os.Getenv("MISTRAL_API_KEY") == "" {
			t.Skipf("Skipping Mistral conformance test: MISTRAL_API_KEY environment variable not set")
			return true
		}
	case "huggingface":
		if os.Getenv("HUGGINGFACE_API_KEY") == "" {
			t.Skipf("Skipping Huggingface conformance test: HUGGINGFACE_API_KEY environment variable not set")
			return true
		}
	case "ollama":
		// Ollama requires a local server - always skip for now unless specifically enabled
		if os.Getenv("OLLAMA_ENABLED") == "" {
			t.Skipf("Skipping Ollama conformance test: requires local Ollama server (set OLLAMA_ENABLED=1 to enable)")
			return true
		}
	case "bedrock":
		if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
			t.Skipf("Skipping AWS Bedrock conformance test: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables not set")
			return true
		}
	}
	return false
}

func loadConversationComponent(name string) conversation.Conversation {
	switch name {
	case "echo":
		return echo.NewEcho(testLogger)
	case "openai.openai", "openai.azure":
		return openai.NewOpenAI(testLogger)
	case "anthropic":
		return anthropic.NewAnthropic(testLogger)
	case "googleai":
		return googleai.NewGoogleAI(testLogger)
	case "mistral":
		return mistral.NewMistral(testLogger)
	case "huggingface":
		return huggingface.NewHuggingface(testLogger)
	case "ollama":
		return ollama.NewOllama(testLogger)
	case "bedrock":
		return bedrock.NewAWSBedrock(testLogger)
	default:
		return nil
	}
}
