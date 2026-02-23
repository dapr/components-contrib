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

package bedrock

import (
	"os"
	"testing"

	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAWSBedrock(t *testing.T) {
	lg := logger.NewLogger("bedrock test")
	b := NewAWSBedrock(lg)
	assert.NotNil(t, b)
	assert.Implements(t, (*conversation.Conversation)(nil), b)
}

func TestInit(t *testing.T) {
	testCases := []struct {
		name        string
		metadata    map[string]string
		expectError bool
		errorMsg    string
	}{
		{
			name: "missing region",
			metadata: map[string]string{
				"model": "amazon.titan-text-lite-v1",
			},
			expectError: false,
		},
		{
			name: "valid metadata",
			metadata: map[string]string{
				"region": "us-east-1",
				"model":  "amazon.titan-text-lite-v1",
			},
			expectError: false,
		},
		{
			name: "with access key and secret",
			metadata: map[string]string{
				"region":    "us-east-1",
				"accessKey": "test-key",
				"secretKey": "test-secret",
				"model":     "amazon.titan-text-lite-v1",
			},
			expectError: false,
		},
		{
			name: "with responseCacheTTL",
			metadata: map[string]string{
				"region":           "us-east-1",
				"model":            "amazon.titan-text-lite-v1",
				"responseCacheTTL": "5m",
			},
			expectError: false,
		},
		{
			name: "invalid responseCacheTTL",
			metadata: map[string]string{
				"region":           "us-east-1",
				"model":            "amazon.titan-text-lite-v1",
				"responseCacheTTL": "invalid",
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b := &AWSBedrock{
				logger: logger.NewLogger("bedrock test"),
			}
			err := b.Init(t.Context(), conversation.Metadata{
				Base: metadata.Base{
					Properties: tc.metadata,
				},
			})
			if tc.expectError {
				require.Error(t, err)
				if tc.errorMsg != "" {
					assert.Contains(t, err.Error(), tc.errorMsg)
				}
			} else {
				// TODO Implement mocks
				if err != nil {
					t.Skipf("Skipping test due to AWS config error: %v", err)
				}
				assert.NotNil(t, b.LLM.Model)
				assert.Equal(t, tc.metadata["model"], b.model)
			}
		})
	}
}

func TestGetComponentMetadata(t *testing.T) {
	b := &AWSBedrock{}
	md := b.GetComponentMetadata()
	assert.NotEmpty(t, md)

	expectedFields := []string{"Region", "Endpoint", "AccessKey", "SecretKey", "SessionToken", "Model", "responseCacheTTL", "AssumeRoleArn", "TrustAnchorArn", "TrustProfileArn"}
	for _, field := range expectedFields {
		_, exists := md[field]
		assert.True(t, exists, "Field %s should exist in metadata", field)
	}
}

func TestClose(t *testing.T) {
	b := &AWSBedrock{}
	err := b.Close()
	assert.NoError(t, err)
}

func TestConverse(t *testing.T) {
	// Skip if no AWS credentials
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Skip("Skipping Converse test: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY not set")
	}

	b := NewAWSBedrock(logger.NewLogger("bedrock test"))
	err := b.Init(t.Context(), conversation.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"region": "us-east-1",
				"model":  "amazon.titan-text-lite-v1",
			},
		},
	})
	require.NoError(t, err)

	resp, err := b.Converse(t.Context(), &conversation.Request{
		Message: &[]llms.MessageContent{
			{
				Role: llms.ChatMessageTypeHuman,
				Parts: []llms.ContentPart{
					llms.TextContent{Text: "Hello"},
				},
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotEmpty(t, resp.Outputs)
}
