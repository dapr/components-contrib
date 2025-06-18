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
package echo

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"
)

// Echo implement is only for test.
type Echo struct {
	model  string
	logger logger.Logger
}

func NewEcho(logger logger.Logger) conversation.Conversation {
	e := &Echo{
		logger: logger,
	}

	return e
}

func (e *Echo) Init(ctx context.Context, meta conversation.Metadata) error {
	r := &conversation.ConversationRequest{}
	err := kmeta.DecodeMetadata(meta.Properties, r)
	if err != nil {
		return err
	}

	e.model = r.Model

	return nil
}

func (e *Echo) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := conversation.ConversationRequest{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}

// Converse returns inputs directly.
func (e *Echo) Converse(ctx context.Context, r *conversation.ConversationRequest) (res *conversation.ConversationResponse, err error) {
	outputs := make([]conversation.ConversationResult, 0, len(r.Inputs))

	for _, input := range r.Inputs {
		outputs = append(outputs, conversation.ConversationResult{
			Result:     input.Message,
			Parameters: r.Parameters,
		})
	}

	// Calculate token usage for demonstration
	totalInputTokens := int32(0)
	totalOutputTokens := int32(0)

	for _, input := range r.Inputs {
		// Simple token estimation: roughly 1 token per 4 characters
		inputTokens := int32(len(input.Message) / int(4)) //nolint:gosec // This is a valid conversion
		if inputTokens == 0 && len(input.Message) > 0 {
			inputTokens = 1 // Minimum 1 token for non-empty input
		}
		totalInputTokens += inputTokens
		totalOutputTokens += inputTokens // Echo returns the same content
	}

	res = &conversation.ConversationResponse{
		Outputs:             outputs,
		ConversationContext: r.ConversationContext, // Echo back the context ID
		Usage: &conversation.UsageInfo{
			PromptTokens:     totalInputTokens,
			CompletionTokens: totalOutputTokens,
			TotalTokens:      totalInputTokens + totalOutputTokens,
		},
	}

	return res, nil
}

// ConverseStream implements streaming support for the echo component
func (e *Echo) ConverseStream(ctx context.Context, r *conversation.ConversationRequest, streamFunc func(ctx context.Context, chunk []byte) error) (*conversation.ConversationResponse, error) {
	outputs := make([]conversation.ConversationResult, 0, len(r.Inputs))

	for _, input := range r.Inputs {
		// Simulate streaming by sending the input message in chunks
		content := input.Message

		// Break content into words for more realistic streaming
		words := strings.Fields(content)
		if len(words) == 0 {
			// Handle empty input
			if err := streamFunc(ctx, []byte("")); err != nil {
				return nil, err
			}
		} else {
			// Send each word as a separate chunk with a space
			for i, word := range words {
				var chunk []byte
				if i == 0 {
					chunk = []byte(word)
				} else {
					chunk = []byte(" " + word)
				}

				// Send the chunk
				if err := streamFunc(ctx, chunk); err != nil {
					return nil, err
				}

				// Add a small delay to simulate real streaming behavior
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(5 * time.Millisecond):
					// Continue
				}
			}
		}

		// Add the complete result to outputs for the final response
		outputs = append(outputs, conversation.ConversationResult{
			Result:     content,
			Parameters: r.Parameters,
		})
	}

	// Generate a context ID if one wasn't provided
	contextID := r.ConversationContext
	if contextID == "" {
		contextID = fmt.Sprintf("echo-context-%d", time.Now().UnixNano())
	}

	// Calculate token usage for demonstration
	// In a real implementation, this would come from the LLM provider
	totalInputTokens := int32(0)
	totalOutputTokens := int32(0)

	for _, input := range r.Inputs {
		// Simple token estimation: roughly 1 token per 4 characters
		inputTokens := int32(len(input.Message) / int(4)) //nolint:gosec // This is a valid conversion
		if inputTokens == 0 && len(input.Message) > 0 {
			inputTokens = 1 // Minimum 1 token for non-empty input
		}
		totalInputTokens += inputTokens
		totalOutputTokens += inputTokens // Echo returns the same content
	}

	res := &conversation.ConversationResponse{
		Outputs:             outputs,
		ConversationContext: contextID,
		Usage: &conversation.UsageInfo{
			PromptTokens:     totalInputTokens,
			CompletionTokens: totalOutputTokens,
			TotalTokens:      totalInputTokens + totalOutputTokens,
		},
	}

	return res, nil
}

func (e *Echo) Close() error {
	return nil
}
