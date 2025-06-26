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

package langchaingokit

import (
	"context"
	"errors"

	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
)

// LLM is a helper struct that wraps a LangChain Go model
type LLM struct {
	llms.Model
	UsageGetterFunc   func(resp *llms.ContentResponse) *conversation.UsageInfo
	StreamingDisabled bool // If true, disables streaming functionality (some providers do not support streaming)
}

var ErrStreamingNotSupported = errors.New("streaming is not supported by this model or provider")

// generateContent is a helper function that generates content using the LangChain Go model.
func (a *LLM) generateContent(ctx context.Context, r *conversation.ConversationRequest, opts []llms.CallOption) (*conversation.ConversationResponse, error) {
	messages := GetMessageFromRequest(r)

	resp, err := a.GenerateContent(ctx, messages, opts...)
	if err != nil {
		return nil, err
	}

	outputs := make([]conversation.ConversationResult, 0, len(resp.Choices))

	for i := range resp.Choices {
		outputs = append(outputs, conversation.ConversationResult{
			Result:     resp.Choices[i].Content,
			Parameters: r.Parameters,
		})
	}

	usageGetter := a.UsageGetterFunc
	if usageGetter == nil {
		usageGetter = conversation.ExtractUsageFromResponse
	}

	return &conversation.ConversationResponse{
		Outputs: outputs,
		Usage:   usageGetter(resp),
	}, nil
}

// Converse executes a non-streaming conversation with the LangChain Go model.
func (a *LLM) Converse(ctx context.Context, r *conversation.ConversationRequest) (res *conversation.ConversationResponse, err error) {
	opts := GetOptionsFromRequest(r)

	return a.generateContent(ctx, r, opts)
}

// ConverseStream executes a streaming conversation with the LangChain Go model.
func (a *LLM) ConverseStream(ctx context.Context, r *conversation.ConversationRequest, streamFunc func(ctx context.Context, chunk []byte) error) (*conversation.ConversationResponse, error) {
	if a.StreamingDisabled {
		return nil, ErrStreamingNotSupported
	}
	opts := GetOptionsFromRequest(r, llms.WithStreamingFunc(streamFunc))

	return a.generateContent(ctx, r, opts)
}
