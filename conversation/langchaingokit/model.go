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

	"github.com/dapr/kit/logger"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
)

// LLM is a helper struct that wraps a LangChain Go model
type LLM struct {
	llms.Model
	model  string
	logger logger.Logger
}

func (a *LLM) Converse(ctx context.Context, r *conversation.Request) (res *conversation.Response, err error) {
	if r.LlmTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.LlmTimeout)
		defer func() {
			cancel()
		}()
	}

	opts := getOptionsFromRequest(r, a.logger)

	var messages []llms.MessageContent
	if r.Message != nil {
		messages = *r.Message
	}

	resp, err := a.GenerateContent(ctx, messages, opts...)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return nil, context.DeadlineExceeded
		}
		return nil, err
	}

	outputs, usage := a.NormalizeConverseResult(resp.Choices)

	// capture request override, otherwise grab component specified model
	var model string
	if r.Model != nil {
		model = *r.Model
	} else {
		model = a.model
	}

	return &conversation.Response{
		Model:   model,
		Usage:   usage,
		Outputs: outputs,
	}, nil
}

// NOTE: ollama does not provide a stop reason at all,
// so server side best we can do is say unknown if this is empty.
func normalizeFinishReason(stopReason string) string {
	if stopReason == "" {
		return "unknown"
	}
	return stopReason
}

func (a *LLM) NormalizeConverseResult(choices []*llms.ContentChoice) ([]conversation.Result, *conversation.Usage) {
	if len(choices) == 0 {
		return nil, nil
	}

	// Extract usage from the first choice's GenerationInfo (all choices share the same usage)
	var usage *conversation.Usage
	if len(choices) > 0 && choices[0].GenerationInfo != nil {
		usage = extractUsageFromLangchainGenerationInfo(choices[0].GenerationInfo)
	}

	outputs := make([]conversation.Result, 0, len(choices))
	for i := range choices {
		choice := conversation.Choice{
			FinishReason: normalizeFinishReason(choices[i].StopReason),
			Index:        int64(i),
		}

		if choices[i].Content != "" {
			choice.Message.Content = choices[i].Content
		}

		if choices[i].ToolCalls != nil {
			choice.Message.ToolCallRequest = &choices[i].ToolCalls
		}

		output := conversation.Result{
			StopReason: normalizeFinishReason(choices[i].StopReason),
			Choices:    []conversation.Choice{choice},
		}

		outputs = append(outputs, output)
	}

	return outputs, usage
}

// getOptionsFromRequest enables a way per request to override component level settings,
// as well as in general define request settings for the conversation.
func getOptionsFromRequest(r *conversation.Request, logger logger.Logger, opts ...llms.CallOption) []llms.CallOption {
	if opts == nil {
		opts = make([]llms.CallOption, 0)
	}

	if r.Temperature > 0 {
		opts = append(opts, llms.WithTemperature(r.Temperature))
	}

	if r.Tools != nil {
		opts = append(opts, llms.WithTools(*r.Tools))
	}

	if r.ToolChoice != nil {
		opts = append(opts, llms.WithToolChoice(r.ToolChoice))
	}

	if r.ResponseFormatAsJSONSchema != nil {
		structuredOutput, err := convertJSONSchemaForLangchain(r.ResponseFormatAsJSONSchema)
		if err != nil {
			logger.Warnf("failed to convert response format to structured output, will continue without structured output: %v", err)
		} else {
			opts = append(opts, llms.WithStructuredOutput(structuredOutput))
		}
		// Note: WithJSONMode() is not needed when using WithStructuredOutput,
		// as structured output already returns JSON so do NOT add that here in this block!
	}

	// Handle prompt cache retention for OpenAI's extended prompt caching feature
	if r.PromptCacheRetention > 0 {
		if r.Metadata == nil {
			r.Metadata = make(map[string]string)
		}
		// OpenAI expects this as a top-level parameter, but we are forced to pass it via metadata,
		// and langchaingo should forward it to the OpenAI client.
		// NOTE: This is absolutely a complete hack that I guarantee you does work.
		// In langchain there is a llms.WithPromptCaching(true) option that is incompatible with Openai yielding an err bc then it tries to use a bool instead of a string,
		// because openai expects this to be a time duration string but used with langchain with their llms.WithPromptCachine(true) does not translate properly.
		// When Langchain fixes this then we can update accordingly :)
		r.Metadata["prompt_cache_retention"] = r.PromptCacheRetention.String()
	}

	// NOTE: we can add these in future! There are others...
	// llms.WithThinkingMode()
	// llms.WithCacheControl()
	// llms.WithMaxLength()
	// llms.WithMinLength()
	// llms.WithMaxTokens()

	if r.Model != nil {
		opts = append(opts, llms.WithModel(*r.Model))
	}

	// Openai accepts this as map[string]string but langchain expects map[string]any,
	// so we go with openai for our type opinion here, and therefore I convert accordingly.
	if r.Metadata != nil {
		opts = append(opts, llms.WithMetadata(stringMapToAny(r.Metadata)))
	}

	return opts
}

func stringMapToAny(m map[string]string) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
