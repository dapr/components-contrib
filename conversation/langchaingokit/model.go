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
	"fmt"

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

func (a *LLM) NormalizeConverseResult(choices []*llms.ContentChoice) ([]conversation.Result, *conversation.Usage) {
	if len(choices) == 0 {
		return nil, nil
	}

	// Extract usage from the first choice's GenerationInfo (all choices share the same usage)
	var usage *conversation.Usage
	if len(choices) > 0 && choices[0].GenerationInfo != nil {
		usage = extractUsageFromGenerationInfo(choices[0].GenerationInfo)
	}

	outputs := make([]conversation.Result, 0, len(choices))
	for i := range choices {
		choice := conversation.Choice{
			FinishReason: choices[i].StopReason,
			Index:        int64(i),
		}

		if choices[i].Content != "" {
			choice.Message.Content = choices[i].Content
		}

		if choices[i].ToolCalls != nil {
			choice.Message.ToolCallRequest = &choices[i].ToolCalls
		}

		output := conversation.Result{
			StopReason: choices[i].StopReason,
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

	if r.ResponseFormatAsJsonSchema != nil {
		structuredOutput, err := convertJsonSchemaForLangchain(r.ResponseFormatAsJsonSchema)
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

// convertJsonSchemaForLangchain converts a JSON schema map to langchain's llms.StructuredOutputDefinition
// Based on langchain's structured output implementation:
// https://github.com/tmc/langchaingo/commit/5b6a093e5995485fdf061609cf987be84be947e2#diff-bb2609d8b74a6201524e3f8c9408b2866e19620c7ee0af3c6c947a5302baf6a1
func convertJsonSchemaForLangchain(jsonSchema map[string]any) (*llms.StructuredOutputDefinition, error) {
	if jsonSchema == nil {
		return nil, fmt.Errorf("json schema cannot be nil")
	}

	if _, ok := jsonSchema["type"].(string); !ok {
		return nil, fmt.Errorf("schema type is required and must be a string")
	}

	schema, err := convertSchemaForLangchain(jsonSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to convert schema: %w", err)
	}

	// build StructuredOutputDefinition
	// use default name and description if not provided
	name := "response"
	if n, ok := jsonSchema["name"].(string); ok && n != "" {
		name = n
	}

	description := ""
	if d, ok := jsonSchema["description"].(string); ok {
		description = d
	}

	strict := false
	if s, ok := jsonSchema["strict"].(bool); ok {
		strict = s
	}

	return &llms.StructuredOutputDefinition{
		Name:        name,
		Description: description,
		Schema:      schema,
		Strict:      strict,
	}, nil
}

// convertSchemaForLangchain converts a JSON schema map to llms.StructuredOutputSchema
func convertSchemaForLangchain(schemaMap map[string]any) (*llms.StructuredOutputSchema, error) {
	if schemaMap == nil {
		return nil, fmt.Errorf("schema map cannot be nil")
	}

	schemaTypeStr, ok := schemaMap["type"].(string)
	if !ok {
		return nil, fmt.Errorf("schema type is required")
	}

	schema := &llms.StructuredOutputSchema{
		Type: llms.SchemaType(schemaTypeStr),
	}

	if desc, ok := schemaMap["description"].(string); ok {
		schema.Description = desc
	}

	if required, ok := schemaMap["required"].([]any); ok {
		schema.Required = make([]string, 0, len(required))
		for _, r := range required {
			if rStr, ok := r.(string); ok {
				schema.Required = append(schema.Required, rStr)
			}
		}
	}

	if additionalProps, ok := schemaMap["additionalProperties"].(bool); ok {
		schema.AdditionalProperties = additionalProps
	}

	// extract enum - convert to []string if needed
	if enum, ok := schemaMap["enum"].([]any); ok {
		enumStrings := make([]string, 0, len(enum))
		for _, e := range enum {
			if eStr, ok := e.(string); ok {
				enumStrings = append(enumStrings, eStr)
			} else {
				// if enum contains non-string values, convert to string representation
				enumStrings = append(enumStrings, fmt.Sprintf("%v", e))
			}
		}
		if len(enumStrings) > 0 {
			schema.Enum = enumStrings
		}
	}

	if schema.Type == llms.SchemaTypeObject {
		if properties, ok := schemaMap["properties"].(map[string]any); ok {
			schema.Properties = make(map[string]*llms.StructuredOutputSchema)
			for propName, propValue := range properties {
				if propMap, ok := propValue.(map[string]any); ok {
					propSchema, err := convertSchemaForLangchain(propMap)
					if err != nil {
						return nil, fmt.Errorf("failed to convert property %q: %w", propName, err)
					}
					schema.Properties[propName] = propSchema
				}
			}
		}
	}

	// handle array items
	if schema.Type == llms.SchemaTypeArray {
		if items, ok := schemaMap["items"].(map[string]any); ok {
			itemsSchema, err := convertSchemaForLangchain(items)
			if err != nil {
				return nil, fmt.Errorf("failed to convert array items: %w", err)
			}
			schema.Items = itemsSchema
		}
	}

	return schema, nil
}

// extractInt64FromGenInfo extracts an int64 value from genInfo map to extract usage data from langchaingo's GenerationInfo map in the choices response.
func extractInt64FromGenInfo(genInfo map[string]any, key string) int64 {
	if v, ok := genInfo[key].(int64); ok {
		return v
	}
	return 0
}

// extractUsageFromGenerationInfo extracts usage statistics from langchaingo's GenerationInfo map.
// Magic strings are based on the fields here:
// ref: https://github.com/openai/openai-go/blob/main/completion.go#L192 for CompletionUsageCompletionTokensDetails
// ref: https://github.com/openai/openai-go/blob/main/completion.go#L162 for CompletionUsagePromptTokensDetails
func extractUsageFromGenerationInfo(genInfo map[string]any) *conversation.Usage {
	if genInfo == nil {
		return nil
	}

	usage := &conversation.Usage{}
	usage.CompletionTokens = extractInt64FromGenInfo(genInfo, "CompletionTokens")
	usage.PromptTokens = extractInt64FromGenInfo(genInfo, "PromptTokens")
	usage.TotalTokens = extractInt64FromGenInfo(genInfo, "TotalTokens")

	completionDetails := &conversation.CompletionTokensDetails{
		AcceptedPredictionTokens: extractInt64FromGenInfo(genInfo, "CompletionAcceptedPredictionTokens"),
		AudioTokens:              extractInt64FromGenInfo(genInfo, "CompletionAudioTokens"),
		ReasoningTokens:          extractInt64FromGenInfo(genInfo, "CompletionReasoningTokens"),
		RejectedPredictionTokens: extractInt64FromGenInfo(genInfo, "CompletionRejectedPredictionTokens"),
	}

	if completionDetails.AcceptedPredictionTokens > 0 || completionDetails.AudioTokens > 0 ||
		completionDetails.ReasoningTokens > 0 || completionDetails.RejectedPredictionTokens > 0 {
		usage.CompletionTokensDetails = completionDetails
	}

	promptDetails := &conversation.PromptTokensDetails{
		AudioTokens:  extractInt64FromGenInfo(genInfo, "PromptAudioTokens"),
		CachedTokens: extractInt64FromGenInfo(genInfo, "PromptCachedTokens"),
	}

	if promptDetails.AudioTokens > 0 || promptDetails.CachedTokens > 0 {
		usage.PromptTokensDetails = promptDetails
	}

	// Only return usage if we have at least some data
	if usage.CompletionTokens > 0 || usage.PromptTokens > 0 || usage.TotalTokens > 0 {
		return usage
	}

	return nil
}
