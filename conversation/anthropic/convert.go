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
package anthropic

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	anthropicsdk "github.com/anthropics/anthropic-sdk-go"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
)

// The dapr conversation surface (Request/Response) is built on langchaingo llms
// types, so these adapters convert between those and the anthropic-sdk-go types
// at the component boundary. That keeps the public contract identical while the
// component talks to the Messages API directly.

var errUnsupportedMessageType = errors.New("unsupported message type")

// buildMessages splits the dapr request messages into a system prompt and the
// anthropic message list. System messages collapse into the top-level system
// string; everything else becomes a MessageParam.
func buildMessages(messages []llms.MessageContent) ([]anthropicsdk.MessageParam, string, error) {
	out := make([]anthropicsdk.MessageParam, 0, len(messages))
	var system strings.Builder

	for _, msg := range messages {
		switch msg.Role {
		case llms.ChatMessageTypeSystem:
			for _, part := range msg.Parts {
				if text, ok := part.(llms.TextContent); ok {
					system.WriteString(text.Text)
				} else {
					return nil, "", fmt.Errorf("anthropic: %w: system message part %T", errUnsupportedMessageType, part)
				}
			}
		case llms.ChatMessageTypeHuman:
			blocks, err := userBlocks(msg.Parts)
			if err != nil {
				return nil, "", err
			}
			out = append(out, anthropicsdk.NewUserMessage(blocks...))
		case llms.ChatMessageTypeAI:
			blocks, err := assistantBlocks(msg.Parts)
			if err != nil {
				return nil, "", err
			}
			out = append(out, anthropicsdk.NewAssistantMessage(blocks...))
		case llms.ChatMessageTypeTool:
			blocks, err := toolResultBlocks(msg.Parts)
			if err != nil {
				return nil, "", err
			}
			// Tool results are sent back to Anthropic on a user-role turn.
			out = append(out, anthropicsdk.NewUserMessage(blocks...))
		default:
			return nil, "", fmt.Errorf("anthropic: %w: %v", errUnsupportedMessageType, msg.Role)
		}
	}

	return out, system.String(), nil
}

func userBlocks(parts []llms.ContentPart) ([]anthropicsdk.ContentBlockParamUnion, error) {
	blocks := make([]anthropicsdk.ContentBlockParamUnion, 0, len(parts))
	for _, part := range parts {
		switch p := part.(type) {
		case llms.TextContent:
			blocks = append(blocks, anthropicsdk.NewTextBlock(p.Text))
		case llms.BinaryContent:
			blocks = append(blocks, anthropicsdk.NewImageBlockBase64(p.MIMEType, base64.StdEncoding.EncodeToString(p.Data)))
		case llms.ToolCallResponse:
			blocks = append(blocks, anthropicsdk.NewToolResultBlock(p.ToolCallID, p.Content, false))
		default:
			return nil, fmt.Errorf("anthropic: %w: human message part %T", errUnsupportedMessageType, part)
		}
	}
	if len(blocks) == 0 {
		return nil, errors.New("anthropic: no valid content in human message")
	}
	return blocks, nil
}

func assistantBlocks(parts []llms.ContentPart) ([]anthropicsdk.ContentBlockParamUnion, error) {
	blocks := make([]anthropicsdk.ContentBlockParamUnion, 0, len(parts))
	for _, part := range parts {
		switch p := part.(type) {
		case llms.TextContent:
			blocks = append(blocks, anthropicsdk.NewTextBlock(p.Text))
		case llms.ToolCall:
			if p.FunctionCall == nil {
				return nil, errors.New("anthropic: assistant tool call is missing a function call")
			}
			var input any
			if args := strings.TrimSpace(p.FunctionCall.Arguments); args != "" {
				if err := json.Unmarshal([]byte(args), &input); err != nil {
					return nil, fmt.Errorf("anthropic: failed to unmarshal tool call arguments: %w", err)
				}
			} else {
				input = map[string]any{}
			}
			blocks = append(blocks, anthropicsdk.NewToolUseBlock(p.ID, input, p.FunctionCall.Name))
		default:
			return nil, fmt.Errorf("anthropic: %w: AI message part %T", errUnsupportedMessageType, part)
		}
	}
	if len(blocks) == 0 {
		return nil, errors.New("anthropic: no valid content in AI message")
	}
	return blocks, nil
}

func toolResultBlocks(parts []llms.ContentPart) ([]anthropicsdk.ContentBlockParamUnion, error) {
	blocks := make([]anthropicsdk.ContentBlockParamUnion, 0, len(parts))
	for _, part := range parts {
		if resp, ok := part.(llms.ToolCallResponse); ok {
			blocks = append(blocks, anthropicsdk.NewToolResultBlock(resp.ToolCallID, resp.Content, false))
			continue
		}
		return nil, fmt.Errorf("anthropic: %w: tool message part %T", errUnsupportedMessageType, part)
	}
	if len(blocks) == 0 {
		return nil, errors.New("anthropic: no valid content in tool message")
	}
	return blocks, nil
}

func buildTools(tools []llms.Tool) ([]anthropicsdk.ToolUnionParam, error) {
	if len(tools) == 0 {
		return nil, nil
	}
	out := make([]anthropicsdk.ToolUnionParam, 0, len(tools))
	for _, tool := range tools {
		if tool.Function == nil {
			return nil, errors.New("anthropic: tool is missing a function definition")
		}
		param := anthropicsdk.ToolParam{
			Name:        tool.Function.Name,
			InputSchema: toolInputSchema(tool.Function.Parameters),
		}
		if tool.Function.Description != "" {
			param.Description = anthropicsdk.String(tool.Function.Description)
		}
		out = append(out, anthropicsdk.ToolUnionParam{OfTool: &param})
	}
	return out, nil
}

// toolInputSchema maps a langchaingo JSON-schema parameters blob onto the SDK's
// ToolInputSchemaParam. Any keys beyond properties/required are preserved via
// ExtraFields so custom schema attributes survive the round trip.
func toolInputSchema(parameters any) anthropicsdk.ToolInputSchemaParam {
	schema := anthropicsdk.ToolInputSchemaParam{}
	params, ok := parameters.(map[string]any)
	if !ok {
		return schema
	}

	if props, ok := params["properties"]; ok {
		schema.Properties = props
	}
	if required, ok := params["required"]; ok {
		schema.Required = toStringSlice(required)
	}

	var extra map[string]any
	for k, v := range params {
		switch k {
		case "type", "properties", "required":
			continue
		default:
			if extra == nil {
				extra = map[string]any{}
			}
			extra[k] = v
		}
	}
	schema.ExtraFields = extra

	return schema
}

func toStringSlice(v any) []string {
	switch vals := v.(type) {
	case []string:
		return vals
	case []any:
		out := make([]string, 0, len(vals))
		for _, item := range vals {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

// buildToolChoice maps the dapr tool_choice string onto the SDK union. The value
// follows the same conventions the other conversation components accept: auto,
// none, required/any, or a specific tool name. Anthropic never received the
// value through the pinned langchaingo, so this passthrough is the point.
func buildToolChoice(toolChoice *string) anthropicsdk.ToolChoiceUnionParam {
	if toolChoice == nil {
		return anthropicsdk.ToolChoiceUnionParam{}
	}

	switch strings.ToLower(strings.TrimSpace(*toolChoice)) {
	case "", "auto":
		return anthropicsdk.ToolChoiceUnionParam{OfAuto: &anthropicsdk.ToolChoiceAutoParam{}}
	case "none":
		return anthropicsdk.ToolChoiceUnionParam{OfNone: &anthropicsdk.ToolChoiceNoneParam{}}
	case "required", "any":
		return anthropicsdk.ToolChoiceUnionParam{OfAny: &anthropicsdk.ToolChoiceAnyParam{}}
	default:
		return anthropicsdk.ToolChoiceUnionParam{OfTool: &anthropicsdk.ToolChoiceToolParam{Name: *toolChoice}}
	}
}

// buildResponse maps an Anthropic Message back onto the dapr conversation
// response, collecting text and tool calls into a single choice.
func buildResponse(model string, msg *anthropicsdk.Message) *conversation.Response {
	var text strings.Builder
	var toolCalls []llms.ToolCall

	for i := range msg.Content {
		block := msg.Content[i]
		switch block.Type {
		case "text":
			if block.Text != "" {
				if text.Len() > 0 {
					text.WriteString("\n")
				}
				text.WriteString(block.Text)
			}
		case "tool_use":
			toolCalls = append(toolCalls, llms.ToolCall{
				ID:   block.ID,
				Type: "function",
				FunctionCall: &llms.FunctionCall{
					Name:      block.Name,
					Arguments: string(block.Input),
				},
			})
		}
	}

	finishReason := normalizeFinishReason(string(msg.StopReason))

	choice := conversation.Choice{
		FinishReason: finishReason,
		Index:        0,
		Message: conversation.Message{
			Content: text.String(),
		},
	}
	if len(toolCalls) > 0 {
		choice.Message.ToolCallRequest = &toolCalls
	}

	return &conversation.Response{
		Model: model,
		Outputs: []conversation.Result{
			{
				StopReason: finishReason,
				Choices:    []conversation.Choice{choice},
			},
		},
		Usage: buildUsage(msg.Usage),
	}
}

func buildUsage(usage anthropicsdk.Usage) *conversation.Usage {
	if usage.InputTokens == 0 && usage.OutputTokens == 0 &&
		usage.CacheReadInputTokens == 0 && usage.CacheCreationInputTokens == 0 {
		return nil
	}

	out := &conversation.Usage{
		PromptTokens:     safeUint64(usage.InputTokens),
		CompletionTokens: safeUint64(usage.OutputTokens),
		TotalTokens:      safeUint64(usage.InputTokens + usage.OutputTokens),
	}

	if usage.CacheReadInputTokens > 0 {
		out.PromptTokensDetails = &conversation.PromptTokensDetails{
			CachedTokens: safeUint64(usage.CacheReadInputTokens),
		}
	}

	return out
}

// safeUint64 converts a token count to uint64, clamping negatives to 0. Token
// counts are never negative, but this keeps the conversion overflow-safe.
func safeUint64(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

// normalizeFinishReason mirrors the langchaingokit behavior: an empty stop
// reason is reported as "unknown" rather than an empty string.
func normalizeFinishReason(stopReason string) string {
	if stopReason == "" {
		return "unknown"
	}
	return stopReason
}
