/*
Copyright 2026 The Dapr Authors
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
	"strings"

	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
)

// thinkingContentKey is where langchaingo's Anthropic client reports thinking,
// which it does not copy into ContentChoice.ReasoningContent.
const thinkingContentKey = "ThinkingContent"

// ReasoningEffortOptions translates a request-level reasoning effort into the
// call options a provider understands. It returns nil for an effort the
// provider does not support, which leaves the provider default in effect.
type ReasoningEffortOptions func(effort string) []llms.CallOption

// SetReasoningEffortOptions opts the component into request-level reasoning
// effort. Without it the effort is ignored: enabling reasoning has
// provider-specific side effects, so only the component can decide the mapping.
func (a *LLM) SetReasoningEffortOptions(fn ReasoningEffortOptions) {
	a.reasoningEffortOptions = fn
}

func (a *LLM) reasoningEffortCallOptions(effort *string) []llms.CallOption {
	if effort == nil || *effort == "" {
		return nil
	}
	if a.reasoningEffortOptions == nil {
		a.logger.Debugf("ignoring reasoning effort %q: the component does not support it", *effort)
		return nil
	}
	return a.reasoningEffortOptions(*effort)
}

// SplitReasoning removes the conversation.ReasoningContent parts from messages
// and returns them separately, joined per message index, so that the messages
// can be handed to langchaingo, which does not know that part type. The input
// slice is not modified. The map is nil when no message carries reasoning.
func SplitReasoning(messages []llms.MessageContent) ([]llms.MessageContent, map[int]string) {
	var reasoning map[int]string
	cleaned := make([]llms.MessageContent, len(messages))
	for i, message := range messages {
		cleaned[i] = message

		var texts []string
		parts := make([]llms.ContentPart, 0, len(message.Parts))
		for _, part := range message.Parts {
			if r, ok := part.(conversation.ReasoningContent); ok {
				texts = append(texts, r.Text)
				continue
			}
			parts = append(parts, part)
		}
		if len(texts) == 0 {
			continue
		}

		cleaned[i].Parts = parts
		if reasoning == nil {
			reasoning = make(map[int]string)
		}
		reasoning[i] = strings.Join(texts, "\n")
	}
	return cleaned, reasoning
}

// reasoningFromChoice returns the reasoning text the provider attached to a
// choice, wherever its langchaingo client put it.
func reasoningFromChoice(choice *llms.ContentChoice) string {
	if choice.ReasoningContent != "" {
		return choice.ReasoningContent
	}
	if thinking, ok := choice.GenerationInfo[thinkingContentKey].(string); ok {
		return thinking
	}
	return ""
}
