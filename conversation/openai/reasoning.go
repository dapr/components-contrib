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

package openai

import (
	"github.com/tmc/langchaingo/llms"
)

// reasoningStrengths are the strengths langchaingo maps back onto OpenAI's
// reasoning_effort values (below 0.34 is low, below 0.67 medium, else high).
// langchaingo has no way to send any other value, such as minimal or xhigh.
var reasoningStrengths = map[string]float64{
	"low":    0.2,
	"medium": 0.5,
	"high":   0.9,
}

// reasoningEffortOptions translates a request-level reasoning effort into the
// option langchaingo turns into reasoning_effort, or nil when it cannot.
func reasoningEffortOptions(effort string) []llms.CallOption {
	strength, ok := reasoningStrengths[effort]
	if !ok {
		return nil
	}
	return []llms.CallOption{llms.WithReasoningStrength(strength)}
}
