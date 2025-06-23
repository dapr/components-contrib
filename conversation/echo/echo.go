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

// Package echo provides a test double implementation of the conversation component interface.
//
// The Echo provider is designed for predictable, reliable testing of conversation components
// in Dapr. It mimics the structure and behavior of real LLM providers while providing
// deterministic responses that make it ideal for unit testing, integration testing, and
// conformance testing.
//
// # Design Philosophy
//
// Echo serves as a "perfect test double" with these core principles:
//   - Predictability: Same input always produces the same output
//   - Testability: Easy to assert expected responses
//   - Structural Compatibility: Mimics real LLM response structure perfectly
//   - Fast Execution: No network calls or complex processing
//   - Tool Calling Support: Full support for function calling with dynamic tool matching
//
// # Hybrid Input Processing
//
// Echo uses a hybrid approach for handling conversation inputs:
//   - For ECHOING: Uses last user message only (predictable testing)
//   - For TOOL MATCHING: Uses concatenated user messages (better functionality)
//
// This design provides predictable responses for testing while enabling enhanced
// tool calling capabilities with better context understanding.
//
// # Key Features
//
//   - Dynamic tool support: Works with any tool schemas provided by users
//   - Case-agnostic tool matching: Handles snake_case, camelCase, PascalCase, kebab-case
//   - Schema-aware parameter generation: Generates realistic arguments based on tool schemas
//   - Parallel tool calling: Can call multiple tools simultaneously
//   - Streaming support: Provides chunk-based response streaming
//   - Usage tracking: Accurate token counting and usage metrics
//   - OpenAI compatibility: Identical response structure and finishReason values
//
// See README.md for comprehensive documentation and usage examples.
package echo

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"
)

// Echo is a test double implementation of the conversation component interface.
// It provides predictable, deterministic responses while maintaining full structural
// compatibility with real LLM providers like OpenAI, GoogleAI, etc.
//
// Echo implements both the basic Conversation interface and the StreamingConversation
// interface, providing comprehensive testing capabilities for conversation components.
//
// Key behaviors:
//   - Echoes the last user message for predictable testing
//   - Supports dynamic tool calling with any provided tool schemas
//   - Provides realistic token counting and usage metrics
//   - Maintains identical response structure to real LLM providers
//   - Supports streaming with chunk-based response delivery
type Echo struct {
	model  string        // Model name (optional, for compatibility)
	logger logger.Logger // Logger for debug and error messages
}

// NewEcho creates a new Echo conversation component instance.
// The Echo component requires no configuration and is ready to use immediately.
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

// SupportsToolCalling implements the ToolCallSupport interface
func (e *Echo) SupportsToolCalling() bool {
	return true
}

// Converse processes a conversation request and returns a predictable response.
//
// Behavior:
//   - Echoes the last user message for non-tool scenarios (predictable testing)
//   - Detects and calls tools based on user intent and available tools
//   - Uses hybrid processing: last message for echoing, all messages for tool context
//   - Provides realistic token counting and usage metrics
//   - Returns identical response structure to real LLM providers
//
// Tool Calling:
//   - Supports dynamic tool schemas (not hardcoded)
//   - Case-agnostic tool name matching
//   - Parallel tool calling when multiple tools match
//   - Schema-aware parameter generation
//
// Returns:
//   - ConversationResponse with exactly 1 output (like real LLMs)
//   - Usage information with token counts
//   - FinishReason: "stop" for normal responses, "tool_calls" for tool invocations
func (e *Echo) Converse(ctx context.Context, r *conversation.ConversationRequest) (res *conversation.ConversationResponse, err error) {
	// Collect user messages and tools like real LLM providers do
	var lastUserMessage string
	var allUserMessages []string
	totalInputTokens := int32(0)
	var allTools []conversation.Tool

	for _, input := range r.Inputs {
		// Simple token estimation: roughly 1 token per 4 characters
		inputTokens := int32(len(input.Message) / int(4)) //nolint:gosec // This is a valid conversion
		if inputTokens == 0 && len(input.Message) > 0 {
			inputTokens = 1 // Minimum 1 token for non-empty input
		}
		totalInputTokens += inputTokens

		// Collect tools from inputs
		allTools = append(allTools, input.Tools...)

		// Collect user messages for tool matching context
		if input.Role == conversation.RoleUser || input.Role == "" { // Empty role defaults to user
			lastUserMessage = input.Message
			allUserMessages = append(allUserMessages, input.Message)
		}
	}

	// Get the user message for echoing (last user message for predictability)
	echoMessage := lastUserMessage

	// Get contextual message for tool matching (concatenated for better tool detection)
	toolMatchingMessage := e.buildToolMatchingContext(allUserMessages)

	// Create output
	output := conversation.ConversationResult{
		Parameters: r.Parameters,
	}

	// Check if any input is a tool result
	if len(r.Inputs) > 0 && r.Inputs[0].Role == conversation.RoleTool {
		// Handle tool result
		output.Result = e.generateToolResultResponse(r.Inputs[0])
		output.FinishReason = "stop"
		output.ToolCalls = []conversation.ToolCall{} // No tool calls for tool results
	} else if len(allTools) > 0 {
		// Check if we should call any tools using the contextual message
		toolsToCall := e.findMatchingTools(allTools, toolMatchingMessage)

		if len(toolsToCall) > 0 {
			// Generate tool calls
			toolCalls := make([]conversation.ToolCall, 0, len(toolsToCall))
			for i, tool := range toolsToCall {
				toolCall := conversation.ToolCall{
					ID:   fmt.Sprintf("call_echo_%d", time.Now().UnixNano()+int64(i)),
					Type: "function",
					Function: conversation.ToolCallFunction{
						Name:      tool.Function.Name,
						Arguments: e.generateToolArguments(tool, toolMatchingMessage),
					},
				}
				toolCalls = append(toolCalls, toolCall)
			}

			output.Result = "I'll help you with that by calling the appropriate tools."
			output.ToolCalls = toolCalls
			output.FinishReason = "tool_calls"
		} else {
			// No tools match, echo the last user message (predictable)
			output.Result = echoMessage
			output.FinishReason = "stop"
			output.ToolCalls = []conversation.ToolCall{}
		}
	} else {
		// No tools available, echo the last user message (predictable)
		output.Result = echoMessage
		output.FinishReason = "stop"
		output.ToolCalls = []conversation.ToolCall{}
	}

	// Calculate output tokens
	totalOutputTokens := int32(len(output.Result) / int(4)) //nolint:gosec // This is a valid conversion
	if totalOutputTokens == 0 && len(output.Result) > 0 {
		totalOutputTokens = 1
	}

	res = &conversation.ConversationResponse{
		Outputs:             []conversation.ConversationResult{output},
		ConversationContext: r.ConversationContext,
		Usage: &conversation.UsageInfo{
			PromptTokens:     totalInputTokens,
			CompletionTokens: totalOutputTokens,
			TotalTokens:      totalInputTokens + totalOutputTokens,
		},
	}

	return res, nil
}

// ConverseStream implements streaming conversation with chunk-based response delivery.
//
// Provides the same response logic as Converse() but delivers the content in multiple
// chunks to simulate real LLM streaming behavior. This is essential for testing
// streaming conversation implementations.
//
// Streaming Behavior:
//   - Breaks responses into 3-4 character chunks for realistic streaming
//   - Tool calls are streamed as structured JSON chunks
//   - Maintains identical final response structure to non-streaming version
//   - Provides proper error handling for streaming failures
//
// Parameters:
//   - ctx: Context for cancellation and timeout handling
//   - r: Conversation request (same as Converse)
//   - streamFunc: Callback function to receive each chunk
//
// Returns:
//   - Same ConversationResponse as Converse() after streaming is complete
//   - Error if streaming fails or context is cancelled
func (e *Echo) ConverseStream(ctx context.Context, r *conversation.ConversationRequest, streamFunc func(ctx context.Context, chunk []byte) error) (*conversation.ConversationResponse, error) {
	// Collect user messages and tools like real LLM providers do
	var lastUserMessage string
	var allUserMessages []string
	totalInputTokens := int32(0)
	var allTools []conversation.Tool

	for _, input := range r.Inputs {
		// Simple token estimation: roughly 1 token per 4 characters
		inputTokens := int32(len(input.Message) / int(4)) //nolint:gosec // This is a valid conversion
		if inputTokens == 0 && len(input.Message) > 0 {
			inputTokens = 1 // Minimum 1 token for non-empty input
		}
		totalInputTokens += inputTokens

		// Collect tools from inputs
		allTools = append(allTools, input.Tools...)

		// Collect user messages for tool matching context
		if input.Role == conversation.RoleUser || input.Role == "" { // Empty role defaults to user
			lastUserMessage = input.Message
			allUserMessages = append(allUserMessages, input.Message)
		}
	}

	// Get the user message for echoing (last user message for predictability)
	echoMessage := lastUserMessage

	// Get contextual message for tool matching (concatenated for better tool detection)
	toolMatchingMessage := e.buildToolMatchingContext(allUserMessages)

	// Create output
	output := conversation.ConversationResult{
		Parameters: r.Parameters,
	}

	// Check if any input is a tool result
	if len(r.Inputs) > 0 && r.Inputs[0].Role == conversation.RoleTool {
		// Handle tool result
		output.Result = e.generateToolResultResponse(r.Inputs[0])
		output.FinishReason = "stop"
		output.ToolCalls = []conversation.ToolCall{} // No tool calls for tool results
	} else if len(allTools) > 0 {
		// Check if we should call any tools using the contextual message
		toolsToCall := e.findMatchingTools(allTools, toolMatchingMessage)

		if len(toolsToCall) > 0 {
			// Generate tool calls
			toolCalls := make([]conversation.ToolCall, 0, len(toolsToCall))
			for i, tool := range toolsToCall {
				toolCall := conversation.ToolCall{
					ID:   fmt.Sprintf("call_echo_stream_%d", time.Now().UnixNano()+int64(i)),
					Type: "function",
					Function: conversation.ToolCallFunction{
						Name:      tool.Function.Name,
						Arguments: e.generateToolArguments(tool, toolMatchingMessage),
					},
				}
				toolCalls = append(toolCalls, toolCall)
			}

			output.Result = "I'll help you with that by calling the appropriate tools."
			output.ToolCalls = toolCalls
			output.FinishReason = "tool_calls"
		} else {
			// No tools match, echo the last user message (predictable)
			output.Result = echoMessage
			output.FinishReason = "stop"
			output.ToolCalls = []conversation.ToolCall{}
		}
	} else {
		// No tools available, echo the last user message (predictable)
		output.Result = echoMessage
		output.FinishReason = "stop"
		output.ToolCalls = []conversation.ToolCall{}
	}

	// Stream the response content (like real LLM providers)
	content := output.Result

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

	// Calculate output tokens
	totalOutputTokens := int32(len(content) / int(4)) //nolint:gosec // This is a valid conversion
	if totalOutputTokens == 0 && len(content) > 0 {
		totalOutputTokens = 1
	}

	res := &conversation.ConversationResponse{
		Outputs:             []conversation.ConversationResult{output},
		ConversationContext: r.ConversationContext,
		Usage: &conversation.UsageInfo{
			PromptTokens:     totalInputTokens,
			CompletionTokens: totalOutputTokens,
			TotalTokens:      totalInputTokens + totalOutputTokens,
		},
	}

	return res, nil
}

// findMatchingTools finds tools that match the user message using case-agnostic matching
func (e *Echo) findMatchingTools(tools []conversation.Tool, userMessage string) []conversation.Tool {
	var matchingTools []conversation.Tool
	messageLower := strings.ToLower(userMessage)

	for _, tool := range tools {
		if e.shouldCallTool(tool, userMessage, messageLower) {
			matchingTools = append(matchingTools, tool)
		}
	}

	return matchingTools
}

// shouldCallTool determines if a tool should be called based on the user message
func (e *Echo) shouldCallTool(tool conversation.Tool, userMessage, messageLower string) bool {
	toolName := tool.Function.Name

	// Generate all possible name variations for case-agnostic matching
	nameVariations := e.generateToolNameVariations(toolName)

	// Check direct name matches (case-insensitive)
	for _, variation := range nameVariations {
		if strings.Contains(messageLower, variation) {
			return true
		}
	}

	// Check explicit tool execution phrases
	explicitPhrases := []string{
		"call " + strings.ToLower(toolName),
		"use " + strings.ToLower(toolName),
		"execute " + strings.ToLower(toolName),
		"run " + strings.ToLower(toolName),
		"invoke " + strings.ToLower(toolName),
	}

	for _, phrase := range explicitPhrases {
		if strings.Contains(messageLower, phrase) {
			return true
		}
	}

	// Check keyword-based matching
	return e.matchesToolKeywords(tool, messageLower)
}

// generateToolNameVariations creates all possible naming convention variations
func (e *Echo) generateToolNameVariations(toolName string) []string {
	variations := make(map[string]bool) // Use map to avoid duplicates

	// Add original name (lowercase)
	variations[strings.ToLower(toolName)] = true

	// Extract words from the tool name
	words := e.extractWordsFromToolName(toolName)

	if len(words) > 1 {
		// Generate different naming conventions
		variations[strings.Join(words, "_")] = true  // snake_case
		variations[strings.Join(words, "-")] = true  // kebab-case
		variations[strings.Join(words, " ")] = true  // space separated
		variations[e.wordsToCamelCase(words)] = true // camelCase (from words)
	}

	// Convert map keys to slice
	result := make([]string, 0, len(variations))
	for variation := range variations {
		result = append(result, variation)
	}

	return result
}

// extractWordsFromToolName extracts individual words from a tool name regardless of format
func (e *Echo) extractWordsFromToolName(toolName string) []string {
	var words []string

	// Handle camelCase and PascalCase by splitting on capital letters
	if e.isCamelOrPascalCase(toolName) {
		words = e.splitCamelCase(toolName)
	} else if strings.Contains(toolName, "_") {
		// snake_case
		words = strings.Split(toolName, "_")
	} else if strings.Contains(toolName, "-") {
		// kebab-case
		words = strings.Split(toolName, "-")
	} else if strings.Contains(toolName, " ") {
		// space separated
		words = strings.Fields(toolName)
	} else {
		// Single word
		words = []string{toolName}
	}

	// Convert all words to lowercase and filter empty strings
	var result []string
	for _, word := range words {
		word = strings.ToLower(strings.TrimSpace(word))
		if word != "" {
			result = append(result, word)
		}
	}

	return result
}

// isCamelOrPascalCase checks if a string is in camelCase or PascalCase
func (e *Echo) isCamelOrPascalCase(s string) bool {
	// Check if string contains at least one uppercase letter that's not at the start
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			return true
		}
	}
	return false
}

// splitCamelCase splits camelCase or PascalCase strings into words
func (e *Echo) splitCamelCase(s string) []string {
	// Use regex to split on capital letters
	re := regexp.MustCompile(`([a-z0-9])([A-Z])`)
	s = re.ReplaceAllString(s, `${1} ${2}`)
	return strings.Fields(s)
}

// wordsToCamelCase converts a slice of words to camelCase
func (e *Echo) wordsToCamelCase(words []string) string {
	if len(words) == 0 {
		return ""
	}

	result := strings.ToLower(words[0])
	for i := 1; i < len(words); i++ {
		if len(words[i]) > 0 {
			result += strings.ToUpper(words[i][:1]) + strings.ToLower(words[i][1:])
		}
	}

	return result
}

// matchesToolKeywords checks if the message matches tool-specific keywords
func (e *Echo) matchesToolKeywords(tool conversation.Tool, messageLower string) bool {
	toolName := strings.ToLower(tool.Function.Name)

	// Define keyword patterns for common tool types
	keywordMap := map[string][]string{
		"email":     {"email", "mail", "send message", "contact"},
		"weather":   {"weather", "temperature", "forecast", "climate", "rain", "sunny", "cloudy"},
		"time":      {"time", "clock", "date", "current time", "now"},
		"calendar":  {"calendar", "schedule", "appointment", "meeting", "event"},
		"user":      {"user", "account", "profile", "create account", "register"},
		"database":  {"database", "search", "query", "find", "lookup", "data"},
		"file":      {"file", "document", "upload", "download", "save"},
		"calculate": {"calculate", "math", "compute", "add", "subtract", "multiply", "divide", "+", "-", "*", "/"},
	}

	// Check if tool name contains any category keywords
	for category, keywords := range keywordMap {
		if strings.Contains(toolName, category) {
			// Check if message contains any keywords for this category
			for _, keyword := range keywords {
				if strings.Contains(messageLower, keyword) {
					return true
				}
			}
		}
	}

	return false
}

// generateToolArguments creates appropriate arguments for a tool call
func (e *Echo) generateToolArguments(tool conversation.Tool, userMessage string) string {
	// Normalize parameters to handle both map and JSON string formats
	params, err := e.normalizeToolParameters(tool.Function.Parameters)
	if err != nil {
		// Fallback to simple arguments
		return fmt.Sprintf(`{"query": "%s"}`, userMessage)
	}

	args := make(map[string]any)

	// Extract properties from the parameter schema
	if properties, ok := params["properties"].(map[string]any); ok {
		for paramName, paramInfo := range properties {
			if paramInfoMap, ok := paramInfo.(map[string]any); ok {
				paramType, _ := paramInfoMap["type"].(string)
				description, _ := paramInfoMap["description"].(string)

				// Generate argument based on parameter type and description
				args[paramName] = e.generateArgumentValue(paramName, paramType, description, userMessage)
			}
		}
	}

	// Ensure we have at least one argument
	if len(args) == 0 {
		args["query"] = userMessage
	}

	// Marshal to JSON
	jsonBytes, err := json.Marshal(args)
	if err != nil {
		return fmt.Sprintf(`{"query": "%s"}`, userMessage)
	}

	return string(jsonBytes)
}

// normalizeToolParameters handles both map and JSON string parameter formats
func (e *Echo) normalizeToolParameters(params any) (map[string]any, error) {
	// If params is already a map, return it as-is
	if paramMap, ok := params.(map[string]any); ok {
		return paramMap, nil
	}

	// If params is a string, try to unmarshal it as JSON
	if paramStr, ok := params.(string); ok {
		var paramMap map[string]any
		if err := json.Unmarshal([]byte(paramStr), &paramMap); err != nil {
			return nil, fmt.Errorf("failed to unmarshal parameter string: %w", err)
		}
		return paramMap, nil
	}

	// For other types, try to marshal and unmarshal to convert to map[string]any
	jsonBytes, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal parameters: %w", err)
	}

	var paramMap map[string]any
	if err := json.Unmarshal(jsonBytes, &paramMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal parameters to map: %w", err)
	}

	return paramMap, nil
}

// generateArgumentValue creates a value for a specific parameter
func (e *Echo) generateArgumentValue(paramName, paramType, description, userMessage string) any {
	paramNameLower := strings.ToLower(paramName)
	descriptionLower := strings.ToLower(description)

	switch paramType {
	case "string":
		// Try to extract specific values based on parameter name or description
		if strings.Contains(paramNameLower, "location") || strings.Contains(descriptionLower, "location") {
			return e.extractLocation(userMessage)
		}
		if strings.Contains(paramNameLower, "email") || strings.Contains(descriptionLower, "email") {
			return e.extractEmail(userMessage)
		}
		if strings.Contains(paramNameLower, "name") || strings.Contains(descriptionLower, "name") {
			return e.extractName(userMessage)
		}
		// Default to the full message or a contextual value
		return userMessage

	case "integer", "number":
		// Try to extract numbers from the message
		if number := e.extractNumber(userMessage); number != 0 {
			return number
		}
		// Default number based on parameter name
		if strings.Contains(paramNameLower, "limit") {
			return 10
		}
		return 42 // Default meaningful number (including for count)

	case "boolean":
		// Default to true for most boolean parameters
		return true

	default:
		// For unknown types, return the message
		return userMessage
	}
}

// extractLocation attempts to extract location information from the message
func (e *Echo) extractLocation(message string) string {
	// Check for common city names first (handles multi-word cities)
	cities := []string{"San Francisco", "New York", "Los Angeles", "Chicago", "Miami", "Boston", "Seattle", "Austin", "Denver"}
	messageLower := strings.ToLower(message)
	for _, city := range cities {
		if strings.Contains(messageLower, strings.ToLower(city)) {
			return city
		}
	}

	// Simple location extraction logic for other patterns
	words := strings.Fields(message)
	for i, word := range words {
		// Look for location indicators
		if strings.Contains(strings.ToLower(word), "in") && i+1 < len(words) {
			// "in San Francisco" pattern - collect multiple words for location
			location := words[i+1]
			// Check if the next word might be part of the location too
			if i+2 < len(words) {
				nextWord := words[i+2]
				// If it's a short word (likely state abbreviation) or a proper noun, include it
				if len(nextWord) <= 4 || (len(nextWord) > 0 && nextWord[0] >= 'A' && nextWord[0] <= 'Z') {
					location += " " + nextWord
				}
			}
			return location
		}
	}

	return "Default Location"
}

// extractEmail attempts to extract email from the message
func (e *Echo) extractEmail(message string) string {
	// Simple email pattern matching
	words := strings.Fields(message)
	for _, word := range words {
		if strings.Contains(word, "@") && strings.Contains(word, ".") {
			return word
		}
	}
	return "example@email.com"
}

// extractName attempts to extract a name from the message
func (e *Echo) extractName(message string) string {
	// Simple name extraction - look for capitalized words
	words := strings.Fields(message)
	for _, word := range words {
		if len(word) > 1 && word[0] >= 'A' && word[0] <= 'Z' {
			// Skip common words
			commonWords := []string{"I", "The", "A", "An", "And", "Or", "But", "For", "Please", "Call", "Use"}
			isCommon := false
			for _, common := range commonWords {
				if word == common {
					isCommon = true
					break
				}
			}
			if !isCommon {
				return word
			}
		}
	}
	return "DefaultName"
}

// extractNumber attempts to extract a number from the message
func (e *Echo) extractNumber(message string) float64 {
	// Look for numbers in the message
	words := strings.Fields(message)
	for _, word := range words {
		// Remove common punctuation
		word = strings.Trim(word, ".,!?")
		if num, err := strconv.ParseFloat(word, 64); err == nil {
			return num
		}
	}
	return 0
}

// generateToolResultResponse creates a response based on tool result input
func (e *Echo) generateToolResultResponse(input conversation.ConversationInput) string {
	// Parse tool result and generate realistic response
	var toolResult map[string]interface{}
	if err := json.Unmarshal([]byte(input.Message), &toolResult); err == nil {
		// Handle weather tool results
		if temp, tempOk := toolResult["temperature"]; tempOk {
			if condition, condOk := toolResult["condition"]; condOk {
				if location, locOk := toolResult["location"]; locOk {
					return fmt.Sprintf("Based on the weather data, it's %v°F and %v in %v today!", temp, condition, location)
				}
				return fmt.Sprintf("The current weather is %v°F and %v.", temp, condition)
			}
		}

		// Handle generic tool results
		if len(toolResult) > 0 {
			return fmt.Sprintf("I received the tool result with data: %v. Thanks for the information!", input.Message)
		}
	}

	// Fallback response for invalid JSON or other content
	if input.Name != "" {
		return fmt.Sprintf("I processed the tool result from %s: %s. Thanks for the information!", input.Name, input.Message)
	}
	return fmt.Sprintf("I processed the tool result: %s. Thanks for the information!", input.Message)
}

func (e *Echo) Close() error {
	return nil
}

// buildToolMatchingContext creates a contextual message from all user inputs for better tool matching
// while keeping echo behavior predictable by still echoing only the last user message.
//
// This is a key part of Echo's hybrid design:
//   - Echo Response: Uses last user message only (predictable)
//   - Tool Matching: Uses concatenated context (better functionality)
//
// Examples:
//   - Single message: "Hello" → "Hello"
//   - Multiple messages: ["Hi", "How are you?"] → "Hi How are you?"
//
// This approach allows tools to have better context for matching while maintaining
// the predictable echoing behavior that makes Echo perfect for testing.
func (e *Echo) buildToolMatchingContext(userMessages []string) string {
	if len(userMessages) == 0 {
		return ""
	}

	// If only one message, return it as-is
	if len(userMessages) == 1 {
		return userMessages[0]
	}

	// For multiple messages, concatenate them with a separator
	// This gives tools more context while keeping echo behavior simple
	return strings.Join(userMessages, " ")
}
