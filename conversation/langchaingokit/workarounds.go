package langchaingokit

import (
	"errors"
	"fmt"
	"strings"

	"github.com/tmc/langchaingo/llms"
)

// NOTE: These are all workarounds due to langchaingo limitations,
// or limitations of certain components (so far only mistral).

// CreateToolCallPart creates mistral and ollama api compatible tool call messages.
// Most LLM providers can handle tool calls using the tool call object;
// however, mistral and ollama requires it as text in conversation history.
// This is due to langchaingo limitations.
func CreateToolCallPart(toolCall *llms.ToolCall) llms.ContentPart {
	if toolCall == nil {
		return nil
	}

	if toolCall.FunctionCall == nil {
		return llms.TextContent{
			Text: "Tool call [ID: " + toolCall.ID + "]: <no function call>",
		}
	}

	return llms.TextContent{
		Text: "Tool call [ID: " + toolCall.ID + "]: " + toolCall.FunctionCall.Name + "(" + toolCall.FunctionCall.Arguments + ")",
	}
}

// CreateToolResponseMessage creates mistral and ollama api compatible tool response message
// using the human role specifically otherwise mistral will reject the tool response message.
// Most LLM providers can handle tool call responses using the tool call response object;
// however, mistral and ollama requires it as text in conversation history.
// This is due to langchaingo limitations.
func CreateToolResponseMessage(responses ...llms.ContentPart) llms.MessageContent {
	msg := llms.MessageContent{
		Role: llms.ChatMessageTypeHuman,
	}
	if len(responses) == 0 {
		return msg
	}
	var toolID, name string

	mistralContentParts := make([]string, 0, len(responses))
	for _, response := range responses {
		if resp, ok := response.(llms.ToolCallResponse); ok {
			if toolID == "" {
				toolID = resp.ToolCallID
			}
			if name == "" {
				name = resp.Name
			}
			mistralContentParts = append(mistralContentParts, resp.Content)
		}
	}
	if len(mistralContentParts) > 0 {
		msg.Parts = []llms.ContentPart{
			llms.TextContent{
				Text: "Tool response [ID: " + toolID + ", Name: " + name + "]: " + strings.Join(mistralContentParts, "\n"),
			},
		}
	}
	return msg
}

// convertToStructuredOutputSchema is an internal helper that converts a JSON schema map to langchaingo's llms.StructuredOutputSchema.
// It recursively processes nested objects and arrays to build the complete schema structure.
func convertToStructuredOutputSchema(schemaMap map[string]any) (*llms.StructuredOutputSchema, error) {
	if schemaMap == nil {
		return nil, errors.New("schema map cannot be nil")
	}

	schemaTypeStr, ok := schemaMap["type"].(string)
	if !ok {
		return nil, errors.New("schema type is required")
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
				// if enum contains non-string values, convert to string
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
					propSchema, err := convertToStructuredOutputSchema(propMap)
					if err != nil {
						return nil, fmt.Errorf("failed to convert property %q: %w", propName, err)
					}
					schema.Properties[propName] = propSchema
				} else {
					return nil, fmt.Errorf("property %q must be a map, got %T", propName, propValue)
				}
			}
		}
	}

	// handle array items
	if schema.Type == llms.SchemaTypeArray {
		if items, ok := schemaMap["items"].(map[string]any); ok {
			itemsSchema, err := convertToStructuredOutputSchema(items)
			if err != nil {
				return nil, fmt.Errorf("failed to convert array items: %w", err)
			}
			schema.Items = itemsSchema
		}
	}

	return schema, nil
}

// convertToStructuredOutputDefinition converts a JSON schema map to langchain's llms.StructuredOutputDefinition.
// Based on langchain's structured output implementation:
// https://github.com/tmc/langchaingo/commit/5b6a093e5995485fdf061609cf987be84be947e2#diff-bb2609d8b74a6201524e3f8c9408b2866e19620c7ee0af3c6c947a5302baf6a1
func convertToStructuredOutputDefinition(jsonSchema map[string]any) (*llms.StructuredOutputDefinition, error) {
	if jsonSchema == nil {
		return nil, errors.New("json schema cannot be nil")
	}

	if _, ok := jsonSchema["type"].(string); !ok {
		return nil, errors.New("schema type is required and must be a string")
	}

	schema, err := convertToStructuredOutputSchema(jsonSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to convert schema: %w", err)
	}

	// build StructuredOutputDefinition
	// name is not a required json schema field, so we use a default value if not provided for langchaingo's llms.StructuredOutputDefinition.Name
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
