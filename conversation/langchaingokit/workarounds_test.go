package langchaingokit

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

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
)

func TestConvertToStructuredOutputDefinition(t *testing.T) {
	tests := []struct {
		name     string
		schema   map[string]any
		wantErr  bool
		validate func(t *testing.T, result *llms.StructuredOutputDefinition, err error)
	}{
		{
			name:    "nil input",
			schema:  nil,
			wantErr: true,
		},
		{
			name: "missing type",
			schema: map[string]any{
				"description": "test schema",
			},
			wantErr: true,
		},
		{
			name: "invalid type format",
			schema: map[string]any{
				"type": 123, // not a string
			},
			wantErr: true,
		},
		{
			name: "simple schema with defaults",
			schema: map[string]any{
				"type": "string",
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputDefinition, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, "response", result.Name)
				assert.Equal(t, "", result.Description)
				assert.False(t, result.Strict)
				assert.NotNil(t, result.Schema)
				assert.Equal(t, llms.SchemaTypeString, result.Schema.Type)
			},
		},
		{
			name: "full strict schema",
			schema: map[string]any{
				"type":        "object",
				"name":        "user_info",
				"description": "User information schema",
				"strict":      true,
				"properties": map[string]any{
					"name": map[string]any{
						"type": "string",
					},
				},
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputDefinition, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, "user_info", result.Name)
				assert.Equal(t, "User information schema", result.Description)
				assert.True(t, result.Strict)
				assert.NotNil(t, result.Schema)
				assert.Equal(t, llms.SchemaTypeObject, result.Schema.Type)
			},
		},
		{
			name: "empty name uses default",
			schema: map[string]any{
				"type": "string",
				"name": "",
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputDefinition, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, "response", result.Name)
			},
		},
		{
			name: "complex nested object schema",
			schema: map[string]any{
				"type":        "object",
				"name":        "complex_schema",
				"description": "Complex nested schema",
				"properties": map[string]any{
					"user": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"name": map[string]any{
								"type": "string",
							},
							"age": map[string]any{
								"type": "integer",
							},
						},
						"required": []any{"name"},
					},
				},
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputDefinition, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, "complex_schema", result.Name)
				assert.NotNil(t, result.Schema.Properties)
				assert.NotNil(t, result.Schema.Properties["user"])
				assert.Equal(t, llms.SchemaTypeObject, result.Schema.Properties["user"].Type)
			},
		},
		{
			name: "array schema",
			schema: map[string]any{
				"type": "array",
				"items": map[string]any{
					"type": "string",
				},
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputDefinition, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, "response", result.Name)
				assert.Equal(t, llms.SchemaTypeArray, result.Schema.Type)
				assert.NotNil(t, result.Schema.Items)
				assert.Equal(t, llms.SchemaTypeString, result.Schema.Items.Type)
			},
		},
		{
			name: "invalid property format",
			schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"invalid": "not a map", // invalid property format here
					"valid": map[string]any{
						"type": "string",
					},
				},
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputDefinition, err error) {
				require.Error(t, err)
				assert.Nil(t, result)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertToStructuredOutputDefinition(tt.schema)
			if tt.wantErr {
				assert.Nil(t, result)
				assert.Error(t, err)
			} else if tt.validate != nil {
				tt.validate(t, result, err)
			}
		})
	}
}

func TestConvertToStructuredOutputSchema(t *testing.T) {
	tests := []struct {
		name     string
		schema   map[string]any
		wantErr  bool
		validate func(t *testing.T, result *llms.StructuredOutputSchema, err error)
	}{
		{
			name:    "nil input",
			schema:  nil,
			wantErr: true,
		},
		{
			name: "missing type",
			schema: map[string]any{
				"description": "test",
			},
			wantErr: true,
		},
		{
			name: "invalid type format",
			schema: map[string]any{
				"type": 123,
			},
			wantErr: true,
		},
		{
			name: "simple string type",
			schema: map[string]any{
				"type":        "string",
				"description": "A string field",
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, llms.SchemaTypeString, result.Type)
				assert.Equal(t, "A string field", result.Description)
			},
		},
		{
			name: "integer type",
			schema: map[string]any{
				"type": "integer",
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.NoError(t, err)
				assert.Equal(t, llms.SchemaTypeInteger, result.Type)
			},
		},
		{
			name: "number type",
			schema: map[string]any{
				"type": "number",
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.NoError(t, err)
				assert.Equal(t, llms.SchemaTypeNumber, result.Type)
			},
		},
		{
			name: "boolean type",
			schema: map[string]any{
				"type": "boolean",
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.NoError(t, err)
				assert.Equal(t, llms.SchemaTypeBoolean, result.Type)
			},
		},
		{
			name: "object with properties",
			schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name": map[string]any{
						"type": "string",
					},
					"age": map[string]any{
						"type": "integer",
					},
				},
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.NoError(t, err)
				assert.Equal(t, llms.SchemaTypeObject, result.Type)
				assert.NotNil(t, result.Properties)
				assert.Len(t, result.Properties, 2)
				assert.Equal(t, llms.SchemaTypeString, result.Properties["name"].Type)
				assert.Equal(t, llms.SchemaTypeInteger, result.Properties["age"].Type)
			},
		},
		{
			name: "object with required fields",
			schema: map[string]any{
				"type":     "object",
				"required": []any{"name", "email"},
				"properties": map[string]any{
					"name": map[string]any{
						"type": "string",
					},
					"email": map[string]any{
						"type": "string",
					},
				},
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.NoError(t, err)
				assert.Equal(t, []string{"name", "email"}, result.Required)
			},
		},
		{
			name: "required fields with non-string values are skipped",
			schema: map[string]any{
				"type":     "object",
				"required": []any{"name", 123, "email"},
				"properties": map[string]any{
					"name": map[string]any{
						"type": "string",
					},
				},
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.NoError(t, err)
				assert.Equal(t, []string{"name", "email"}, result.Required)
			},
		},
		{
			name: "additionalProperties false",
			schema: map[string]any{
				"type":                 "object",
				"additionalProperties": false,
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.NoError(t, err)
				assert.False(t, result.AdditionalProperties)
			},
		},
		{
			name: "additionalProperties true",
			schema: map[string]any{
				"type":                 "object",
				"additionalProperties": true,
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.NoError(t, err)
				assert.True(t, result.AdditionalProperties)
			},
		},
		{
			name: "enum with string values",
			schema: map[string]any{
				"type": "string",
				"enum": []any{"option1", "option2", "option3"},
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, []string{"option1", "option2", "option3"}, result.Enum)
			},
		},
		{
			name: "enum with mixed types converts to strings",
			schema: map[string]any{
				"type": "string",
				"enum": []any{"option1", 123, true},
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, []string{"option1", "123", "true"}, result.Enum)
			},
		},
		{
			name: "array with items",
			schema: map[string]any{
				"type": "array",
				"items": map[string]any{
					"type": "string",
				},
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.NoError(t, err)
				assert.Equal(t, llms.SchemaTypeArray, result.Type)
				assert.NotNil(t, result.Items)
				assert.Equal(t, llms.SchemaTypeString, result.Items.Type)
			},
		},
		{
			name: "nested object properties",
			schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"user": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"name": map[string]any{
								"type": "string",
							},
							"address": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"street": map[string]any{
										"type": "string",
									},
								},
							},
						},
					},
				},
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.NoError(t, err)
				assert.NotNil(t, result.Properties["user"])
				assert.NotNil(t, result.Properties["user"].Properties["address"])
				assert.NotNil(t, result.Properties["user"].Properties["address"].Properties["street"])
			},
		},
		{
			name: "array of objects",
			schema: map[string]any{
				"type": "array",
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"id": map[string]any{
							"type": "integer",
						},
					},
				},
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.NoError(t, err)
				assert.Equal(t, llms.SchemaTypeArray, result.Type)
				assert.Equal(t, llms.SchemaTypeObject, result.Items.Type)
				assert.NotNil(t, result.Items.Properties["id"])
			},
		},
		{
			name: "invalid property format",
			schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"invalid": "not a map", // invalid property format here
					"valid": map[string]any{
						"type": "string",
					},
				},
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.Error(t, err)
				assert.Nil(t, result)
			},
		},
		{
			name: "invalid array items format",
			schema: map[string]any{
				"type":  "array",
				"items": "not a map",
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.NoError(t, err)
				assert.Nil(t, result.Items)
			},
		},
		{
			name: "nested property with invalid format",
			schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"nested": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"invalid": "not a map", // invalid property format here
							"valid": map[string]any{
								"type": "string",
							},
						},
					},
				},
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.Error(t, err)
				assert.Nil(t, result)
			},
		},
		{
			name: "nested array items with invalid property",
			schema: map[string]any{
				"type": "array",
				"items": map[string]any{
					"type": "array",
					"items": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"invalid": "not a map", // invalid property format here
							"valid": map[string]any{
								"type": "string",
							},
						},
					},
				},
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.Error(t, err)
				assert.Nil(t, result)
			},
		},
		{
			name: "object without properties",
			schema: map[string]any{
				"type": "object",
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.NoError(t, err)
				assert.Equal(t, llms.SchemaTypeObject, result.Type)
				assert.Nil(t, result.Properties)
			},
		},
		{
			name: "array without items",
			schema: map[string]any{
				"type": "array",
			},
			wantErr: false,
			validate: func(t *testing.T, result *llms.StructuredOutputSchema, err error) {
				require.NoError(t, err)
				assert.Equal(t, llms.SchemaTypeArray, result.Type)
				assert.Nil(t, result.Items)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertToStructuredOutputSchema(tt.schema)
			if tt.wantErr {
				assert.Nil(t, result)
				assert.Error(t, err)
			} else if tt.validate != nil {
				tt.validate(t, result, err)
			}
		})
	}
}

func TestExtractInt64FromGenInfo(t *testing.T) {
	tests := []struct {
		name     string
		genInfo  map[string]any
		key      string
		expected int64
	}{
		{
			name: "extract int64 value",
			genInfo: map[string]any{
				"CompletionTokens": int64(100),
			},
			key:      "CompletionTokens",
			expected: int64(100),
		},
		{
			name: "missing key",
			genInfo: map[string]any{
				"OtherKey": int64(50),
			},
			key:      "CompletionTokens",
			expected: int64(0),
		},
		{
			name:     "nil genInfo returns zero",
			genInfo:  nil,
			key:      "CompletionTokens",
			expected: int64(0),
		},
		{
			name: "wrong type returns zero",
			genInfo: map[string]any{
				"CompletionTokens": "not an int",
			},
			key:      "CompletionTokens",
			expected: int64(0),
		},
		{
			name: "zero value",
			genInfo: map[string]any{
				"CompletionTokens": int64(0),
			},
			key:      "CompletionTokens",
			expected: int64(0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractInt64FromGenInfo(tt.genInfo, tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractUsageFromLangchainGenerationInfo(t *testing.T) {
	tests := []struct {
		name     string
		genInfo  map[string]any
		validate func(t *testing.T, result *conversation.Usage)
	}{
		{
			name:    "nil genInfo",
			genInfo: nil,
			validate: func(t *testing.T, result *conversation.Usage) {
				assert.Nil(t, result)
			},
		},
		{
			name:    "empty genInfo",
			genInfo: map[string]any{},
			validate: func(t *testing.T, result *conversation.Usage) {
				assert.Nil(t, result)
			},
		},
		{
			name: "basic usage",
			genInfo: map[string]any{
				"CompletionTokens": int64(100),
				"PromptTokens":     int64(50),
				"TotalTokens":      int64(150),
			},
			validate: func(t *testing.T, result *conversation.Usage) {
				require.NotNil(t, result)
				assert.Equal(t, int64(100), result.CompletionTokens)
				assert.Equal(t, int64(50), result.PromptTokens)
				assert.Equal(t, int64(150), result.TotalTokens)
				assert.Nil(t, result.CompletionTokensDetails)
				assert.Nil(t, result.PromptTokensDetails)
			},
		},
		{
			name: "usage with completion token details",
			genInfo: map[string]any{
				"CompletionTokens":                   int64(200),
				"PromptTokens":                       int64(100),
				"TotalTokens":                        int64(300),
				"CompletionAcceptedPredictionTokens": int64(10),
				"CompletionAudioTokens":              int64(5),
				"CompletionReasoningTokens":          int64(15),
				"CompletionRejectedPredictionTokens": int64(2),
			},
			validate: func(t *testing.T, result *conversation.Usage) {
				require.NotNil(t, result)
				assert.Equal(t, int64(200), result.CompletionTokens)
				assert.NotNil(t, result.CompletionTokensDetails)
				assert.Equal(t, int64(10), result.CompletionTokensDetails.AcceptedPredictionTokens)
				assert.Equal(t, int64(5), result.CompletionTokensDetails.AudioTokens)
				assert.Equal(t, int64(15), result.CompletionTokensDetails.ReasoningTokens)
				assert.Equal(t, int64(2), result.CompletionTokensDetails.RejectedPredictionTokens)
				assert.Nil(t, result.PromptTokensDetails)
			},
		},
		{
			name: "usage with prompt token details",
			genInfo: map[string]any{
				"CompletionTokens":   int64(150),
				"PromptTokens":       int64(75),
				"TotalTokens":        int64(225),
				"PromptAudioTokens":  int64(10),
				"PromptCachedTokens": int64(20),
			},
			validate: func(t *testing.T, result *conversation.Usage) {
				require.NotNil(t, result)
				assert.Equal(t, int64(150), result.CompletionTokens)
				assert.Nil(t, result.CompletionTokensDetails)
				assert.NotNil(t, result.PromptTokensDetails)
				assert.Equal(t, int64(10), result.PromptTokensDetails.AudioTokens)
				assert.Equal(t, int64(20), result.PromptTokensDetails.CachedTokens)
			},
		},
		{
			name: "usage with all details",
			genInfo: map[string]any{
				"CompletionTokens":                   int64(250),
				"PromptTokens":                       int64(125),
				"TotalTokens":                        int64(375),
				"CompletionAcceptedPredictionTokens": int64(20),
				"CompletionAudioTokens":              int64(8),
				"CompletionReasoningTokens":          int64(25),
				"CompletionRejectedPredictionTokens": int64(3),
				"PromptAudioTokens":                  int64(15),
				"PromptCachedTokens":                 int64(30),
			},
			validate: func(t *testing.T, result *conversation.Usage) {
				require.NotNil(t, result)
				assert.Equal(t, int64(250), result.CompletionTokens)
				assert.Equal(t, int64(125), result.PromptTokens)
				assert.Equal(t, int64(375), result.TotalTokens)
				assert.NotNil(t, result.CompletionTokensDetails)
				assert.Equal(t, int64(20), result.CompletionTokensDetails.AcceptedPredictionTokens)
				assert.Equal(t, int64(8), result.CompletionTokensDetails.AudioTokens)
				assert.Equal(t, int64(25), result.CompletionTokensDetails.ReasoningTokens)
				assert.Equal(t, int64(3), result.CompletionTokensDetails.RejectedPredictionTokens)
				assert.NotNil(t, result.PromptTokensDetails)
				assert.Equal(t, int64(15), result.PromptTokensDetails.AudioTokens)
				assert.Equal(t, int64(30), result.PromptTokensDetails.CachedTokens)
			},
		},
		{
			name: "completion details with zero values are not included",
			genInfo: map[string]any{
				"CompletionTokens":                   int64(100),
				"PromptTokens":                       int64(50),
				"TotalTokens":                        int64(150),
				"CompletionAcceptedPredictionTokens": int64(0),
				"CompletionAudioTokens":              int64(0),
				"CompletionReasoningTokens":          int64(0),
				"CompletionRejectedPredictionTokens": int64(0),
			},
			validate: func(t *testing.T, result *conversation.Usage) {
				require.NotNil(t, result)
				assert.Nil(t, result.CompletionTokensDetails)
			},
		},
		{
			name: "prompt details with zero values are not included",
			genInfo: map[string]any{
				"CompletionTokens":   int64(100),
				"PromptTokens":       int64(50),
				"TotalTokens":        int64(150),
				"PromptAudioTokens":  int64(0),
				"PromptCachedTokens": int64(0),
			},
			validate: func(t *testing.T, result *conversation.Usage) {
				require.NotNil(t, result)
				assert.Nil(t, result.PromptTokensDetails)
			},
		},
		{
			name: "completion details included if any field is non-zero",
			genInfo: map[string]any{
				"CompletionTokens":                   int64(100),
				"PromptTokens":                       int64(50),
				"TotalTokens":                        int64(150),
				"CompletionAcceptedPredictionTokens": int64(0),
				"CompletionAudioTokens":              int64(5),
				"CompletionReasoningTokens":          int64(0),
				"CompletionRejectedPredictionTokens": int64(0),
			},
			validate: func(t *testing.T, result *conversation.Usage) {
				require.NotNil(t, result)
				assert.NotNil(t, result.CompletionTokensDetails)
				assert.Equal(t, int64(5), result.CompletionTokensDetails.AudioTokens)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractUsageFromLangchainGenerationInfo(tt.genInfo)
			tt.validate(t, result)
		})
	}
}

func TestStringMapToAny(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		validate func(t *testing.T, result map[string]any)
	}{
		{
			name:  "nil map returns nil",
			input: nil,
			validate: func(t *testing.T, result map[string]any) {
				assert.Nil(t, result)
			},
		},
		{
			name:  "empty map returns empty map",
			input: map[string]string{},
			validate: func(t *testing.T, result map[string]any) {
				assert.NotNil(t, result)
				assert.Empty(t, result)
			},
		},
		{
			name: "converts map[string]string to map[string]any",
			input: map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			},
			validate: func(t *testing.T, result map[string]any) {
				require.NotNil(t, result)
				assert.Len(t, result, 3)
				assert.Equal(t, "value1", result["key1"])
				assert.Equal(t, "value2", result["key2"])
				assert.Equal(t, "value3", result["key3"])
				// Verify types
				for _, v := range result {
					_, ok := v.(string)
					assert.True(t, ok, "All values should be strings")
				}
			},
		},
		{
			name: "preserves all key-value pairs",
			input: map[string]string{
				"metadata1": "data1",
				"metadata2": "data2",
			},
			validate: func(t *testing.T, result map[string]any) {
				assert.Equal(t, "data1", result["metadata1"])
				assert.Equal(t, "data2", result["metadata2"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stringMapToAny(tt.input)
			tt.validate(t, result)
		})
	}
}
