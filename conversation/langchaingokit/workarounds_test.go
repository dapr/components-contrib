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
