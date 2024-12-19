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

package cmd

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
)

func TestGetCmdFlags(t *testing.T) {
	tests := []struct {
		name           string
		flags          map[string]interface{}
		expectedResult *cliFlags
	}{
		{
			name: "All flags set",
			flags: map[string]interface{}{
				"type":        "state",
				"builtinAuth": []string{"auth1", "auth2"},
				"status":      "stable",
				"version":     "v1",
				"direction":   "input",
				"origin":      "/bindings/aws/s3",
				"title":       "Test Title",
			},
			expectedResult: &cliFlags{
				componentType: "state",
				builtinAuth:   []string{"auth1", "auth2"},
				status:        "stable",
				version:       "v1",
				direction:     "input",
				origin:        "/bindings/aws/s3",
				title:         "Test Title",
			},
		},
		{
			name: "Test missing flags",
			flags: map[string]interface{}{
				"type": "pubsub",
			},
			expectedResult: &cliFlags{
				componentType: "pubsub",
				builtinAuth:   []string{},
				status:        "",
				version:       "",
				direction:     "",
				origin:        "",
				title:         "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new cobra command with flags
			cmd := &cobra.Command{}
			for flag, value := range tt.flags {
				switch v := value.(type) {
				case string:
					cmd.Flags().String(flag, v, "")
				case []string:
					cmd.Flags().StringSlice(flag, v, "")
				}
			}

			result := getCmdFlags(cmd)

			// Assert the result
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestGetComponentName(t *testing.T) {
	tests := []struct {
		name           string
		origin         string
		pkg            string
		expectedResult string
	}{
		{
			name:           "Test AWS component",
			origin:         "/bindings/aws/s3",
			pkg:            "s3",
			expectedResult: "aws.s3",
		},
		{
			name:           "Test Azure component",
			origin:         "/bindings/azure/blobstorage",
			pkg:            "blobstorage",
			expectedResult: "azure.blobstorage",
		},
		{
			name:           "Test GCP component",
			origin:         "pubsub/gcp/pubsub",
			pkg:            "pubsub",
			expectedResult: "gcp.pubsub",
		},
		{
			name:           "Test with no CSP in path",
			origin:         "",
			pkg:            "component",
			expectedResult: "component",
		},
		{
			name:           "Test with no CSP in path again",
			origin:         "/apns",
			pkg:            "apns",
			expectedResult: "apns",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getComponentName(tt.origin, tt.pkg)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestAssembleComponentMetadata(t *testing.T) {
	tests := []struct {
		name           string
		flags          *cliFlags
		componentName  string
		metadata       []metadataschema.Metadata
		bindingSpec    metadataschema.Binding
		expectedResult metadataschema.ComponentMetadata
	}{
		{
			name: "Test with all fields set and builtinAuth",
			flags: &cliFlags{
				componentType: "testType",
				builtinAuth:   []string{"aws", "azuread"},
				status:        "stable",
				version:       "v1",
				title:         "Test title",
			},
			componentName: "component1",
			metadata: []metadataschema.Metadata{
				{
					Name:      "TestMetadataField",
					Required:  true,
					Sensitive: false,
				},
			},
			bindingSpec: metadataschema.Binding{
				Input:  true,
				Output: false,
				Operations: []metadataschema.BindingOperation{
					{
						Name:        "completion",
						Description: "Text completion",
					},
					{
						Name:        "chat-completion",
						Description: "Chat completion",
					},
				},
			},
			expectedResult: metadataschema.ComponentMetadata{
				SchemaVersion: "v2",
				Type:          "testType",
				Name:          "component1",
				Version:       "v1",
				Status:        "stable",
				Title:         "Test title",
				Metadata: []metadataschema.Metadata{
					{
						Name:      "TestMetadataField",
						Required:  true,
						Sensitive: false,
					},
				},
				URLs: supportedComponentURL("testType", "component1"),
				Binding: &metadataschema.Binding{
					Input:  true,
					Output: false,
					Operations: []metadataschema.BindingOperation{
						{
							Name:        "completion",
							Description: "Text completion",
						},
						{
							Name:        "chat-completion",
							Description: "Chat completion",
						},
					},
				},
				BuiltInAuthenticationProfiles: []metadataschema.BuiltinAuthenticationProfile{{Name: "aws", Metadata: []metadataschema.Metadata(nil)}, {Name: "azuread", Metadata: []metadataschema.Metadata(nil)}},
			},
		},
		{
			name: "Test with missing builtinAuth",
			flags: &cliFlags{
				componentType: "type2",
				builtinAuth:   nil,
				status:        "alpha",
				version:       "v2",
				title:         "Title2",
			},
			componentName: "component2",
			metadata: []metadataschema.Metadata{
				{
					Name:       "TestMetadataField",
					Required:   true,
					Sensitive:  false,
					Type:       "string",
					Example:    "test example",
					Deprecated: false,
				},
			},
			bindingSpec: metadataschema.Binding{
				Input:  true,
				Output: true,
				Operations: []metadataschema.BindingOperation{
					{
						Name:        "completion",
						Description: "Text completion",
					},
				},
			},
			expectedResult: metadataschema.ComponentMetadata{
				SchemaVersion: "v2",
				Type:          "type2",
				Name:          "component2",
				Version:       "v2",
				Status:        "alpha",
				Title:         "Title2",
				Metadata: []metadataschema.Metadata{
					{
						Name:       "TestMetadataField",
						Required:   true,
						Sensitive:  false,
						Type:       "string",
						Example:    "test example",
						Deprecated: false,
					},
				},
				URLs: supportedComponentURL("type2", "component2"),
				Binding: &metadataschema.Binding{
					Input:  true,
					Output: true,
					Operations: []metadataschema.BindingOperation{
						{
							Name:        "completion",
							Description: "Text completion",
						},
					},
				},
			},
		},
		{
			name: "Test with empty flags and metadata",
			flags: &cliFlags{
				componentType: "",
				builtinAuth:   nil,
				status:        "",
				version:       "",
				title:         "",
			},
			componentName: "component3",
			metadata:      nil,
			bindingSpec:   metadataschema.Binding{},
			expectedResult: metadataschema.ComponentMetadata{
				SchemaVersion: "v2",
				Type:          "",
				Name:          "component3",
				Version:       "",
				Status:        "",
				Title:         "",
				Metadata:      nil,
				URLs:          supportedComponentURL("", "component3"),
				Binding:       &metadataschema.Binding{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := assembleComponentMetadata(tt.flags, tt.componentName, tt.metadata, tt.bindingSpec)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedResult.SchemaVersion, result.SchemaVersion)
			assert.Equal(t, tt.expectedResult.Version, result.Version)
			assert.Equal(t, tt.expectedResult.Name, result.Name)
			assert.Equal(t, tt.expectedResult.Type, result.Type)
			assert.Equal(t, tt.expectedResult.Status, result.Status)
			assert.Equal(t, tt.expectedResult.Title, result.Title)
			assert.Equal(t, tt.expectedResult.Description, result.Description)
			assert.Equal(t, tt.expectedResult.URLs, result.URLs)
			assert.Equal(t, tt.expectedResult.Binding.Input, result.Binding.Input)
			assert.Equal(t, tt.expectedResult.Binding.Output, result.Binding.Output)
			assert.Equal(t, tt.expectedResult.Binding.Operations, result.Binding.Operations)
			assert.Equal(t, tt.expectedResult.Capabilities, result.Capabilities)
			assert.Equal(t, tt.expectedResult.AuthenticationProfiles, result.AuthenticationProfiles)
			assert.Equal(t, tt.expectedResult.BuiltInAuthenticationProfiles, result.BuiltInAuthenticationProfiles)
			assert.Equal(t, tt.expectedResult.Metadata, result.Metadata)
		})
	}
}

func TestAddBuiltinAuthProfiles(t *testing.T) {
	tests := []struct {
		name             string
		authProfiles     []string
		expectedProfiles []metadataschema.BuiltinAuthenticationProfile
	}{
		{
			name:             "Valid auth profiles",
			authProfiles:     []string{"aws", "azuread", "gcp"},
			expectedProfiles: []metadataschema.BuiltinAuthenticationProfile{{Name: "aws"}, {Name: "azuread"}, {Name: "gcp"}},
		},
		{
			name:             "Mixed case auth profiles",
			authProfiles:     []string{"Aws", "AZUREAD", "Gcp"},
			expectedProfiles: []metadataschema.BuiltinAuthenticationProfile{{Name: "aws"}, {Name: "azuread"}, {Name: "gcp"}},
		},
		{
			name:             "Non-existent profiles",
			authProfiles:     []string{"ldap", "saml"},
			expectedProfiles: []metadataschema.BuiltinAuthenticationProfile{},
		},
		{
			name:             "Some valid and some non-existent profiles",
			authProfiles:     []string{"aws", "ldap", "gcp"},
			expectedProfiles: []metadataschema.BuiltinAuthenticationProfile{{Name: "aws"}, {Name: "gcp"}},
		},
		{
			name:             "Empty auth profiles",
			authProfiles:     []string{},
			expectedProfiles: []metadataschema.BuiltinAuthenticationProfile{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			component := &metadataschema.ComponentMetadata{}
			addBuiltinAuthProfiles(tt.authProfiles, component)
			assert.ElementsMatch(t, tt.expectedProfiles, component.BuiltInAuthenticationProfiles)
		})
	}
}

// TODO: add test for writeMetadataToFile

// TODO: add test for generateComponentOperations

func TestFindBindingInFunction(t *testing.T) {
	tests := []struct {
		name            string
		node            *ast.File
		funcName        string
		expectedBinding *metadataschema.Binding
		expectedError   error
	}{
		{
			name:     "Test function found with binding",
			funcName: "Binding",
			node:     createMockASTFileWithFunc("Binding", true),
			expectedBinding: &metadataschema.Binding{
				Input:  true,
				Output: false,
				Operations: []metadataschema.BindingOperation{
					metadataschema.BindingOperation{Name: "create", Description: "Create blob"},
					metadataschema.BindingOperation{Name: "get", Description: "Get blob"},
				},
			},
			expectedError: nil,
		},
		{
			name:            "Test function Binding not found",
			funcName:        "Binding",
			node:            createMockASTFileWithFunc("Binding", false),
			expectedBinding: nil,
			expectedError:   errors.New("function Binding not found"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binding, err := findBindingInFunction(tt.node, tt.funcName)
			assert.Equal(t, tt.expectedBinding, binding)
			if tt.expectedError != nil || err != nil {
				assert.Equal(t, tt.expectedError, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestExtractBindingFromFunctionBody(t *testing.T) {
	tests := []struct {
		name            string
		body            *ast.BlockStmt
		expectedBinding *metadataschema.Binding
		expectedError   error
	}{
		{
			name: "Test with valid binding return statement",
			body: createValidFunctionBody(),
			expectedBinding: &metadataschema.Binding{
				Input:  true,
				Output: false,
				Operations: []metadataschema.BindingOperation{
					metadataschema.BindingOperation{Name: "create", Description: "Create blob"},
					metadataschema.BindingOperation{Name: "get", Description: "Get blob"},
				},
			},
			expectedError: nil,
		},
		{
			name:            "Test with empty body",
			body:            &ast.BlockStmt{},
			expectedBinding: nil,
			expectedError:   errors.New("function body is empty"),
		},
		{
			name:            "Test with no return statement with binding",
			body:            createBodyWithoutBindingReturn(),
			expectedBinding: nil,
			expectedError:   errors.New("no return statement with a Binding found"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binding, err := extractBindingFromFunctionBody(tt.body)
			assert.Equal(t, tt.expectedBinding, binding)
			if tt.expectedError != nil || err != nil {
				assert.Equal(t, tt.expectedError, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestParseBindingStruct(t *testing.T) {
	tests := []struct {
		name            string
		expr            ast.Expr
		expectedBinding *metadataschema.Binding
		expectedError   error
	}{
		{
			name: "Test with valid composite literal",
			expr: createValidCompositeLit(),
			expectedBinding: &metadataschema.Binding{
				Input:  true,
				Output: false,
				Operations: []metadataschema.BindingOperation{
					{
						Name:        "create",
						Description: "Create blob",
					},
					{
						Name:        "get",
						Description: "Get blob",
					},
				},
			},
			expectedError: nil,
		},
		{
			name:            "Test with invalid composite literal",
			expr:            createInvalidCompositeLit(),
			expectedBinding: nil,
			expectedError:   errors.New("return value is not a composite literal of type Binding"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binding, err := parseBindingStruct(tt.expr)
			assert.Equal(t, tt.expectedBinding, binding)
			if tt.expectedError != nil || err != nil {
				assert.Equal(t, tt.expectedError, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestPopulateBindingField(t *testing.T) {
	tests := []struct {
		name            string
		binding         *metadataschema.Binding
		kv              *ast.KeyValueExpr
		expectedBinding *metadataschema.Binding
		expectedError   error
	}{
		{
			name:    "Test populating Input field",
			binding: &metadataschema.Binding{},
			kv:      createKeyValueExpr("Input", "true"),
			expectedBinding: &metadataschema.Binding{
				Input: true,
			},
			expectedError: nil,
		},
		{
			name:    "Populate Output field",
			binding: &metadataschema.Binding{},
			kv:      createKeyValueExpr("Output", "false"),
			expectedBinding: &metadataschema.Binding{
				Output: false,
			},
			expectedError: nil,
		},
		{
			name:            "Invalid key",
			binding:         &metadataschema.Binding{},
			kv:              createInvalidKeyValueExpr("true"),
			expectedBinding: nil,
			expectedError:   errors.New("key is not an identifier"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := populateBindingField(tt.binding, tt.kv)
			if tt.expectedError != nil || err != nil {
				assert.Equal(t, tt.expectedError, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestParseBoolValue(t *testing.T) {
	tests := []struct {
		name           string
		expr           ast.Expr
		expectedResult bool
	}{
		{
			name:           "True expression",
			expr:           &ast.Ident{Name: "true"},
			expectedResult: true,
		},
		{
			name:           "False expression",
			expr:           &ast.Ident{Name: "false"},
			expectedResult: false,
		},
		{
			name:           "Non-boolean expression",
			expr:           &ast.Ident{Name: "notABool"},
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseBoolValue(tt.expr)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestParseOperationsArray(t *testing.T) {
	tests := []struct {
		name          string
		value         ast.Expr
		expectedOps   []metadataschema.BindingOperation
		expectedError error
	}{
		{
			name: "Valid operations array",
			value: &ast.CompositeLit{
				Elts: []ast.Expr{
					createBindingOperationCompositeLit("Create", "Create blob"),
					createBindingOperationCompositeLit("Get", "Get blob"),
				},
			},
			expectedOps: []metadataschema.BindingOperation{
				{Name: "Create", Description: "Create blob"},
				{Name: "Get", Description: "Get blob"},
			},
			expectedError: nil,
		},
		{
			name: "Invalid element type in operations array",
			value: &ast.CompositeLit{
				Elts: []ast.Expr{
					&ast.BasicLit{Kind: token.STRING, Value: `"Invalid"`}, // Invalid element (should be CompositeLit)
				},
			},
			expectedOps:   nil,
			expectedError: errors.New("expected CompositeLit for BindingOperation"),
		},
		{
			name: "Non-CompositeLit Operations field",
			value: &ast.BasicLit{
				Kind:  token.STRING,
				Value: `"Not a CompositeLit"`,
			},
			expectedOps:   nil,
			expectedError: errors.New("operations field is not a composite literal"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operations, err := parseOperationsArray(tt.value)
			if tt.expectedError != nil {
				assert.EqualError(t, err, tt.expectedError.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedOps, operations)
			}
		})
	}
}

func TestValidateOperationFields(t *testing.T) {
	tests := []struct {
		name          string
		operation     metadataschema.BindingOperation
		expectedError error
	}{
		{
			name:          "Valid operation with both fields",
			operation:     metadataschema.BindingOperation{Name: "Create", Description: "Create a notification"},
			expectedError: nil,
		},
		{
			name:          "Missing Name field",
			operation:     metadataschema.BindingOperation{Description: "Create a notification"},
			expectedError: errors.New("missing 'Name' field in BindingOperation"),
		},
		{
			name:          "Missing Description field",
			operation:     metadataschema.BindingOperation{Name: "Create"},
			expectedError: errors.New("missing 'Description' field in BindingOperation"),
		},
		{
			name:          "Missing both Name and Description fields",
			operation:     metadataschema.BindingOperation{},
			expectedError: errors.New("missing 'Name' field in BindingOperation"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOperationFields(tt.operation)
			if tt.expectedError != nil {
				assert.EqualError(t, err, tt.expectedError.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestExtractStringValue(t *testing.T) {
	tests := []struct {
		name     string
		expr     ast.Expr
		expected string
	}{
		{
			name: "Basic string literal",
			expr: &ast.BasicLit{
				Kind:  token.STRING,
				Value: "\"hello\"",
			},
			expected: "hello",
		},
		{
			name: "Empty string literal",
			expr: &ast.BasicLit{
				Kind:  token.STRING,
				Value: "\"\"",
			},
			expected: "",
		},
		{
			name: "Non-string literal",
			expr: &ast.BasicLit{
				Kind:  token.INT,
				Value: "123",
			},
			expected: "123",
		},
		{
			name:     "Nil expression",
			expr:     nil,
			expected: "",
		},
		{
			name: "Unsupported expression type",
			expr: &ast.Ident{
				Name: "identifier",
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractStringValue(tt.expr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseJSONTag(t *testing.T) {
	tests := []struct {
		name     string
		tag      string
		expected string
	}{
		{
			name:     "Simple JSON tag",
			tag:      `json:"name"`,
			expected: "name",
		},
		{
			name:     "JSON tag with omitempty",
			tag:      `json:"name,omitempty"`,
			expected: "name",
		},
		{
			name:     "JSON tag with multiple attributes",
			tag:      `json:"name" xml:"name"`,
			expected: "name",
		},
		{
			name:     "Empty JSON tag",
			tag:      `json:""`,
			expected: "",
		},
		{
			name:     "No JSON tag",
			tag:      `mapstructure:"name"`,
			expected: "",
		},
		{
			name:     "Tag without backticks",
			tag:      "json:\"name\"",
			expected: "name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseJSONTag(tt.tag)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGenerateMetadataFromStructs(t *testing.T) {
	tests := []struct {
		name                     string
		filePath                 string
		structName               string
		underlyingComponentName  string
		expectedMetadataEntries  int
		expectedAuthProfileCount int
		expectError              bool
	}{
		{
			name:                     "Valid Go struct",
			filePath:                 "./testdata/valid_struct.go",
			structName:               "TestStruct",
			underlyingComponentName:  "componentA",
			expectedMetadataEntries:  2,
			expectedAuthProfileCount: 1,
			expectError:              false,
		},
		{
			name:                     "Nonexistent file",
			filePath:                 "./testdata/nonexistent.go",
			structName:               "TestStruct",
			underlyingComponentName:  "componentA",
			expectedMetadataEntries:  0,
			expectedAuthProfileCount: 0,
			expectError:              true,
		},
		{
			name:                     "Invalid Go syntax",
			filePath:                 "./testdata/invalid_syntax.go",
			structName:               "TestStruct",
			underlyingComponentName:  "componentA",
			expectedMetadataEntries:  0,
			expectedAuthProfileCount: 0,
			expectError:              true,
		},
		{
			name:                     "Struct not found",
			filePath:                 "./testdata/valid_struct.go",
			structName:               "NonExistentStruct",
			underlyingComponentName:  "componentA",
			expectedMetadataEntries:  0,
			expectedAuthProfileCount: 0,
			expectError:              true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadataEntries, authProfileMetadataMap, err := generateMetadataFromStructs(tt.filePath, tt.structName, tt.underlyingComponentName)
			if err != nil || tt.expectError {
				assert.Error(t, err, tt.expectError)
			}
			assert.Equal(t, len(metadataEntries), tt.expectedMetadataEntries)
			assert.Equal(t, len(authProfileMetadataMap), tt.expectedAuthProfileCount)
		})
	}
}
func TestExtractMetadataFromAST(t *testing.T) {
	tests := []struct {
		name                     string
		inputAST                 *ast.File
		structName               string
		filePath                 string
		underlyingComponentName  string
		expectedMetadataEntries  int
		expectedAuthProfileCount int
	}{
		{
			name: "Struct with metadata",
			inputAST: func() *ast.File {
				fset := token.NewFileSet()
				src := `package test

				type TestStruct struct {
					FieldA string ` + "`json:\"field_a\"`" + `
					FieldB string ` + "`json:\"field_b\"`" + `
				}
				`
				file, _ := parser.ParseFile(fset, "", src, parser.AllErrors)
				return file
			}(),
			structName:               "TestStruct",
			filePath:                 "test.go",
			underlyingComponentName:  "componentA",
			expectedMetadataEntries:  2,
			expectedAuthProfileCount: 0,
		},
		{
			name: "Empty struct",
			inputAST: func() *ast.File {
				fset := token.NewFileSet()
				src := `package test

				type EmptyStruct struct {}
				`
				file, _ := parser.ParseFile(fset, "", src, parser.AllErrors)
				return file
			}(),
			structName:               "EmptyStruct",
			filePath:                 "test.go",
			underlyingComponentName:  "componentA",
			expectedMetadataEntries:  0,
			expectedAuthProfileCount: 0,
		},
		{
			name: "Struct not found",
			inputAST: func() *ast.File {
				fset := token.NewFileSet()
				src := `package test

				type OtherStruct struct {}
				`
				file, _ := parser.ParseFile(fset, "", src, parser.AllErrors)
				return file
			}(),
			structName:               "NonExistentStruct",
			filePath:                 "test.go",
			underlyingComponentName:  "componentA",
			expectedMetadataEntries:  0,
			expectedAuthProfileCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadataEntries, authProfileMetadataMap := extractMetadataFromAST(tt.inputAST, tt.structName, tt.filePath, tt.underlyingComponentName)
			assert.Equal(t, len(metadataEntries), tt.expectedMetadataEntries)
			assert.Equal(t, len(authProfileMetadataMap), tt.expectedAuthProfileCount)
		})
	}
}

func TestConvertFieldInfoToMetadata(t *testing.T) {
	tests := []struct {
		name     string
		input    *FieldInfo
		expected *metadataschema.Metadata
	}{
		{
			name: "Complete FieldInfo",
			input: &FieldInfo{
				JSONTag:       "name",
				Description:   "A sample description",
				Required:      true,
				Sensitive:     false,
				Type:          "string",
				Default:       "defaultValue",
				Example:       "exampleValue",
				AllowedValues: []string{"value1", "value2"},
				Binding: &metadataschema.MetadataBinding{
					Input:  true,
					Output: true,
				},
				Deprecated: false,
			},
			expected: &metadataschema.Metadata{
				Name:          "name",
				Description:   "A sample description",
				Required:      true,
				Sensitive:     false,
				Type:          "string",
				Default:       "defaultValue",
				Example:       "exampleValue",
				AllowedValues: []string{"value1", "value2"},
				Binding: &metadataschema.MetadataBinding{
					Input:  true,
					Output: true,
				},
				Deprecated: false,
			},
		},
		{
			name: "Complete FieldInfo except for binding info",
			input: &FieldInfo{
				JSONTag:       "name",
				Description:   "A sample description",
				Required:      true,
				Sensitive:     false,
				Type:          "string",
				Default:       "defaultValue",
				Example:       "exampleValue",
				AllowedValues: []string{"value1", "value2"},
				Binding:       &metadataschema.MetadataBinding{},
				Deprecated:    false,
			},
			expected: &metadataschema.Metadata{
				Name:          "name",
				Description:   "A sample description",
				Required:      true,
				Sensitive:     false,
				Type:          "string",
				Default:       "defaultValue",
				Example:       "exampleValue",
				AllowedValues: []string{"value1", "value2"},
				Binding:       &metadataschema.MetadataBinding{},
				Deprecated:    false,
			},
		},
		{
			name:     "Nil FieldInfo",
			input:    nil,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertFieldInfoToMetadata(tt.input)
			if tt.expected == nil {
				assert.Nil(t, result)
				return
			}
			assert.Equal(t, tt.expected.Name, result.Name)
			assert.Equal(t, tt.expected.Type, result.Type)
			assert.Equal(t, tt.expected.Description, result.Description)
			if tt.expected.Binding != nil {
				assert.Equal(t, tt.expected.Binding.Input, result.Binding.Input)
				assert.Equal(t, tt.expected.Binding.Output, result.Binding.Output)
			}
		})
	}
}

func TestParseAuthProfileTag(t *testing.T) {
	tests := []struct {
		name     string
		tag      string
		expected string
	}{
		{
			name:     "Valid authenticationProfile tag",
			tag:      `authenticationProfile:"profileKey"`,
			expected: "profileKey",
		},
		{
			name:     "Tag with multiple attributes",
			tag:      `json:"field" authenticationProfile:"profileKey"`,
			expected: "profileKey",
		},
		{
			name:     "Malformed authenticationProfile tag",
			tag:      `authenticationProfile:profileKey`,
			expected: "profileKey",
		},
		{
			name:     "No authenticationProfile tag",
			tag:      `json:"field"`,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseAuthProfileTag(tt.tag)
			assert.Equal(t, result, tt.expected)
		})
	}
}
