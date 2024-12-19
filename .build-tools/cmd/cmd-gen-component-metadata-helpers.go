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
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
)

/*
sam todos:
-
- allowed values
- if there is no binding direction in encoding tags, then can use the binding direction itself on the field direction if not provided.
*/

type cliFlags struct {
	componentType string
	builtinAuth   []string
	status        string
	version       string
	direction     string
	origin        string
	title         string
}

// Helper function to fetch command flags
func getCmdFlags(cmd *cobra.Command) *cliFlags {
	componentType, _ := cmd.Flags().GetString("type")
	builtinAuth, _ := cmd.Flags().GetStringSlice("builtinAuth")
	status, _ := cmd.Flags().GetString("status")
	version, _ := cmd.Flags().GetString("version")
	direction, _ := cmd.Flags().GetString("direction")
	origin, _ := cmd.Flags().GetString("origin")
	title, _ := cmd.Flags().GetString("title")

	return &cliFlags{
		componentType: componentType,
		builtinAuth:   builtinAuth,
		status:        status,
		version:       version,
		direction:     direction,
		origin:        origin,
		title:         title,
	}
}

// Helper function to get component name based on origin
func getComponentName(origin, pkg string) string {
	const (
		awsPrefix      = "aws"
		azurePrefix    = "azure"
		gcpPrefix      = "gcp"
		alicloudPrefix = "alicloud"
	)

	switch {
	case strings.Contains(origin, awsPrefix):
		return awsPrefix + "." + pkg
	case strings.Contains(origin, azurePrefix):
		return azurePrefix + "." + pkg
	case strings.Contains(origin, gcpPrefix):
		return gcpPrefix + "." + pkg
	case strings.Contains(origin, alicloudPrefix):
		// special edge case
		if pkg == "webhook" {
			return "dingtalk.webhook"
		}
		return alicloudPrefix + "." + pkg
	default:
		return pkg
	}
}

func assembleComponentMetadata(flags *cliFlags, componentName string, metadata []metadataschema.Metadata, bindingSpec metadataschema.Binding) (metadataschema.ComponentMetadata, error) {
	component := metadataschema.ComponentMetadata{
		SchemaVersion: "v2",
		Type:          flags.componentType,
		Name:          componentName,
		Version:       flags.version,
		Status:        flags.status,
		Title:         flags.title,
		Metadata:      metadata,
		URLs:          supportedComponentURL(flags.componentType, componentName),
		Binding:       &bindingSpec,
	}

	if len(flags.builtinAuth) > 0 {
		addBuiltinAuthProfiles(flags.builtinAuth, &component)
	}

	if err := component.AppendBuiltin(); err != nil {
		return metadataschema.ComponentMetadata{}, fmt.Errorf("Error appending built-in metadata: %v", err)
	}
	return component, nil
}

func addBuiltinAuthProfiles(authProfiles []string, component *metadataschema.ComponentMetadata) {
	allBuiltinProfiles := []metadataschema.BuiltinAuthenticationProfile{
		{Name: "aws"},
		{Name: "azuread"},
		{Name: "gcp"},
	}

	for _, compProfiles := range authProfiles {
		for _, options := range allBuiltinProfiles {
			if strings.EqualFold(compProfiles, options.Name) {
				component.BuiltInAuthenticationProfiles = append(component.BuiltInAuthenticationProfiles, options)
			}
		}
	}
}

// TODO: add test
// generateComponentOperations reads the Go file and extracts the s3Operations variable as a Binding type.
func generateComponentOperations(filePath, funcName string) (*metadataschema.Binding, error) {
	node, err := parseGoFile(filePath)
	if err != nil {
		return nil, err
	}

	// Search for the specified function and extract its binding
	return findBindingInFunction(node, funcName)
}

// findBindingInFunction searches for a specific function and extracts a Binding struct from its return statement
func findBindingInFunction(node *ast.File, funcName string) (*metadataschema.Binding, error) {
	for _, decl := range node.Decls {
		funcDecl, ok := decl.(*ast.FuncDecl)
		if !ok || funcDecl.Name.Name != funcName {
			continue
		}

		return extractBindingFromFunctionBody(funcDecl.Body)
	}

	return nil, fmt.Errorf("function %s not found", funcName)
}

// extractBindingFromFunctionBody processes the body of a function to find and construct a Binding struct
func extractBindingFromFunctionBody(body *ast.BlockStmt) (*metadataschema.Binding, error) {
	if body == nil || len(body.List) == 0 {
		return nil, errors.New("function body is empty")
	}

	for _, stmt := range body.List {
		retStmt, ok := stmt.(*ast.ReturnStmt)
		if !ok || len(retStmt.Results) != 1 {
			continue
		}

		binding, err := parseBindingStruct(retStmt.Results[0])
		if err != nil {
			return nil, err
		}

		return binding, nil
	}

	return nil, errors.New("no return statement with a Binding found")
}

// parseBindingStruct parses a CompositeLit expression into a Binding struct
func parseBindingStruct(expr ast.Expr) (*metadataschema.Binding, error) {
	structLit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return nil, errors.New("return value is not a composite literal")
	}

	// Check if the type of the composite literal is "Binding" type
	// Note: the underlying structLit.Type is &{metadataschema Binding} which is pkg and then actual type
	// So, we must do this below to account for.
	/*
		Type: &ast.SelectorExpr{
			X:   &ast.Ident{Name: "metadataschema"}, // Package name
			Sel: &ast.Ident{Name: "Binding"},        // Type name
		},
	*/
	if ident, ok := structLit.Type.(*ast.SelectorExpr); !ok || ident.Sel.Name != "Binding" {
		return nil, errors.New("return value is not a composite literal of type Binding")
	}
	binding := &metadataschema.Binding{}
	for _, elt := range structLit.Elts {
		if kv, ok := elt.(*ast.KeyValueExpr); ok {
			if err := populateBindingField(binding, kv); err != nil {
				return nil, err
			}
		}
	}
	return binding, nil
}

func populateBindingField(binding *metadataschema.Binding, kv *ast.KeyValueExpr) error {
	key, ok := kv.Key.(*ast.Ident)
	if !ok {
		return errors.New("key is not an identifier")
	}

	switch key.Name {
	case "Input":
		binding.Input = parseBoolValue(kv.Value)
	case "Output":
		binding.Output = parseBoolValue(kv.Value)
	case "Operations":
		// Parse the operations array
		operations, err := parseOperationsArray(kv.Value)
		if err != nil {
			return err
		}
		binding.Operations = operations
	}

	return nil
}

// parseBoolValue parses a boolean value from an AST node
func parseBoolValue(value ast.Expr) bool {
	ident, ok := value.(*ast.Ident)
	if !ok {
		return false
	}
	return ident.Name == "true"
}

// parseOperationsArray parses the operations array and returns a slice of BindingOperation
func parseOperationsArray(value ast.Expr) ([]metadataschema.BindingOperation, error) {
	// Ensure the value is a CompositeLit
	compLit, ok := value.(*ast.CompositeLit)
	if !ok {
		return nil, errors.New("operations field is not a composite literal")
	}
	var operations []metadataschema.BindingOperation
	for _, elt := range compLit.Elts {
		// Each element in the operations array is a CompositeLit
		operationLit, ok := elt.(*ast.CompositeLit)
		if !ok {
			return nil, errors.New("expected CompositeLit for BindingOperation")
		}

		operation := metadataschema.BindingOperation{}
		for _, opElt := range operationLit.Elts {
			// Each element inside the operation is a KeyValueExpr (Name, Description)
			if kv, ok := opElt.(*ast.KeyValueExpr); ok {
				// Extract Name and Description from the KeyValueExpr
				// Check the keys and extract the correct values
				if kv.Key.(*ast.Ident).Name == "Name" {
					operation.Name = extractStringValue(kv.Value)
				}
				if kv.Key.(*ast.Ident).Name == "Description" {
					operation.Description = extractStringValue(kv.Value)
				}
			}
		}

		// validate Name and Description fields as both must be present
		if err := validateOperationFields(operation); err != nil {
			return nil, err
		}

		operations = append(operations, operation)
	}

	return operations, nil
}

// validateOperationFields checks if both Name and Description are populated.
func validateOperationFields(op metadataschema.BindingOperation) error {
	if op.Name == "" {
		return errors.New("missing 'Name' field in BindingOperation")
	}
	if op.Description == "" {
		return errors.New("missing 'Description' field in BindingOperation")
	}
	return nil
}

// extractStringValue extracts a string literal value
func extractStringValue(expr ast.Expr) string {
	if strLit, ok := expr.(*ast.BasicLit); ok {
		return strings.Trim(strLit.Value, `"`)
	}
	return ""
}

func writeToFile(outputFilePath string, component metadataschema.ComponentMetadata) error {
	// Open the file for writing (create if not exists)
	file, err := os.Create(outputFilePath)
	if err != nil {
		return fmt.Errorf("could not create file: %v", err)
	}
	defer file.Close()

	// update the field names to lowercase
	for i := range component.Metadata {
		component.Metadata[i].Name = lowercaseFirstLetter(component.Metadata[i].Name)
	}

	yamlEncoder := yaml.NewEncoder(file)
	yamlEncoder.SetIndent(2)
	if err := yamlEncoder.Encode(&component); err != nil {
		return fmt.Errorf("error encoding YAML: %w", err)
	}

	return nil
}

func checkFileExists(filePath string) {
	_, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("File does not exist: %s\n", filePath)
		} else {
			fmt.Printf("Error checking file: %v\n", err)
		}
		return
	}
}

// parseJSONTag extracts field information from component metadata field json tags
func parseJSONTag(tag string) string {
	// Remove the backticks and split the tag
	tag = strings.Trim(tag, "`")
	parts := strings.Split(tag, " ")
	for _, part := range parts {
		if strings.HasPrefix(part, "json:") {
			// Extract the json field name and remove any options like "omitempty"
			jsonTag := part[len("json:"):]
			jsonTag = strings.Trim(jsonTag, `"`)
			if commaIdx := strings.Index(jsonTag, ","); commaIdx != -1 {
				jsonTag = jsonTag[:commaIdx]
			}
			return jsonTag
		}
	}
	return ""
}

type FieldInfo struct {
	Name                     string
	JSONTag                  string
	Description              string
	Required                 bool
	Sensitive                bool
	Type                     string
	Default                  string
	Example                  string
	AllowedValues            []string
	Binding                  *metadataschema.MetadataBinding
	Deprecated               bool
	AuthenticationProfileKey string
}

// generateMetadataFromStructs processes a Go struct to extract metadata and authentication profile data.
func generateMetadataFromStructs(filePath, structName, underlyingComponentName string) ([]metadataschema.Metadata, []metadataschema.AuthenticationProfile, error) {
	src, err := os.ReadFile(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Parse the Go source file
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, filePath, src, parser.AllErrors|parser.ParseComments)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse Go file: %w", err)
	}

	// Extract metadata and authentication profiles
	metadataEntries, authProfileMetadataMap := extractMetadataFromAST(node, structName, filePath, underlyingComponentName)
	if len(metadataEntries) == 0 {
		return nil, nil, errors.New("component has no metadata fields")
	}

	return metadataEntries, convertAuthProfileMetadata(authProfileMetadataMap), nil
}

// TODO: in future need to leverage description field in AuthenticationProfile.
// However, for now is fine to ignore since very few components actually use this.
func convertAuthProfileMetadata(profileMap map[string][]metadataschema.Metadata) []metadataschema.AuthenticationProfile {
	// TODO could add dictionary here for now.
	// Master Key description: Authenticate using a pre-shared "master key".
	var authProfiles []metadataschema.AuthenticationProfile
	for title, profileMetadata := range profileMap {
		newProfile := metadataschema.AuthenticationProfile{
			Title:    title,
			Metadata: profileMetadata,
		}
		authProfiles = append(authProfiles, newProfile)
	}
	return authProfiles
}

// extractMetadataFromAST traverses the AST to locate the struct and extract metadata.
func extractMetadataFromAST(node *ast.File, structName, filePath, underlyingComponentName string) ([]metadataschema.Metadata, map[string][]metadataschema.Metadata) {
	var metadataEntries []metadataschema.Metadata
	authProfileMetadataMap := make(map[string][]metadataschema.Metadata)

	for _, decl := range node.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.TYPE {
			continue
		}

		for _, spec := range genDecl.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != structName {
				continue
			}

			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				continue
			}

			// Process each field in the struct
			for _, field := range structType.Fields.List {
				fieldInfo := extractFieldInfo(filePath, field, underlyingComponentName)
				if fieldInfo == nil {
					continue
				}

				metadata := convertFieldInfoToMetadata(fieldInfo)
				if fieldInfo.AuthenticationProfileKey != "" {
					authProfileMetadataMap[fieldInfo.AuthenticationProfileKey] = append(authProfileMetadataMap[fieldInfo.AuthenticationProfileKey], *metadata)
				} else {
					metadataEntries = append(metadataEntries, *metadata)
				}
			}
		}
	}

	return metadataEntries, authProfileMetadataMap
}

// convertFieldInfoToMetadata converts a FieldInfo object into a Metadata object.
func convertFieldInfoToMetadata(fieldInfo *FieldInfo) *metadataschema.Metadata {
	if fieldInfo == nil {
		return nil
	}
	var binding metadataschema.MetadataBinding
	if fieldInfo.Binding != nil {
		binding = *fieldInfo.Binding
	}
	return &metadataschema.Metadata{
		Name:          fieldInfo.JSONTag,
		Description:   fieldInfo.Description,
		Required:      fieldInfo.Required,
		Sensitive:     fieldInfo.Sensitive,
		Type:          fieldInfo.Type,
		Default:       fieldInfo.Default,
		Example:       fieldInfo.Example,
		AllowedValues: fieldInfo.AllowedValues,
		Binding:       &binding,
		Deprecated:    fieldInfo.Deprecated,
	}
}

// parseAuthProfileTag extracts the authentication profile key from a struct tag.
// Note: this does handle malformed encoding tags.
func parseAuthProfileTag(tag string) string {
	tagParts := strings.Split(tag, " ")
	for _, part := range tagParts {
		if strings.Contains(part, "authenticationProfile:") {
			return strings.Trim(part[len("authenticationProfile:"):], "\"`")
		}
	}
	return ""
}

// extractFieldInfo extracts metadata for a struct field.
func extractFieldInfo(filePath string, field *ast.Field, componentName string) *FieldInfo {
	if len(field.Names) == 0 {
		return nil
	}

	fieldName := field.Names[0].Name
	fieldType := fmt.Sprintf("%s", field.Type)

	// Extract comments
	description := extractFieldDescription(field)

	// Extract JSON tag and metadata
	jsonTag, required, authProfileKey, bindingsMeta := extractTags(field.Tag)
	fmt.Printf("sam authprofile key %v\n", authProfileKey)

	// Ignore fields based on specific tags
	if shouldIgnoreField(field.Tag) {
		return nil
	}

	// Populate and return FieldInfo
	return &FieldInfo{
		Name:                     fieldName,
		JSONTag:                  jsonTag,
		Description:              description,
		Required:                 required,
		Sensitive:                isFieldSensitive(fieldName),
		Type:                     fieldType,
		Default:                  getFunctionValueForField(filePath, componentName, fieldName, "Defaults"),
		Example:                  getFunctionValueForField(filePath, componentName, fieldName, "Examples"),
		AllowedValues:            []string{}, // TODO: Populate as needed
		Binding:                  &bindingsMeta,
		Deprecated:               false,
		AuthenticationProfileKey: strings.TrimSpace(strings.ToLower(authProfileKey)),
	}
}

// extractFieldDescription extracts the description for a field from its comments or documentation.
func extractFieldDescription(field *ast.Field) string {
	if field.Comment != nil {
		return strings.TrimSpace(field.Comment.Text())
	}
	if field.Doc != nil {
		return strings.TrimSpace(field.Doc.Text())
	}
	return ""
}

// extractTags parses the field's tags to extract JSON tag, required status, authentication profile key, and bindings metadata.
func extractTags(tag *ast.BasicLit) (jsonTag string, required bool, authProfileKey string, bindingsMeta metadataschema.MetadataBinding) {
	if tag == nil {
		return "", true, "", metadataschema.MetadataBinding{}
	}

	tagValue := tag.Value
	jsonTag = parseJSONTag(tagValue)

	// Determine if the field is required
	required = !strings.Contains(tagValue, "omitempty")

	// Extract authentication profile key and bindings metadata
	authProfileKey = parseAuthProfileTag(tagValue)
	bindingsMeta = extractBindingMetadata(tagValue)

	return jsonTag, required, authProfileKey, bindingsMeta
}

// shouldIgnoreField determines whether a field should be ignored based on specific tags.
func shouldIgnoreField(tag *ast.BasicLit) bool {
	if tag == nil {
		return false
	}

	tagValue := tag.Value
	return strings.Contains(tagValue, "mdignore") || strings.Contains(tagValue, `mapstructure:"-"`)
}

// extractBindingMetadata parses the binding metadata from a field tag.
func extractBindingMetadata(tagValue string) metadataschema.MetadataBinding {
	bindingsMeta := metadataschema.MetadataBinding{}

	if strings.Contains(tagValue, `binding:"input"`) {
		bindingsMeta.Input = true
	}
	if strings.Contains(tagValue, `binding:"output"`) {
		bindingsMeta.Output = true
	}

	return bindingsMeta
}

// isFieldSensitive determines if a field name indicates that the field contains sensitive information.
func isFieldSensitive(field string) bool {
	fieldLower := strings.ToLower(field)
	return strings.Contains(fieldLower, "key") || strings.Contains(fieldLower, "token")
}

// isPointerReceiver checks if a function's receiver is a pointer type.
func isPointerReceiver(fn *ast.FuncDecl) bool {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return false
	}
	// Check if the receiver is a pointer type
	_, isPointer := fn.Recv.List[0].Type.(*ast.StarExpr)
	return isPointer
}

// getFunctionValueForField retrieves the value of a specific field returned by a given function in a Go file.
func getFunctionValueForField(filePath, componentName, fieldName, functionName string) string {
	// Parse the Go file
	node, err := parseGoFile(filePath)
	if err != nil {
		fmt.Printf("Failed to parse file: %v\n", err)
		return ""
	}

	// Find the target function with a pointer receiver
	functionNode := findFunctionByName(node, functionName)
	if functionNode == nil {
		return functionName + " function not found"
	}

	// Extract the return statement
	returnStmt := findReturnStatement(functionNode)
	if returnStmt == nil {
		return "No return statement found in " + functionName
	}

	// Retrieve the field value from the return statement
	return extractFieldValueFromReturn(returnStmt, fieldName)
}

// parseGoFile parses a Go source file and returns the root node.
func parseGoFile(filePath string) (*ast.File, error) {
	fs := token.NewFileSet()
	return parser.ParseFile(fs, filePath, nil, parser.AllErrors)
}

// findFunctionByName locates a function declaration by its name and checks for a pointer receiver.
func findFunctionByName(node *ast.File, functionName string) *ast.FuncDecl {
	var targetFunc *ast.FuncDecl
	ast.Inspect(node, func(n ast.Node) bool {
		if fn, ok := n.(*ast.FuncDecl); ok && fn.Name.Name == functionName && isPointerReceiver(fn) {
			targetFunc = fn
			return false // Stop traversal once found
		}
		return true
	})
	return targetFunc
}

// findReturnStatement locates the first return statement in a function body.
func findReturnStatement(fn *ast.FuncDecl) *ast.ReturnStmt {
	var returnStmt *ast.ReturnStmt
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if stmt, ok := n.(*ast.ReturnStmt); ok {
			returnStmt = stmt
			return false // Stop traversal once found
		}
		return true
	})
	return returnStmt
}

// extractFieldValueFromReturn extracts the value of a specific field from a return statement.
func extractFieldValueFromReturn(returnStmt *ast.ReturnStmt, fieldName string) string {
	for _, result := range returnStmt.Results {
		if structLit, ok := result.(*ast.CompositeLit); ok {
			for _, elt := range structLit.Elts {
				if kv, ok := elt.(*ast.KeyValueExpr); ok {
					if key, ok := kv.Key.(*ast.Ident); ok && strings.EqualFold(key.Name, fieldName) {
						return parseFieldValue(kv.Value)
					}
				}
			}
		}
	}
	return ""
}

// parseFieldValue parses a field value and returns it as a string.
func parseFieldValue(value ast.Expr) string {
	switch v := value.(type) {
	case *ast.BasicLit:
		return parseBasicLiteral(v)
	case *ast.Ident:
		return v.Name // For booleans or other identifiers
	default:
		return ""
	}
}

// parseBasicLiteral parses a basic literal and returns its value as a string.
func parseBasicLiteral(value *ast.BasicLit) string {
	switch value.Kind {
	case token.STRING:
		return strings.Trim(value.Value, `'"`)
	case token.INT, token.FLOAT:
		return value.Value
	default:
		return value.Value
	}
}

// lowercaseFirstLetter lowercases the first letter of a string
func lowercaseFirstLetter(s string) string {
	if len(s) == 0 {
		return s
	}
	return strings.ToLower(string(s[0])) + s[1:]
}

func supportedComponentURL(componentType, componentName string) []metadataschema.URL {
	referenceURLs := map[string]string{
		"bindings":     "bindings",
		"state":        "state-stores",
		"secretstores": "secret-stores",
		"TODO":         "conversation",
		"pubsub":       "pubsub",
	}

	// other edge cases?
	// if componentName == "alicloud.tablestore" {
	// 	componentName = strings.ReplaceAll(componentName, ".", "")
	// }
	// if componentName == "dingtalk-webhook" {
	// 	componentName = strings.ReplaceAll(componentName, "-", "")
	// }

	// TODO(@Sam): docs must be updated to this format that is standardized for all components, as docs are not atm.

	componentName = strings.ReplaceAll(componentName, ".", "/")

	var urls []metadataschema.URL
	urls = append(urls, metadataschema.URL{
		Title: "Reference",
		URL: fmt.Sprintf("https://docs.dapr.io/reference/components-reference/supported-%s/%s/",
			referenceURLs[componentType], componentName),
	})

	return urls
}
