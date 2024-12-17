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
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/dapr/components-contrib/bindings/aws/dynamodb"
	"github.com/dapr/components-contrib/bindings/aws/kinesis"
	"github.com/dapr/components-contrib/bindings/aws/s3"
	"github.com/dapr/components-contrib/bindings/aws/sns"
	"github.com/dapr/components-contrib/bindings/azure/cosmosdb"
	"github.com/dapr/components-contrib/bindings/azure/cosmosdb/gremlinapi"
	"github.com/dapr/components-contrib/bindings/azure/eventgrid"
	"github.com/dapr/components-contrib/bindings/azure/openai"
	"github.com/dapr/components-contrib/bindings/azure/storagequeues"
	"github.com/dapr/components-contrib/build-tools/pkg/componentmetadata"
	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// generateComponentMetadataCmd represents the command to generate the yaml manifest for each component
var generateComponentMetadataNewCmd = &cobra.Command{
	Use:   "generate-component-metadata-new",
	Short: "Generates the component metadata yaml file per component",
	Long:  `Generates the component metadata yaml file per component so we don't have to rely on community to manually create it.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("sam running new cmd")
		// Navigate to the root of the repo
		err := cwdToRepoRoot()
		if err != nil {
			panic(err)
		}

		// Get flag values
		componentType, _ := cmd.Flags().GetString("type")
		builtinAuth, _ := cmd.Flags().GetStringSlice("builtinAuth")
		status, _ := cmd.Flags().GetString("status")
		version, _ := cmd.Flags().GetString("version")
		direction, _ := cmd.Flags().GetString("direction")
		origin, _ := cmd.Flags().GetString("origin")
		title, _ := cmd.Flags().GetString("title")

		fmt.Printf("sam origin %v", origin)

		// Find all components
		list, err := componentmetadata.FindValidComponents(ComponentFolders, ExcludeFolders)
		// fmt.Println("sam list %v", list)
		if err != nil {
			panic(err)
		}
		// Load the metadata for all components
		bundle := metadataschema.Bundle{
			SchemaVersion: "v1",
			Date:          time.Now().Format("20060102150405"),
			Components:    make([]*metadataschema.ComponentMetadata, 0, len(list)),
		}

		_ = bundle
		fmt.Printf("sam componentType %s builtinAuth %s status %s version %s direction %s", componentType, builtinAuth, status, version, direction)
		filepath := origin //os.Getenv("DOLLAR")
		metadataFile := filepath + "/metadata.go"
		componentPkg := os.Getenv("GOPACKAGE")
		metadata, _, err := generateMetadataFromStructs(metadataFile, componentPkg+"Metadata", componentPkg)
		if err != nil {
			fmt.Printf("sam error: %v", err)
		}

		var componentName string
		const (
			awsPrefix   = "aws"
			azurePrefix = "azure"
		)
		switch {
		case strings.Contains(origin, awsPrefix):
			componentName = awsPrefix + "." + componentPkg
		case strings.Contains(origin, azurePrefix):
			componentName = azurePrefix + "." + componentPkg
		default:
			componentName = componentPkg
		}
		bindingSpecification, err := generateComponentOperations(metadataFile, componentPkg+"Binding")
		fmt.Printf("\nsam the binding spec %v\n", bindingSpecification)
		if err != nil {
			panic(err)
		}
		component := metadataschema.ComponentMetadata{
			SchemaVersion: "v2",
			Type:          componentType,
			Name:          componentName,
			Version:       version,
			Status:        status,
			Title:         title,
			Metadata:      metadata,
			URLs:          supportedComponentURL(componentType, componentPkg),
			Binding:       bindingSpecification,
		}
		var awsAuthProfile = metadataschema.BuiltinAuthenticationProfile{Name: "aws"}
		var azureAuthProfile = metadataschema.BuiltinAuthenticationProfile{Name: "azuread"}
		var gcpAuthProfile = metadataschema.BuiltinAuthenticationProfile{Name: "gcp"}
		var allBuiltinProfiles = []metadataschema.BuiltinAuthenticationProfile{awsAuthProfile, azureAuthProfile, gcpAuthProfile}
		for _, compProfiles := range builtinAuth {
			for _, options := range allBuiltinProfiles {
				if strings.ToLower(compProfiles) == strings.ToLower(options.Name) {
					component.BuiltInAuthenticationProfiles = append(component.BuiltInAuthenticationProfiles, options)
				}
			}
		}

		// Append built-in metadata properties
		err = component.AppendBuiltin()
		if err != nil {
			fmt.Printf("sam error from AppendBuildin %v", err)
		}

		for _, profile := range component.BuiltInAuthenticationProfiles {
			appendProfiles, err := metadataschema.ParseBuiltinAuthenticationProfile(profile, component.Title)
			if err != nil {
				fmt.Printf("sam error from ParseBuiltinAuthenticationProfile %v", err)
			}

			component.AuthenticationProfiles = append(component.AuthenticationProfiles, appendProfiles...)
		}

		outputFilePath := filepath + "/samhereoutput.yaml"
		err = writeToFile(outputFilePath, component)
		if err != nil {
			fmt.Printf("Error writing to file: %v", err)
		} else {
			fmt.Printf("Metadata written to %s\n", outputFilePath)
		}

		// print everything out if i want
		enc := json.NewEncoder(os.Stdout)
		enc.SetEscapeHTML(false)
		enc.SetIndent("", "  ")
		err = enc.Encode(component)
		if err != nil {
			panic(fmt.Errorf("failed to encode bundle to JSON: %w", err))
		}
	},
}

// generateComponentOperations reads the Go file and extracts the s3Operations variable as a Binding type.
func generateComponentOperations(filePath, varName string) (*metadataschema.Binding, error) {
	// Read the Go file
	src, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Parse the Go source file
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, filePath, src, parser.AllErrors|parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Go file: %w", err)
	}

	// Loop through the declarations in the file
	for _, decl := range node.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.VAR {
			continue
		}

		// Check the variable declarations for the name
		for _, spec := range genDecl.Specs {
			// Extract the variable declaration
			vspec, ok := spec.(*ast.ValueSpec)
			fmt.Printf("sam here 1 %v %v", vspec, ok)
			if !ok || len(vspec.Names) == 0 || vspec.Names[0].Name != varName {
				continue
			}

			// The variable was found, now process it
			fmt.Printf("sam length %v\n", len(vspec.Values))
			if len(vspec.Values) == 1 {
				// The variable definition is in the first value position
				// Convert the value to a Go expression (it's a literal, so we can use it directly)
				structLiteral, ok := vspec.Values[0].(*ast.CompositeLit)
				fmt.Printf("sam %v\n\n", ok)
				if !ok {
					continue
				}

				// Initialize an empty Binding struct
				binding := &metadataschema.Binding{}

				// Process the struct fields
				for _, elt := range structLiteral.Elts {
					fmt.Printf("sam now processing fields %v\n", elt)
					switch field := elt.(type) {

					case *ast.KeyValueExpr:
						// Extract the key (field name)
						fmt.Printf("sam field.Key %t\n", field.Key)
						key, ok := field.Key.(*ast.Ident)
						if !ok {
							continue
						}
						fmt.Printf("sam here is key\n", key)

						// Handle each field based on the key
						switch key.Name {
						case "Input":
							fmt.Printf("\nsam in Input %T\n", field.Value)
							// Parse the Input field
							if ident, ok := field.Value.(*ast.Ident); ok {
								// In case it's an identifier, check its name and manually assign the value
								// For example, "true" or "false" might be assigned to Input
								switch ident.Name {
								case "true":
									binding.Input = true
								case "false":
									binding.Input = false
								}
								fmt.Printf("sam just set Input value: %v\n", binding.Input)
							}
						case "Output":
							// Parse the Output field
							fmt.Printf("sam Output value: %+v\n", field.Value)
							// Check if the value is an Ident (identifier)
							if ident, ok := field.Value.(*ast.Ident); ok {
								// In case it's an identifier, check its name and manually assign the value
								// For example, "true" or "false" might be assigned to Output
								switch ident.Name {
								case "true":
									binding.Output = true
								case "false":
									binding.Output = false
								}
								fmt.Printf("sam just set Output value: %v\n", binding.Output)
							}
						case "Operations":
							fmt.Printf("sam the oper type %T\n", field.Value)
							// Parse the Operations field (which is an array of BindingOperation)
							if arrayLit, ok := field.Value.(*ast.CompositeLit); ok {
								fmt.Printf("sam in the if for operations %v %v\n", ok, arrayLit)
								// Process the array of operations
								var operations []metadataschema.BindingOperation
								for _, elem := range arrayLit.Elts {
									// Process each operation (BindingOperation)
									if opLit, ok := elem.(*ast.CompositeLit); ok {
										operation := metadataschema.BindingOperation{}
										// Process each field in the operation struct
										for _, opElt := range opLit.Elts {
											if opField, ok := opElt.(*ast.KeyValueExpr); ok {
												opKey, ok := opField.Key.(*ast.Ident)
												if !ok {
													continue
												}

												// Map each key in the operation to its corresponding value
												switch opKey.Name {
												case "Name":
													if strVal, ok := opField.Value.(*ast.BasicLit); ok {
														operation.Name = strings.Trim(strVal.Value, `"`)
													}
												case "Description":
													if strVal, ok := opField.Value.(*ast.BasicLit); ok {
														operation.Description = strings.Trim(strVal.Value, `"`)
													}
												}
											}
										}
										fmt.Printf("sam just appended op %v\n", operation)

										operations = append(operations, operation)
									}
								}
								binding.Operations = operations
							}
						}
					}
				}

				// Return the binding instance
				fmt.Printf("\nini func still sam %v %v", binding.Input, binding.Output)
				return binding, nil
			}
		}
	}

	return nil, fmt.Errorf("variable %s not found", varName)
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

func init() {

	// Add flags to the command
	generateComponentMetadataNewCmd.Flags().String("type", "", "The component type (e.g., 'binding')")
	generateComponentMetadataNewCmd.Flags().StringSlice("builtinAuth", []string{}, "The authentication profile (e.g., 'aws')")
	generateComponentMetadataNewCmd.Flags().String("status", "", "The status of the component (e.g., 'stable')")
	generateComponentMetadataNewCmd.Flags().String("version", "", "The version of the component (e.g., 'v1')")
	generateComponentMetadataNewCmd.Flags().String("direction", "", "The direction of the component (e.g., 'output')")
	generateComponentMetadataNewCmd.Flags().String("origin", "", "The direction of the component (e.g., 'output')")
	generateComponentMetadataNewCmd.Flags().String("title", "", "The direction of the component (e.g., 'output')")

	rootCmd.AddCommand(generateComponentMetadataNewCmd)

}

func convertPrefixToPath(componentName string) string {
	return strings.ReplaceAll(componentName, ".", "/")
}
func parseJSONTag(tag string) string {
	// Remove the backticks and split the tag
	tag = strings.Trim(tag, "`")
	parts := strings.Split(tag, " ")
	for _, part := range parts {
		if strings.HasPrefix(part, "json:") {
			// Extract the json field name and remove any options like "omitempty"
			jsonTag := part[len("json:"):]
			jsonTag = strings.Trim(jsonTag, `"`) // Remove surrounding quotes
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

// generateMetadataFromStructs processes a Go struct to extract metadata and authentication profile data
func generateMetadataFromStructs(filePath, structName, underlyingComponentName string) ([]metadataschema.Metadata, map[string][]metadataschema.Metadata, error) {
	src, err := os.ReadFile(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read file: %w", err)
	}

	fmt.Printf("sam the structName %v\n", structName)

	// Parse the Go source file
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, filePath, src, parser.AllErrors|parser.ParseComments)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse Go file: %w", err)
	}

	var metadataEntries []metadataschema.Metadata
	authProfileMetadataMap := make(map[string][]metadataschema.Metadata)

	// Traverse the AST to locate the struct
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
				fieldInfo := extractFieldInfo(field, underlyingComponentName)
				if fieldInfo == nil {
					continue
				}

				metadata := metadataschema.Metadata{
					Name:          fieldInfo.JSONTag,
					Description:   fieldInfo.Description,
					Required:      fieldInfo.Required,
					Sensitive:     fieldInfo.Sensitive,
					Type:          fieldInfo.Type,
					Default:       fieldInfo.Default,
					Example:       fieldInfo.Example,
					AllowedValues: fieldInfo.AllowedValues,
					Binding:       fieldInfo.Binding,
					Deprecated:    fieldInfo.Deprecated,
				}

				if fieldInfo.AuthenticationProfileKey != "" {
					authProfileMetadataMap[fieldInfo.AuthenticationProfileKey] = append(authProfileMetadataMap[fieldInfo.AuthenticationProfileKey], metadata)
				} else {
					metadataEntries = append(metadataEntries, metadata)
				}
			}
		}
	}

	return metadataEntries, authProfileMetadataMap, nil
}

func parseAuthProfileTag(tag string) string {
	tagParts := strings.Split(tag, " ")
	for _, part := range tagParts {
		if strings.HasPrefix(part, "authenticationProfile:") {
			return strings.Trim(part[len("authenticationProfile:"):], "\"`")
		}
	}
	return ""
}

// extractFieldInfo extracts metadata for a struct field
func extractFieldInfo(field *ast.Field, componentName string) *FieldInfo {
	if len(field.Names) == 0 {
		return nil
	}

	fieldName := field.Names[0].Name
	fieldType := fmt.Sprintf("%s", field.Type)

	// Extract comments
	description := ""
	if field.Comment != nil {
		description = strings.TrimSpace(field.Comment.Text())
	} else if field.Doc != nil {
		description = strings.TrimSpace(field.Doc.Text())
	}

	// Extract JSON tag and metadata
	var jsonTag string
	required := true
	authProfileKey := ""
	bindingsMeta := metadataschema.MetadataBinding{}

	if field.Tag != nil {
		tag := field.Tag.Value
		jsonTag = parseJSONTag(tag)

		if strings.Contains(tag, "omitempty") {
			required = false
		}
		if strings.Contains(tag, "mdignore") || strings.Contains(tag, "-") {
			return nil
		}
		if strings.Contains(tag, "authenticationProfile:") {
			authProfileKey = parseAuthProfileTag(tag)
		}
		if strings.Contains(tag, `binding:"input"`) {
			bindingsMeta.Input = true
		}
		if strings.Contains(tag, `binding:"output"`) {
			bindingsMeta.Output = true
		}
	}

	// Populate field info
	return &FieldInfo{
		Name:                     fieldName,
		JSONTag:                  jsonTag,
		Description:              description,
		Required:                 required,
		Sensitive:                isFieldSensitive(fieldName),
		Type:                     fieldType,
		Default:                  getDefaultValueForField(componentName, fieldName),
		Example:                  getExampleValueForField(componentName, fieldName),
		AllowedValues:            []string{}, // TODO: Populate as needed
		Binding:                  &bindingsMeta,
		Deprecated:               false,
		AuthenticationProfileKey: strings.TrimSpace(strings.ToLower(authProfileKey)),
	}
}

func isFieldSensitive(field string) bool {
	return strings.Contains(strings.ToLower(field), "key") || strings.Contains(strings.ToLower(field), "token")
}

func getDefaultValueForField(underlyingComponentName, fieldName string) string {
	switch underlyingComponentName {
	case "s3":
		meta := s3.Defaults()
		defaultsMap := getStructDefaultValue(meta)
		return defaultsMap[fieldName]
	case "sns":
		meta := sns.Defaults()
		defaultsMap := getStructDefaultValue(meta)
		return defaultsMap[fieldName]
	case "dynamodb":
		meta := dynamodb.Defaults()
		defaultsMap := getStructDefaultValue(meta)
		return defaultsMap[fieldName]
	case "cosmosdb":
		meta := cosmosdb.Defaults()
		defaultsMap := getStructDefaultValue(meta)
		return defaultsMap[fieldName]
	case "gremlinapi":
		meta := gremlinapi.Defaults()
		defaultsMap := getStructDefaultValue(meta)
		return defaultsMap[fieldName]
	case "eventgrid":
		meta := eventgrid.Defaults()
		defaultsMap := getStructDefaultValue(meta)
		return defaultsMap[fieldName]
	case "openai":
		meta := openai.Defaults()
		defaultsMap := getStructDefaultValue(meta)
		return defaultsMap[fieldName]
	case "storagequeues":
		meta := storagequeues.Defaults()
		defaultsMap := getStructDefaultValue(meta)
		return defaultsMap[fieldName]
	case "kinesis":
		meta := kinesis.Defaults()
		defaultsMap := getStructDefaultValue(meta)
		return defaultsMap[fieldName]
	// Add more cases as needed for other field types
	default:
		return "Sam TODO need to add the call for defaults/examples" // Fallback default
	}
}

func getExampleValueForField(underlyingComponentName, fieldName string) string {
	fmt.Printf("sam fieldName %s", fieldName)
	switch underlyingComponentName {
	case "s3":
		meta := s3.Examples()
		defaultsMap := getStructDefaultValue(meta)
		return defaultsMap[fieldName]
	case "sns":
		meta := sns.Examples()
		defaultsMap := getStructDefaultValue(meta)
		return defaultsMap[fieldName]
	// TODO: skipping blobstorage bc it needs a lot of rework to add here
	// case "blobstorage":
	// 	meta := blobstorage.Examples()
	// 	defaultsMap := getStructDefaultValue(meta)
	// return defaultsMap[fieldName]
	case "dynamodb":
		meta := dynamodb.Examples()
		defaultsMap := getStructDefaultValue(meta)
		return defaultsMap[fieldName]
	case "cosmosdb":
		meta := cosmosdb.Examples()
		defaultsMap := getStructDefaultValue(meta)
		return defaultsMap[fieldName]
	case "gremlinapi":
		meta := gremlinapi.Examples()
		defaultsMap := getStructDefaultValue(meta)
		return defaultsMap[fieldName]
	case "eventgrid":
		meta := eventgrid.Examples()
		defaultsMap := getStructDefaultValue(meta)
		return defaultsMap[fieldName]
	case "openai":
		meta := openai.Examples()
		defaultsMap := getStructDefaultValue(meta)
		return defaultsMap[fieldName]
	case "storagequeues":
		meta := storagequeues.Examples()
		defaultsMap := getStructDefaultValue(meta)
		return defaultsMap[fieldName]
	case "kinesis":
		meta := kinesis.Examples()
		defaultsMap := getStructDefaultValue(meta)
		return defaultsMap[fieldName]
	// Add more cases as needed for other field types
	default:
		return "Sam TODO need to add the call for defaults/examples" // Fallback default
	}
}

func getStructDefaultValue(meta interface{}) map[string]string {
	defaultValues := make(map[string]string)

	// Use reflection to inspect the struct fields
	val := reflect.ValueOf(meta)
	typ := reflect.TypeOf(meta)

	// Ensure we're working with a pointer to a struct
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
		typ = typ.Elem()
	}

	// Iterate over the struct's fields
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldName := typ.Field(i).Name

		// only process exported fields
		// Only process exported fields (fields whose names start with an uppercase letter)
		if fieldName[0] >= 'A' && fieldName[0] <= 'Z' {
			// Directly use the field's value (which was initialized by New) for the default value
			defaultValue := fmt.Sprintf("%v", field.Interface())
			// Store the default value for this field
			defaultValues[fieldName] = defaultValue
		}
	}

	return defaultValues
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
	var urls []metadataschema.URL
	urls = append(urls, metadataschema.URL{
		Title: "Reference",
		URL: fmt.Sprintf("https://docs.dapr.io/reference/components-reference/supported-%s/%s/",
			referenceURLs[componentType], componentName),
	})

	return urls
}
