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
	"github.com/dapr/components-contrib/build-tools/pkg/dictionary"
	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var (
	//go:embed builtin-authentication-profiles.yaml
	builtinAuthenticationProfilesYAML []byte
)

// generateComponentMetadataCmd represents the command to generate the yaml manifest for each component
var generateComponentMetadataCmd = &cobra.Command{
	Use:   "generate-component-metadata",
	Short: "Generates the component metadata yaml file per component",
	Long:  `Generates the component metadata yaml file per component so we don't have to rely on community to manually create it.`,
	Run: func(cmd *cobra.Command, args []string) {
		// Navigate to the root of the repo
		err := cwdToRepoRoot()
		if err != nil {
			panic(err)
		}

		// Find all components
		list, err := componentmetadata.FindValidComponents(ComponentFolders, ExcludeFolders)
		fmt.Println("sam list %v", list)
		if err != nil {
			panic(err)
		}

		// Load the metadata for all components
		bundle := metadataschema.Bundle{
			SchemaVersion: "v1",
			Date:          time.Now().Format("20060102150405"),
			Components:    make([]*metadataschema.ComponentMetadata, 0, len(list)),
		}

		// step 0. gather all components initial data for static info we hard code.
		var components []*metadataschema.ComponentMetadata
		// bindings
		bindings := dictionary.GetAllBindings()
		components = append(components, bindings...)

		// state
		state := dictionary.GetAllStateStores()
		components = append(components, state...)

		// configuration
		configs := dictionary.GetAllConfiguration()
		components = append(components, configs...)

		// conversation
		convo := dictionary.GetAllConversation()
		components = append(components, convo...)

		// middleware
		mid := dictionary.GetAllMiddleware()
		components = append(components, mid...)

		// pubsub
		ps := dictionary.GetAllPubsubs()
		components = append(components, ps...)

		// secretstores
		secrets := dictionary.GetAllSecretStores()
		components = append(components, secrets...)

		// create a map for components based on their names
		componentMap := make(map[string]*metadataschema.ComponentMetadata)
		for _, component := range components {
			// step 1. auto-generate metadata based on struct tags
			fmt.Printf("sam component type %s and name %s\n", component.Type, component.Name)
			metadataFilePath := fmt.Sprintf("%s/%s", component.Type, convertPrefixToPath(component.Name))
			metadataFile := fmt.Sprintf("%s/metadata.go", metadataFilePath)
			finalCompName := getFinalComponentName(component.Name)
			fmt.Printf("sam finalCompName %s", finalCompName)
			var authProfileMetadataMap map[string][]metadataschema.Metadata
			component.Metadata, authProfileMetadataMap, err = generateMetadataFromStructs(metadataFile, finalCompName+"Metadata", finalCompName)
			fmt.Printf("sam the metadata: \n", component.Metadata)
			if err != nil {
				fmt.Printf("sam error: %v", err)
			}

			// authProfileFile := fmt.Sprintf("%s/authentication-profiles.yaml", metadataFilePath)
			// authProfiles, err := generateAuthProfiles(authProfileFile)
			// if err != nil {
			// 	fmt.Printf("sam error: %v", err)
			// }
			ap := transformToAuthProfileMetadata(component, authProfileMetadataMap)
			component.AuthenticationProfiles = ap
			// fmt.Printf("sam the auth profiles %v\n", authProfiles)
			// step 1.5. auto-generate any other missing fields (e.g., auth profile)
			// TODO: Add logic for other fields if necessary

			// add component to the map
			componentMap[component.Name] = component

			component.SchemaVersion = "v2" // TODO: is this okay bc it's technically the same content, just autogenerated now.
			component.URLs = supportedComponentURL(component.Type, finalCompName)
			fmt.Printf("URLs set for %s: %+v\n", component.Name, component.URLs)

			fmt.Printf("sam the urls %v\n", component.URLs)

			// Append built-in metadata properties
			err = component.AppendBuiltin()
			if err != nil {
				fmt.Printf("sam error from AppendBuildin %v", err)
			}

			// // Parse builtin-authentication-profiles.yaml
			// var builtinAuthProfiles map[string][]metadataschema.AuthenticationProfile
			// err = yaml.Unmarshal(builtinAuthenticationProfilesYAML, &metadataschema.BuiltinAuthenticationProfiles)
			// if err != nil {
			// 	panic(err)
			// }

			// Append built-in authentication profiles metadata fields
			for _, profile := range component.BuiltInAuthenticationProfiles {
				appendProfiles, err := metadataschema.ParseBuiltinAuthenticationProfile(profile, component.Title)
				if err != nil {
					fmt.Printf("sam error from ParseBuiltinAuthenticationProfile %v", err)
				}

				component.AuthenticationProfiles = append(component.AuthenticationProfiles, appendProfiles...)
			}

			bundle.Components = append(bundle.Components, component)

			//  write file for components
			fileName := fmt.Sprintf("%s.yaml", component.Name)
			if err := writeYAMLFile(metadataFilePath, *component); err != nil {
				fmt.Printf("Failed to write %s: %v\n", fileName, err)
			}
		}

		// print everything out if i want
		// enc := json.NewEncoder(os.Stdout)
		// enc.SetEscapeHTML(false)
		// enc.SetIndent("", "  ")
		// err = enc.Encode(bundle.Components)
		// if err != nil {
		// 	panic(fmt.Errorf("failed to encode bundle to JSON: %w", err))
		// }
	},
}

func transformToAuthProfileMetadata(component *metadataschema.ComponentMetadata, authProfileMetadataFieldsMap map[string][]metadataschema.Metadata) []metadataschema.AuthenticationProfile {
	fmt.Printf("Processing authProfileMetadataFieldsMap: %v\n", authProfileMetadataFieldsMap)
	if len(component.AuthenticationProfiles) == 0 {
		return nil
	}

	var finalAP []metadataschema.AuthenticationProfile
	for _, authProfile := range component.AuthenticationProfiles {
		// Normalize the title for map lookup
		normalizedTitle := strings.ToLower(authProfile.Title)
		normalizedTitle = strings.ReplaceAll(normalizedTitle, "'", "")
		normalizedTitle = strings.ReplaceAll(normalizedTitle, " ", "") // Remove spaces
		normalizedTitle = strings.TrimSpace(normalizedTitle)           // Remove leading/trailing spaces if any
		fmt.Printf("Normalized title for lookup: '%s'\n", normalizedTitle)

		if metadataFields, exists := authProfileMetadataFieldsMap[normalizedTitle]; exists && metadataFields != nil {
			fmt.Printf("Found metadata for profile '%s'\n", authProfile.Title)
			authProfile.Metadata = metadataFields
			finalAP = append(finalAP, authProfile)
		} else {
			fmt.Printf("No metadata found for normalized title: '%s'\n", normalizedTitle)
		}
	}

	fmt.Printf("Final Authentication Profiles: %v\n", finalAP)
	return finalAP
}

func getFinalComponentName(longName string) string {
	parts := strings.SplitAfter(longName, ".")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

func generateAuthProfiles(filePath string) ([]metadataschema.AuthenticationProfile, error) {

	// Read the file
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Parse the YAML content into a slice of AuthenticationProfile structs
	var authProfiles []metadataschema.AuthenticationProfile
	err = yaml.Unmarshal(data, &authProfiles)
	if err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	return authProfiles, nil
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

func init() {
	rootCmd.AddCommand(generateComponentMetadataCmd)
}

func writeYAMLFile(filePath string, componentMetadata metadataschema.ComponentMetadata) error {
	fmt.Printf("sam writing to %v", filePath)
	file, err := os.Create(filePath + "/metadatanew.yaml")
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer file.Close()

	// update the field names to lowercase
	for i := range componentMetadata.Metadata {
		componentMetadata.Metadata[i].Name = lowercaseFirstLetter(componentMetadata.Metadata[i].Name)
	}

	yamlEncoder := yaml.NewEncoder(file)
	yamlEncoder.SetIndent(2)
	if err := yamlEncoder.Encode(&componentMetadata); err != nil {
		return fmt.Errorf("error encoding YAML: %w", err)
	}

	return nil
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
