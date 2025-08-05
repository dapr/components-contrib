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

package cmd

import (
	"fmt"
	"os/exec"
	"slices"
	"strings"

	"github.com/spf13/cobra"
)

// checkComponentRegistrationsCmd represents the check-component-registrations command
// go run . check-component-registrations
// This automates an endgame task that must be completed before a release.
// It checks that all components in components-contrib are properly registered in dapr/dapr.
var checkComponentRegistrationsCmd = &cobra.Command{
	Use:   "check-component-registrations",
	Short: "Checks that all components are properly registered",
	Long: `Checks that all components in components-contrib are properly registered in dapr/dapr.
This includes checking for:
- Registry files in dapr/pkg/components/
- Registration of specific components in dapr/cmd/daprd/components/
- Metadata files in component directories

This is a required step before an official Dapr release.`,
	Run: func(cmd *cobra.Command, args []string) {
		// Navigate to the root of the repo
		err := cwdToRepoRoot()
		if err != nil {
			panic(err)
		}

		fmt.Println("Checking Dapr Component Registrations across runtime in dapr/dapr and components-contrib")
		fmt.Println("========================================================================================")

		checkConversationComponents()
		checkStateComponents()
		checkPubSubComponents()

		// TODO: secretstore, binding, config

		fmt.Println("\nCheck completed!")
	},
}

func init() {
	rootCmd.AddCommand(checkComponentRegistrationsCmd)
}

func checkConversationComponents() {
	fmt.Println("\nChecking conversation components...")
	checkComponents("conversation", []string{}, []string{})
}

func checkStateComponents() {
	fmt.Println("\nChecking state components...")

	// Yugabyte are supported via the postgres component, so in contrib this is covered by the postgresql component in the contrib list.
	// Also, postgres = postgresql, so we can ignore postgres.
	ignoreDaprComponents := []string{"yugabyte", "yugabytedb", "postgres"}
	ignoreContribComponents := []string{"azure.blobstorage.internal"}
	checkComponents("state", ignoreDaprComponents, ignoreContribComponents)
}

func checkPubSubComponents() {
	fmt.Println("\nChecking pubsub components...")

	// mqtt3 = mqtt, so ignore mqtt3 for now in this cli
	ignoreDaprComponents := []string{"mqtt3"}
	ignoreContribComponents := []string{"mqtt3"}
	checkComponents("pubsub", ignoreDaprComponents, ignoreContribComponents)
}

// Note: because this cli cmd changes to the working directory to the root of the repo so pathing is relative to that.
func checkComponents(componentType string, ignoreDaprComponents []string, ignoreContribComponents []string) {
	if err := checkRegistry(componentType); err != nil {
		fmt.Printf("Registry check failed: %v\n", err)
		return
	}

	contribComponents, daprComponents, err := findComponentsInBothRepos(componentType, ignoreContribComponents)
	if err != nil {
		fmt.Printf("Component discovery across repos failed: %v\n", err)
		return
	}

	fmt.Printf("Components to ignore: %v\n", ignoreDaprComponents)
	fmt.Printf("Dapr components before filtering: %v\n", daprComponents)

	// Filter out components to ignore
	filteredDaprComponents := make([]string, 0, len(daprComponents))
	for _, comp := range daprComponents {
		if !slices.Contains(ignoreDaprComponents, comp) {
			filteredDaprComponents = append(filteredDaprComponents, comp)
		} else {
			fmt.Printf("Ignoring component: %s\n", comp)
		}
	}
	daprComponents = filteredDaprComponents

	// Apply vendor prefix mapping and deduplication to both lists.
	// This removes things like the CSP prefixing.
	mappedContribComponents := mapAndDeduplicateComponents(contribComponents)
	mappedDaprComponents := mapAndDeduplicateComponents(daprComponents)

	fmt.Printf("Components in contrib: %d\n", len(mappedContribComponents))
	fmt.Printf("Components registered: %d\n", len(mappedDaprComponents))

	if len(mappedContribComponents) != len(mappedDaprComponents) {
		fmt.Printf("\nNumber of components in contrib and dapr/dapr do not match")
		fmt.Printf("Contrib: %v\n", mappedContribComponents)
		fmt.Printf("Dapr: %v\n", mappedDaprComponents)
		return
	}

	missingRegistrations, missingBuildTags, missingMetadata, err := validateComponents(contribComponents, componentType)
	if err != nil {
		fmt.Printf("Component validation failed: %v\n", err)
		return
	}

	reportResults(missingRegistrations, missingBuildTags, missingMetadata, componentType)
}

func checkRegistry(componentType string) error {
	// Check for default registry singleton
	registryCmd := exec.Command("grep", "-r", `var DefaultRegistry \*Registry = NewRegistry\(\)`, fmt.Sprintf("../dapr/pkg/components/%s/registry.go", componentType))
	_, err := registryCmd.Output()
	if err != nil {
		return fmt.Errorf("could not find default registry: %v", err)
	}

	// Check for RegisterComponent()
	registerComponentCmd := exec.Command("grep", "-r", `Registry) RegisterComponent(componentFactory func(logger.Logger)`, fmt.Sprintf("../dapr/pkg/components/%s/registry.go", componentType))
	_, err = registerComponentCmd.Output()
	if err != nil {
		return fmt.Errorf("could not find RegisterComponent method: %v", err)
	}

	// Check for Create()
	createCmd := exec.Command("grep", "-r", `Registry) Create(name, version, logName string)`, fmt.Sprintf("../dapr/pkg/components/%s/registry.go", componentType))
	_, err = createCmd.Output()
	if err != nil {
		return fmt.Errorf("could not find Create method: %v", err)
	}

	return nil
}

func findComponentsInBothRepos(componentType string, ignoreContribComponents []string) ([]string, []string, error) {
	// Find all components in components-contrib, excluding utility files
	// Configure exclude list based on component type
	var excludeFiles []string
	// keeping this here for now since not all components exclude files... will make func param if needed.
	switch componentType {
	case "state":
		excludeFiles = []string{"--exclude=errors.go", "--exclude=bulk.go", "--exclude=query.go"}
	case "pubsub":
		excludeFiles = []string{"--exclude=envelope.go", "--exclude=responses.go"}
	default:
		excludeFiles = []string{}
	}

	// Build the grep command with dynamic excludes
	grepArgs := []string{"-rl", "--include=*.go"}
	grepArgs = append(grepArgs, excludeFiles...)
	grepArgs = append(grepArgs, "func New", componentType)

	contribCmd := exec.Command("grep", grepArgs...)
	contribOutput, err := contribCmd.Output()
	if err != nil {
		return nil, nil, fmt.Errorf("could not find components within components-contrib: %v", err)
	}

	// Find all registered components in dapr/dapr
	registeredCmd := exec.Command("sh", "-c", fmt.Sprintf(`grep -r "RegisterComponent" ../dapr/cmd/daprd/components/%s_*.go`, componentType))
	registeredOutput, err := registeredCmd.Output()
	if err != nil {
		return nil, nil, fmt.Errorf("could not find all registered components in dapr/dapr: %v", err)
	}

	contribComponents := parseContribComponents(string(contribOutput), componentType, ignoreContribComponents)
	registeredComponents := parseRegisteredComponents(string(registeredOutput))

	return contribComponents, registeredComponents, nil
}

func validateComponents(contribComponents []string, componentType string) ([]string, []string, []string, error) {
	missingRegistrations := []string{}
	missingBuildTags := []string{}
	missingMetadata := []string{}

	for _, contrib := range contribComponents {
		registrationErr := checkComponentRegistration(contrib, componentType)
		buildTagErr := checkBuildTag(contrib, componentType)
		metadataErr := checkMetadataFile(contrib, componentType)

		if registrationErr != nil {
			missingRegistrations = append(missingRegistrations, contrib)
		}
		if buildTagErr != nil {
			missingBuildTags = append(missingBuildTags, contrib)
		}
		if metadataErr != nil {
			missingMetadata = append(missingMetadata, contrib)
		}
	}

	return missingRegistrations, missingBuildTags, missingMetadata, nil
}

func checkComponentRegistration(contrib, componentType string) error {
	// For registration files, convert component name to file naming convention
	// EX: aws.bedrock -> conversation_bedrock.go
	// EX: alicloud.tablestore -> state_alicloud_tablestore.go
	compFileName := getRegistrationFileName(contrib, componentType)
	fmt.Printf("sam the comp file name: %s\n", compFileName)

	fileExistsCmd := exec.Command("ls", compFileName)
	_, err := fileExistsCmd.Output()
	if err != nil {
		return fmt.Errorf("registration file for '%s' not found: %s", contrib, compFileName)
	}

	if err := checkComponentIsActuallyRegisteredInFile(contrib, compFileName); err != nil {
		return fmt.Errorf("component '%s' not registered in %s: %v", contrib, compFileName, err)
	}

	fmt.Printf("Component '%s' properly registered in %s\n", contrib, compFileName)
	return nil
}

// checkComponentIsActuallyRegisteredInFile basically checks for if the component name string is within the file
// to ensure it is properly registered within runtime.
func checkComponentIsActuallyRegisteredInFile(contrib, registrationFile string) error {
	registrationCheckCmd := exec.Command("grep", "-r", `"`, registrationFile)
	registrationOutput, err := registrationCheckCmd.Output()
	if err != nil {
		return fmt.Errorf("could not read registration file: %v", err)
	}

	lines := strings.FieldsFunc(string(registrationOutput), func(r rune) bool {
		return r == '\n'
	})

	for _, line := range lines {
		if strings.Contains(line, "RegisterComponent") {
			quoteIndex := strings.Index(line, "\"")
			if quoteIndex > 0 {
				rest := line[quoteIndex+1:]
				endQuoteIndex := strings.Index(rest, "\"")
				if endQuoteIndex > 0 {
					registeredName := rest[:endQuoteIndex]
					// Check exact match
					if registeredName == contrib {
						return nil
					}
					// Check vendor prefix mapping (e.g., hashicorp.consul -> consul)
					normalizedName := normalizeComponentName(contrib)
					fmt.Printf("sam the normalized name: %s with registered name: %s\n", normalizedName, registeredName)
					// registeredName can be something like azure.blobstorage, so need to see if contains too
					if registeredName == normalizedName || strings.Contains(registeredName, normalizedName) {
						return nil
					}
				}
			}
		}
	}

	return fmt.Errorf("component '%s' not found in registration file '%s'", contrib, registrationFile)
}

func checkBuildTag(contrib, componentType string) error {
	compFileName := getRegistrationFileName(contrib, componentType)

	buildTagCmd := exec.Command("grep", "-q", "go:build allcomponents", compFileName)
	_, err := buildTagCmd.Output()
	if err != nil {
		return fmt.Errorf("build tag 'go:build allcomponents' not found in %s", compFileName)
	}

	fmt.Printf("Build tag found in %s\n", compFileName)
	return nil
}

// normalizeComponentName strips vendor prefixes and versions from component names
// EX: aws.bedrock -> bedrock
// EX: postgresql.v1 -> postgresql
// EX: azure.blobstorage.v1 -> blobstorage
// EX: hashicorp.consul -> consul
func normalizeComponentName(contrib string) string {
	if !strings.Contains(contrib, ".") {
		return contrib
	}

	parts := strings.Split(contrib, ".")
	vendorPrefixes := []string{"hashicorp", "aws", "azure", "gcp", "alicloud", "oci", "cloudflare"}
	versionSuffixes := []string{"v1", "v2", "internal"}

	// Handle 2-part names (vendor.component)
	if len(parts) == 2 {
		// Strip vendor prefixes
		if slices.Contains(vendorPrefixes, parts[0]) {
			return parts[1]
		}
		// Strip version suffixes
		if slices.Contains(versionSuffixes, parts[1]) {
			return parts[0]
		}
	}

	// Handle 3+ part names (vendor.component.version)
	if len(parts) >= 3 {
		// Check if first part is a vendor prefix
		if slices.Contains(vendorPrefixes, parts[0]) {
			// Check if last part is a version suffix
			if slices.Contains(versionSuffixes, parts[len(parts)-1]) {
				// Return middle parts: azure.blobstorage.v1 -> blobstorage
				return strings.Join(parts[1:len(parts)-1], ".")
			}
			// Return all parts except vendor: azure.cosmosdb -> cosmosdb
			return strings.Join(parts[1:], ".")
		}
		// Check if last part is a version suffix
		if slices.Contains(versionSuffixes, parts[len(parts)-1]) {
			// Return all parts except version: component.v1 -> component
			return strings.Join(parts[:len(parts)-1], ".")
		}
	}

	return contrib
}

func mapAndDeduplicateComponents(components []string) []string {
	mappedComponents := make([]string, 0, len(components))
	seen := make(map[string]bool)

	for _, component := range components {
		simpleName := normalizeComponentName(component)
		if !seen[simpleName] {
			mappedComponents = append(mappedComponents, simpleName)
			seen[simpleName] = true
		}
	}

	return mappedComponents
}

func checkMetadataFile(contrib, componentType string) error {
	metadataFile := getMetadataFilePath(contrib, componentType)

	metadataExistsCmd := exec.Command("ls", metadataFile)
	_, err := metadataExistsCmd.Output()
	if err != nil {
		return fmt.Errorf("metadata file for '%s' not found: %s", contrib, metadataFile)
	}

	fmt.Printf("Metadata file for '%s' found: %s\n", contrib, metadataFile)
	return nil
}

func getMetadataFilePath(contrib, componentType string) string {
	if strings.Contains(contrib, ".") {
		// For nested components like "aws.bedrock", split and join with "/"
		parts := strings.Split(contrib, ".")
		return fmt.Sprintf("%s/%s/metadata.yaml", componentType, strings.Join(parts, "/"))
	}
	// For simple components like "echo"
	return fmt.Sprintf("%s/%s/metadata.yaml", componentType, contrib)
}

func reportResults(missingRegistrations, missingBuildTags, missingMetadata []string, componentType string) {
	totalIssues := len(missingRegistrations) + len(missingBuildTags) + len(missingMetadata)

	if totalIssues > 0 {
		fmt.Printf("\nValidation Results for %s components:\n", componentType)

		if len(missingRegistrations) > 0 {
			fmt.Printf("Missing registrations: %d\n", len(missingRegistrations))
			for _, m := range missingRegistrations {
				fmt.Printf("  - %s\n", m)
			}
		}

		if len(missingBuildTags) > 0 {
			fmt.Printf("Missing build tags: %d\n", len(missingBuildTags))
			for _, m := range missingBuildTags {
				fmt.Printf("  - %s\n", m)
			}
		}

		if len(missingMetadata) > 0 {
			fmt.Printf("Missing metadata files: %d\n", len(missingMetadata))
			for _, m := range missingMetadata {
				fmt.Printf("  - %s\n", m)
			}
		}
	} else {
		fmt.Printf("All %s components are properly configured\n", componentType)
	}
}

func parseContribComponents(output, componentType string, ignoreContribComponents []string) []string {
	components := []string{}
	lines := strings.FieldsFunc(output, func(r rune) bool {
		return r == '\n'
	})

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		componentName := extractComponentNameFromPath(line, componentType)
		if componentName != "" {
			if !slices.Contains(ignoreContribComponents, componentName) {
				components = append(components, componentName)
			}
		}
	}

	return components
}

// extractComponentNameFromPath extracts a component name from file path
// EX: conversation/echo/echo.go -> echo
// EX: conversation/aws/bedrock/bedrock.go -> aws.bedrock
func extractComponentNameFromPath(filePath, componentType string) string {
	// Find the component type directory in the path
	componentTypeIndex := strings.Index(filePath, componentType+"/")
	if componentTypeIndex < 0 {
		return ""
	}

	// Extract the path after componentType/
	pathAfterComponentType := filePath[componentTypeIndex+len(componentType+"/"):]
	pathWithoutExt := strings.TrimSuffix(pathAfterComponentType, ".go")

	// Convert directory structure to component name
	// Replace slashes with dots, but only up to the last directory
	parts := strings.Split(pathWithoutExt, "/")
	if len(parts) >= 2 {
		// For nested components: aws/bedrock/bedrock.go -> aws.bedrock
		// Take all parts except the last (which is the filename)
		dirParts := parts[:len(parts)-1]
		return strings.Join(dirParts, ".")
	} else if len(parts) == 1 {
		// For simple components: echo/echo.go -> echo
		// The part before the slash is the component name
		return parts[0]
	}

	return ""
}

// getRegistrationFileName converts a component name to its registration file name.
// Note: runtime does not deliniate versions in file names so we ignore that in pathing.
// EX: aws.bedrock -> ../dapr/cmd/daprd/components/conversation_bedrock.go
// EX: alicloud.tablestore -> ../dapr/cmd/daprd/components/state_alicloud_tablestore.go
// EX: postgresql.v1 -> ../dapr/cmd/daprd/components/state_postgres.go
func getRegistrationFileName(contrib, componentType string) string {
	// For versioned components, strip the version suffix
	parts := strings.Split(contrib, ".")
	if len(parts) >= 2 {
		// Check if the last part is a version (v1, v2, etc.)
		lastPart := parts[len(parts)-1]
		if strings.HasPrefix(lastPart, "v") && len(lastPart) <= 3 {
			// Remove the version part
			baseName := strings.Join(parts[:len(parts)-1], ".")
			fileName := strings.ReplaceAll(baseName, ".", "_")
			// TODO: update runtime file names to match this
			if fileName == "postgresql" {
				fileName = "postgres"
			}
			fmt.Printf("sam the file name: %s\n", fileName)
			return fmt.Sprintf("../dapr/cmd/daprd/components/%s_%s.go", componentType, fileName)
		}
	}

	// TODO: update runtime to match??? it's bc contrib has it as hashicorp.consul but runtime has it as "consul"
	if contrib == "hashicorp.consul" {
		normalizedName := normalizeComponentName(contrib)
		fileName := strings.ReplaceAll(normalizedName, ".", "_")
		return fmt.Sprintf("../dapr/cmd/daprd/components/%s_%s.go", componentType, fileName)
	}

	fileName := strings.ReplaceAll(contrib, ".", "_")
	return fmt.Sprintf("../dapr/cmd/daprd/components/%s_%s.go", componentType, fileName)
}

// parseRegisteredComponents parses the output of the grep command to find all registered components
func parseRegisteredComponents(output string) []string {
	components := []string{}
	lines := strings.FieldsFunc(output, func(r rune) bool {
		return r == '\n'
	})

	for _, line := range lines {
		if strings.Contains(line, "RegisterComponent") {
			quoteIndex := strings.Index(line, "\"")
			if quoteIndex > 0 {
				rest := line[quoteIndex+1:]
				endQuoteIndex := strings.Index(rest, "\"")
				if endQuoteIndex > 0 {
					componentName := rest[:endQuoteIndex]
					components = append(components, componentName)
				}
			}
		}
	}

	return components
}
