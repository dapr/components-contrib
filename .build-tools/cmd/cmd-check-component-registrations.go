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

var verbose bool

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
		checkSecretStoreComponents()
		checkBindingComponents()
		checkConfigurationComponents()
		checkLockComponents()
		checkCryptographyComponents()
		checkNameResolutionComponents()
		checkMiddlewareComponents()

		fmt.Println("\nCheck completed!")
	},
}

func init() {
	checkComponentRegistrationsCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose output")
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
	// also sqlite3 is an alias for sqlite so we can ignore one.
	ignoreDaprComponents := []string{"yugabyte", "yugabytedb", "postgres", "sqlite3"}
	ignoreContribComponents := []string{"azure.blobstorage.internal"}
	checkComponents("state", ignoreDaprComponents, ignoreContribComponents)
}

func checkPubSubComponents() {
	fmt.Println("\nChecking pubsub components...")

	// mqtt3 = mqtt, so ignore mqtt (keep mqtt3 since it exists in contrib)
	// azure.servicebusqueues is an alias for azure.servicebus.queues (keep azure.servicebus.queues) so ignore it
	// azure.servicebus is an alias for azure.servicebus.topics (keep azure.servicebus.topics) so ignore it
	ignoreDaprComponents := []string{"mqtt", "azure.servicebusqueues", "azure.servicebus"}
	ignoreContribComponents := []string{}
	checkComponents("pubsub", ignoreDaprComponents, ignoreContribComponents)
}

func checkSecretStoreComponents() {
	fmt.Println("\nChecking secretstore components...")
	checkComponents("secretstores", []string{}, []string{})
}

func checkBindingComponents() {
	fmt.Println("\nChecking bindings components...")
	// ignore servicebus.queues as runtime has an alias on this so we're checking for servicebusqueues
	ignoreDaprComponents := []string{"mqtt3", "azure.servicebus.queues", "postgresql"}
	ignoreContribComponents := []string{}
	checkComponents("bindings", ignoreDaprComponents, ignoreContribComponents)
}

func checkConfigurationComponents() {
	fmt.Println("\nChecking configuration components...")
	ignoreDaprComponents := []string{"postgresql"}
	checkComponents("configuration", ignoreDaprComponents, []string{})
}

func checkLockComponents() {
	fmt.Println("\nChecking lock components...")
	checkComponents("lock", []string{}, []string{})
}

func checkCryptographyComponents() {
	fmt.Println("\nChecking cryptography components...")
	// below is not actually a component. Cryptography section in contrib needs quite a bit of clean up/organization to clean this up so I don't have to "ignore" it.
	ignoreContribComponents := []string{"pubkey_cache"}
	// We ignore the dapr prefixes here bc they are captured properly without the dapr prefix.
	ignoreDaprComponents := []string{"dapr.localstorage", "dapr.kubernetes.secrets", "dapr.jwks"}
	// TODO: in future update this to cryptography once we have a cryptography component in contrib and not crypto components
	checkComponents("crypto", ignoreDaprComponents, ignoreContribComponents)
}

func checkNameResolutionComponents() {
	fmt.Println("\nChecking name resolution components...")
	checkComponents("nameresolution", []string{}, []string{})
}

func checkMiddlewareComponents() {
	fmt.Println("\nChecking middleware components...")
	// uppercase is a component only in runtime which doesn't make sense give the nameresolution components have no real metadata of their own for the most part;
	// however, it's definition is only in runtime and not in contrib so we must ignore it.
	ignoreDaprComponents := []string{"uppercase"}
	ignoreContribComponents := []string{}
	checkComponents("middleware", ignoreDaprComponents, ignoreContribComponents)
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

	if verbose {
		fmt.Printf("Components to ignore: %v\n", ignoreDaprComponents)
		fmt.Printf("Dapr components before filtering: %v\n", daprComponents)
	}

	// Filter out components to ignore
	filteredDaprComponents := make([]string, 0, len(daprComponents))
	for _, comp := range daprComponents {
		if !slices.Contains(ignoreDaprComponents, comp) {
			filteredDaprComponents = append(filteredDaprComponents, comp)
		} else {
			if verbose {
				fmt.Printf("Ignoring component: %s\n", comp)
			}
		}
	}
	daprComponents = filteredDaprComponents

	filteredContribComponents := make([]string, 0, len(contribComponents))
	for _, comp := range contribComponents {
		if !slices.Contains(ignoreContribComponents, comp) {
			filteredContribComponents = append(filteredContribComponents, comp)
		} else {
			if verbose {
				fmt.Printf("Ignoring component: %s\n", comp)
			}
		}
	}
	contribComponents = filteredContribComponents

	// Apply vendor prefix mapping and deduplication to both lists.
	// This removes things like the CSP and/or vendor prefixing.
	mappedContribComponents := mapAndDeduplicateComponents(contribComponents)
	mappedDaprComponents := mapAndDeduplicateComponents(daprComponents)

	fmt.Printf("Components in contrib: %d\n", len(mappedContribComponents))
	fmt.Printf("Components registered in runtime: %d\n", len(mappedDaprComponents))

	if len(mappedContribComponents) != len(mappedDaprComponents) {
		fmt.Println("\nNumber of components in contrib and dapr/dapr do not match")
		fmt.Printf("Contrib: %v\n\n", mappedContribComponents)
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

// getRegistryPath is needed to get the correct registry file for the component,
// and middleware components are nested under a specific http dir, so we must handle this case.
func getRegistryPath(componentType string) string {
	if componentType == "middleware" {
		return fmt.Sprintf("../dapr/pkg/components/%s/http/registry.go", componentType)
	}
	return fmt.Sprintf("../dapr/pkg/components/%s/registry.go", componentType)
}

func checkRegistry(componentType string) error {
	registryPath := getRegistryPath(componentType)

	// Check for default registry singleton
	registryCmd := exec.Command("grep", "-r", `var DefaultRegistry \*Registry = NewRegistry\(\)`, registryPath)
	_, err := registryCmd.Output()
	if err != nil {
		return fmt.Errorf("could not find default registry: %v", err)
	}

	// Check for RegisterComponent()
	if componentType != "bindings" {
		registerComponentCmd := exec.Command("grep", "-r", `Registry) RegisterComponent(componentFactory`, registryPath)
		_, err := registerComponentCmd.Output()
		if err != nil {
			return fmt.Errorf("could not find RegisterComponent method: %v", err)
		}

		// Check for Create()
		createCmd := exec.Command("grep", "-r", `Registry) Create(name, version`, registryPath)
		_, err = createCmd.Output()
		if err != nil {
			return fmt.Errorf("could not find Create method: %v", err)
		}
	} else {
		registerInputBindingCmd := exec.Command("grep", "-r", `Registry) RegisterInputBinding(componentFactory func(logger.Logger)`, registryPath)
		_, err := registerInputBindingCmd.Output()
		if err != nil {
			return fmt.Errorf("could not find registerInputBindingCmd method: %v", err)
		}

		registerOutputBindingCmd := exec.Command("grep", "-r", `Registry) RegisterOutputBinding(componentFactory func(logger.Logger)`, registryPath)
		_, err = registerOutputBindingCmd.Output()
		if err != nil {
			return fmt.Errorf("could not find registerOutputBindingCmd method: %v", err)
		}

		// Check for Creates
		createInputBindingCmd := exec.Command("grep", "-r", `Registry) CreateInputBinding(name, version, logName string)`, registryPath)
		_, err = createInputBindingCmd.Output()
		if err != nil {
			return fmt.Errorf("could not find CreateInputBinding method: %v", err)
		}

		createOutputBindingCmd := exec.Command("grep", "-r", `Registry) CreateOutputBinding(name, version, logName string)`, registryPath)
		_, err = createOutputBindingCmd.Output()
		if err != nil {
			return fmt.Errorf("could not find CreateInputBinding method: %v", err)
		}
	}

	return nil
}

func findComponentsInBothRepos(componentType string, ignoreContribComponents []string) ([]string, []string, error) {
	// Find all components in components-contrib, excluding utility files
	// Configure exclude list based on component type
	var excludeFiles []string
	// we have to exclude certain files that match the grep, but are not components.
	// In future, this can be cleaned up if files are moved to proper pkg like directories.
	switch componentType {
	case "state":
		excludeFiles = []string{"--exclude=errors.go", "--exclude=bulk.go", "--exclude=query.go"}
	case "pubsub":
		excludeFiles = []string{"--exclude=envelope.go", "--exclude=responses.go"}
	case "bindings":
		excludeFiles = []string{"--exclude=client.go"}
	case "crypto":
		excludeFiles = []string{"--exclude=key.go", "--exclude=pubkey_cache.go"}
	case "middleware":
		excludeFiles = []string{"--exclude=mock*"}
	default:
		excludeFiles = []string{}
	}

	grepArgs := []string{"-rl", "--include=*.go"}
	grepArgs = append(grepArgs, excludeFiles...)
	grepArgs = append(grepArgs, "func New", componentType)

	contribCmd := exec.Command("grep", grepArgs...)
	contribOutput, err := contribCmd.Output()
	if err != nil {
		return nil, nil, fmt.Errorf("could not find components within components-contrib: %v", err)
	}

	// Find all registered components in dapr/dapr
	var registeredOutput []byte
	if componentType != "bindings" {
		var searchPattern string
		// bc middleware components are nested under a specific http dir, we must handle this case.
		if componentType == "middleware" {
			searchPattern = "../dapr/cmd/daprd/components/middleware_http_*.go"
		} else {
			searchPattern = fmt.Sprintf("../dapr/cmd/daprd/components/%s_*.go", componentType)
		}
		registeredCmd := exec.Command("sh", "-c", fmt.Sprintf(`grep -r "RegisterComponent" %s`, searchPattern))
		registeredOutput, err = registeredCmd.Output()
		if err != nil {
			return nil, nil, fmt.Errorf("could not find all registered components in dapr/dapr: %v", err)
		}
	} else {
		// For bindings, capture both RegisterInputBinding and RegisterOutputBinding
		registeredCmd := exec.Command("sh", "-c", fmt.Sprintf(`grep -r "RegisterInputBinding\|RegisterOutputBinding" ../dapr/cmd/daprd/components/%s_*.go`, componentType))
		registeredOutput, err = registeredCmd.Output()
		if err != nil {
			return nil, nil, fmt.Errorf("could not find all registered components in dapr/dapr: %v", err)
		}
	}

	contribComponents := parseContribComponents(string(contribOutput), componentType, ignoreContribComponents)
	registeredComponents := parseRegisteredComponents(string(registeredOutput), componentType)

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
	// EX: alicloud.tablestore -> state_alicloud_tablestore.go
	compFileName := getRegistrationFileName(contrib, componentType)

	fileExistsCmd := exec.Command("ls", compFileName)
	_, err := fileExistsCmd.Output()
	if err != nil {
		return fmt.Errorf("registration file for '%s' not found: %s", contrib, compFileName)
	}

	// Check if this is a versioned component
	// EX: postgresql.v1, azure.blobstorage.v2
	parts := strings.Split(contrib, ".")
	if len(parts) >= 2 {
		lastPart := parts[len(parts)-1]
		if strings.HasPrefix(lastPart, "v") && len(lastPart) <= 3 {
			// This is a versioned component, check if the base component is registered
			baseComponent := strings.Join(parts[:len(parts)-1], ".")
			if err := checkComponentIsActuallyRegisteredInFile(baseComponent, compFileName); err != nil {
				return fmt.Errorf("versioned component '%s' base component '%s' not registered in %s: %v", contrib, baseComponent, compFileName, err)
			}
			if verbose {
				fmt.Printf("Versioned component '%s' properly registered via base component '%s' in %s\n", contrib, baseComponent, compFileName)
			}
			return nil
		}
	}

	if err := checkComponentIsActuallyRegisteredInFile(contrib, compFileName); err != nil {
		return fmt.Errorf("component '%s' not registered in %s: %v", contrib, compFileName, err)
	}

	if verbose {
		fmt.Printf("Component '%s' properly registered in %s\n", contrib, compFileName)
	}
	return nil
}

// checkComponentIsActuallyRegisteredInFile basically checks for if the component name string is within the file
// to ensure it is properly registered within runtime.
func checkComponentIsActuallyRegisteredInFile(contrib, registrationFile string) error {
	// Use grep to find the exact component name in quotes
	grepCmd := exec.Command("grep", "-q", fmt.Sprintf(`"%s"`, contrib), registrationFile)
	_, err := grepCmd.Output()
	if err != nil {
		return fmt.Errorf("component '%s' not found in registration file '%s'", contrib, registrationFile)
	}

	return nil
}

func checkBuildTag(contrib, componentType string) error {
	compFileName := getRegistrationFileName(contrib, componentType)

	// Check for "go:build allcomponents"
	buildTagCmd := exec.Command("grep", "-q", "allcomponents", compFileName)
	_, err := buildTagCmd.Output()
	if err != nil {
		return fmt.Errorf("build tag for 'allcomponents' not found in %s", compFileName)
	}

	// TODO: in future, add check for stable components
	return nil
}

// normalizeComponentName strips vendor prefixes and versions from component names
// EX: aws.bedrock -> bedrock
// EX: postgresql.v1 -> postgresql
// EX: azure.blobstorage.v1 -> blobstorage
func normalizeComponentName(contrib string) string {
	if !strings.Contains(contrib, ".") {
		return contrib
	}

	parts := strings.Split(contrib, ".")
	// Note: I am putting http as a vendor prefix, but that should be removed and all http middleware components should be http.blah bc
	// in future we could add grpc middleware components.
	vendorPrefixes := []string{"hashicorp", "aws", "azure", "gcp", "alicloud", "oci", "cloudflare", "ibm", "tencentcloud", "huaweicloud", "twilio", "http"}
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

	if verbose {
		fmt.Printf("Metadata file for '%s' found: %s\n", contrib, metadataFile)
	}
	return nil
}

func getMetadataFilePath(contrib, componentType string) string {
	// Special handling for HTTP middleware components
	// The metadata files are located at middleware/http/componentname/metadata.yaml
	if componentType == "middleware" {
		return fmt.Sprintf("%s/http/%s/metadata.yaml", componentType, contrib)
	}

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

		// Special handling for HTTP middleware components
		// middleware/http/componentname/ -> componentname (not http.componentname)
		if componentType == "middleware" && len(dirParts) >= 2 && dirParts[0] == "http" {
			return strings.Join(dirParts[1:], ".")
		}

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
			return fmt.Sprintf("../dapr/cmd/daprd/components/%s_%s.go", componentType, fileName)
		}
	}

	fileName := strings.ReplaceAll(contrib, ".", "_")

	// Special handling for HTTP middleware components
	// The registration files are named middleware_http_componentname.go
	if componentType == "middleware" {
		return fmt.Sprintf("../dapr/cmd/daprd/components/%s_http_%s.go", componentType, fileName)
	}

	return fmt.Sprintf("../dapr/cmd/daprd/components/%s_%s.go", componentType, fileName)
}

// parseRegisteredComponents parses the output of the grep command to find all registered components
func parseRegisteredComponents(output string, componentType string) []string {
	components := []string{}
	lines := strings.FieldsFunc(output, func(r rune) bool {
		return r == '\n'
	})

	// Extract unique file paths from grep output
	uniqueFiles := make(map[string]bool)
	for _, line := range lines {
		colonIndex := strings.Index(line, ":")
		if colonIndex == -1 {
			continue
		}
		filePath := line[:colonIndex]
		uniqueFiles[filePath] = true
	}

	// Process each registration file
	for filePath := range uniqueFiles {
		// Skip uppercase component as it contains many magic strings that aren't component names
		if strings.Contains(filePath, "uppercase") {
			continue
		}

		// Read the entire file content
		cmd := exec.Command("cat", filePath)
		fileContent, err := cmd.Output()
		if err != nil {
			continue
		}

		fileLines := strings.FieldsFunc(string(fileContent), func(r rune) bool {
			return r == '\n'
		})

		// Check if this file contains registration calls and find the starting line
		hasRegistrationCalls := false
		lineToContinueFrom := 0
		for i, line := range fileLines {
			if componentType == "bindings" {
				if strings.Contains(line, "RegisterInputBinding") || strings.Contains(line, "RegisterOutputBinding") {
					hasRegistrationCalls = true
					lineToContinueFrom = i
					break
				}
			} else {
				if strings.Contains(line, "RegisterComponent") {
					hasRegistrationCalls = true
					lineToContinueFrom = i
					break
				}
			}
		}

		// Extract component names from lines starting from the first registration call
		if hasRegistrationCalls {
			for i, line := range fileLines {
				if i < lineToContinueFrom {
					continue
				}
				quotedStrings := extractAllQuotedStrings(line)
				if len(quotedStrings) > 0 {
					components = append(components, quotedStrings...)
				}
			}
		}
	}

	return components
}

// extractAllQuotedStrings extracts all quoted strings from a line
func extractAllQuotedStrings(line string) []string {
	var quotedStrings []string
	start := 0
	for {
		quoteIndex := strings.Index(line[start:], "\"")
		if quoteIndex == -1 {
			break
		}
		quoteIndex += start

		rest := line[quoteIndex+1:]
		endQuoteIndex := strings.Index(rest, "\"")
		if endQuoteIndex == -1 {
			break
		}

		quotedString := rest[:endQuoteIndex]
		if quotedString != "v1" && quotedString != "v2" {
			quotedStrings = append(quotedStrings, quotedString)
		}

		start = quoteIndex + 1 + endQuoteIndex + 1
	}

	return quotedStrings
}
