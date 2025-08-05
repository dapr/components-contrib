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

		// TODO: secretstore, pubsub, binding, config

		fmt.Println("\nCheck completed!")
	},
}

func init() {
	rootCmd.AddCommand(checkComponentRegistrationsCmd)
}

func checkConversationComponents() {
	fmt.Println("\nChecking conversation components...")
	checkComponents("conversation")
}

func checkStateComponents() {
	fmt.Println("\nChecking state components...")

	// ignoreDaprComponents := []string{"yugabyte, yugabytedb"}
	checkComponents("state")
}

// Note: because this cli cmd changes to the working directory to the root of the repo so pathing is relative to that.
func checkComponents(componentType string) {
	if err := checkRegistry(componentType); err != nil {
		fmt.Printf("Registry check failed: %v\n", err)
		return
	}

	contribComponents, daprComponents, err := findComponentsInBothRepos(componentType)
	if err != nil {
		fmt.Printf("Component discovery across repos failed: %v\n", err)
		return
	}

	// Apply vendor prefix mapping and deduplication to both lists.
	// This removes things like the CSP prefixing.
	mappedContribComponents := mapAndDeduplicateComponents(contribComponents)
	mappedDaprComponents := mapAndDeduplicateComponents(daprComponents)

	fmt.Printf("Components in contrib: %d\n", len(mappedContribComponents))
	fmt.Printf("Components registered: %d\n", len(mappedDaprComponents))

	if len(mappedContribComponents) != len(mappedDaprComponents) {
		fmt.Printf("Number of components in contrib and dapr/dapr do not match")
		fmt.Printf("Contrib: %v\n", mappedContribComponents)
		fmt.Printf("Dapr: %v\n", mappedDaprComponents)
		return
	}

	missing, err := validateComponents(contribComponents, componentType)
	if err != nil {
		fmt.Printf("Component validation failed: %v\n", err)
		return
	}

	reportResults(missing, componentType)
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

func findComponentsInBothRepos(componentType string) ([]string, []string, error) {
	// Find all components in components-contrib
	contribCmd := exec.Command("grep", "-rl", "--include=*.go", "func New", componentType)
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

	contribComponents := parseContribComponents(string(contribOutput), componentType)
	registeredComponents := parseRegisteredComponents(string(registeredOutput))

	return contribComponents, registeredComponents, nil
}

func validateComponents(contribComponents []string, componentType string) ([]string, error) {
	missing := []string{}

	for _, contrib := range contribComponents {
		if err := validateComponent(contrib, componentType); err != nil {
			missing = append(missing, contrib)
		}
	}

	return missing, nil
}

func validateComponent(contrib, componentType string) error {
	if err := checkComponentRegistration(contrib, componentType); err != nil {
		return err
	}

	if err := checkBuildTag(contrib, componentType); err != nil {
		return err
	}

	if err := checkMetadataFile(contrib, componentType); err != nil {
		return err
	}

	return nil
}

func checkComponentRegistration(contrib, componentType string) error {
	// For registration files, use the normalized component name
	// EX: aws.bedrock -> conversation_bedrock.go
	compName := normalizeComponentName(contrib)
	compFileName := fmt.Sprintf("../dapr/cmd/daprd/components/%s_%s.go", componentType, compName)

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
					if registeredName == normalizedName {
						return nil
					}
				}
			}
		}
	}

	return fmt.Errorf("component not found in registration file")
}

func checkBuildTag(contrib, componentType string) error {
	compName := normalizeComponentName(contrib)
	compFileName := fmt.Sprintf("../dapr/cmd/daprd/components/%s_%s.go", componentType, compName)

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
// EX: hashicorp.consul -> consul
func normalizeComponentName(contrib string) string {
	if strings.Contains(contrib, ".") {
		parts := strings.Split(contrib, ".")
		if len(parts) == 2 {
			vendorPrefixes := []string{"hashicorp", "aws", "azure", "gcp", "alicloud", "oci", "cloudflare"}
			for _, prefix := range vendorPrefixes {
				if parts[0] == prefix {
					return parts[1]
				}
			}
			// Strip version suffixes
			if parts[1] == "v1" || parts[1] == "v2" {
				return parts[0]
			}
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

func reportResults(missing []string, componentType string) {
	if len(missing) > 0 {
		fmt.Printf("Missing registrations: %d\n", len(missing))
		for _, m := range missing {
			fmt.Printf("  - %s\n", m)
		}
	} else {
		fmt.Printf("All %s components are registered\n", componentType)
	}
}

func parseContribComponents(output, componentType string) []string {
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
			components = append(components, componentName)
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
