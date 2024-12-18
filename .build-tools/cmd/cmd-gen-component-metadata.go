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
	"os"

	"github.com/spf13/cobra"
)

func init() {
	const (
		compType        = "type"
		compBuiltinAuth = "builtinAuth"
		compStatus      = "status"
		compVersion     = "version"
		compDirection   = "direction"
		compOrigin      = "origin"
		compTitle       = "title"
	)
	generateComponentMetadataNewCmd.Flags().String(compType, "", "The component type (e.g., 'binding')")
	generateComponentMetadataNewCmd.Flags().StringSlice(compBuiltinAuth, []string{}, "The authentication profile (e.g., 'aws')")
	generateComponentMetadataNewCmd.Flags().String(compStatus, "", "The status of the component (e.g., 'stable')")
	generateComponentMetadataNewCmd.Flags().String(compVersion, "", "The version of the component (e.g., 'v1')")
	generateComponentMetadataNewCmd.Flags().String(compDirection, "", "The direction of the component (e.g., 'output')")
	generateComponentMetadataNewCmd.Flags().String(compOrigin, "", "The origin of the component (e.g., 'Dapr')")
	generateComponentMetadataNewCmd.Flags().String(compTitle, "", "The title of the component (e.g., 'My Component')")

	generateComponentMetadataNewCmd.MarkFlagRequired(compType)
	// Note: Built in auth profiles are not required
	generateComponentMetadataNewCmd.MarkFlagRequired(compStatus)
	generateComponentMetadataNewCmd.MarkFlagRequired(compVersion)
	// Note: Direction is not required as that is binding specific
	generateComponentMetadataNewCmd.MarkFlagRequired(compOrigin)
	generateComponentMetadataNewCmd.MarkFlagRequired(compTitle)

	rootCmd.AddCommand(generateComponentMetadataNewCmd)
}

// generateComponentMetadataCmd represents the command to generate the yaml manifest for each component
var generateComponentMetadataNewCmd = &cobra.Command{
	Use:   "generate-component-metadata",
	Short: "Generates the component metadata yaml file per component",
	Long:  `Generates the component metadata yaml file per component so we don't have to rely on community to manually create it.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Running generate-component-metadata command\n")

		err := cwdToRepoRoot()
		if err != nil {
			fmt.Printf("Failed to navigate to repo root: %v\n", err)
			return
		}

		flags := getCmdFlags(cmd)

		// TODO: flag validation for allowed values

		fmt.Printf("Origin: %v\n", flags.origin)

		metadataFile := flags.origin + "/metadata.go"
		componentPkg := os.Getenv("GOPACKAGE")
		checkFileExists(metadataFile)

		metadata, _, err := generateMetadataFromStructs(metadataFile, componentPkg+"Metadata", componentPkg)
		if err != nil {
			fmt.Printf("Error generating metadata from structs: %v\n", err)
			return
		}

		componentName := getComponentName(flags.origin, componentPkg)
		bindingSpec, err := generateComponentOperations(metadataFile, "Binding")
		if err != nil {
			fmt.Printf("Component must implement the component.MetadataBuilder interface to auto-generate its manifest: %v\n", err)
			return
		}

		component, err := assembleComponentMetadata(flags, componentName, metadata, *bindingSpec)
		if err != nil {
			fmt.Printf("Failed to assemble component metadata: %v\n", err)
			return
		}
		filePath := flags.origin + "/samhereoutput.yaml"
		err = writeToFile(filePath, component)
		if err != nil {
			fmt.Printf("Error writing to file: %v\n", err)
		} else {
			fmt.Printf("Metadata written to %s\n", filePath)
		}
	},
}
