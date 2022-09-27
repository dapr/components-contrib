/*
Copyright 2022 The Dapr Authors
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
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/dapr/components-contrib/build-tools/pkg/componentmetadata"
	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
)

// bundleComponentMetadataCmd represents the bundle-component-metadata command
var bundleComponentMetadataCmd = &cobra.Command{
	Use:   "bundle-component-metadata",
	Short: "Generates the component metadata bundle",
	Long:  `Generates the component metadata bundle as a JSON files, and outputs it to stdout.`,
	Run: func(cmd *cobra.Command, args []string) {
		// Navigate to the root of the repo
		err := cwdToRepoRoot()
		if err != nil {
			panic(err)
		}

		// Create a parser that also loads the JSON schema
		parser, err := componentmetadata.NewParserWithSchemaFile("component-metadata-schema.json")
		if err != nil {
			panic(err)
		}

		// Find all components
		list, err := componentmetadata.FindComponents(ComponentFolders, ExcludeFolders)
		if err != nil {
			panic(err)
		}

		// Load the metadata for all components
		bundle := metadataschema.Bundle{
			SchemaVersion: "v1",
			Date:          time.Now().Format("20060102150405"),
			Components:    make([]*metadataschema.ComponentMetadata, 0, len(list)),
		}
		for _, component := range list {
			componentMetadata, err := parser.LoadForComponent(component)
			if err != nil {
				panic(fmt.Errorf("Failed to load metadata for component %s: %w", component, err))
			}
			if componentMetadata == nil {
				fmt.Fprintln(os.Stderr, "Info: metadata file not found in component "+component)
				continue
			}
			bundle.Components = append(bundle.Components, componentMetadata)
		}

		// Write to stdout
		enc := json.NewEncoder(os.Stdout)
		enc.SetEscapeHTML(false)
		enc.SetIndent("", "  ")
		err = enc.Encode(bundle)
		if err != nil {
			panic(fmt.Errorf("Failed to encode bundle to JSON: %w", err))
		}
	},
}

func init() {
	rootCmd.AddCommand(bundleComponentMetadataCmd)
}
