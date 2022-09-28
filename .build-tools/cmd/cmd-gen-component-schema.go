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
	"os"

	"github.com/invopop/jsonschema"
	"github.com/spf13/cobra"

	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
)

// genComponentSchemaCmd represents the genComponentSchema command
var genComponentSchemaCmd = &cobra.Command{
	Use:   "gen-component-schema",
	Short: "Generates the schema for the metadata.yaml / metadata.json files",
	Long: `Generates the schema for the metadata.yaml / metadata.json files by parsing the model in pkg/schema.
The result is written to stdout.`,
	Run: func(cmd *cobra.Command, args []string) {
		// Generate the schema from the struct
		reflector := &jsonschema.Reflector{}
		reflector.ExpandedStruct = true
		err := reflector.AddGoComments("github.com/dapr/components-contrib/build-tools", "./pkg/metadataschema")
		if err != nil {
			panic(err)
		}
		res := reflector.Reflect(&metadataschema.ComponentMetadata{})
		res.Title = "ComponentMetadata"
		// Print resut to stdout
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(res)
	},
}

func init() {
	rootCmd.AddCommand(genComponentSchemaCmd)
}
