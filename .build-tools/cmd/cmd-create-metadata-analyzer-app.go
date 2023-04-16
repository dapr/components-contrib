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
	"github.com/spf13/cobra"

	"github.com/dapr/components-contrib/build-tools/pkg/metadataanalyzer"
)

// generateMetadataAnalyzerAppCmd generates the go program file to analyze component metadata
var generateMetadataAnalyzerAppCmd = &cobra.Command{
	Use:   "generate-metadata-analyzer-app",
	Short: "Generates the component metadata analyzer app",
	Long:  `Generates the component metadata analyzer app as a go program file, and outputs it to a standard file location.`,
	Run: func(cmd *cobra.Command, args []string) {
		// Navigate to the root of the repo
		err := cwdToRepoRoot()
		if err != nil {
			panic(err)
		}

		outputfile, _ := cmd.Flags().GetString("outputfile")

		if outputfile == "" {
			panic("flag outputfile is required")
		}

		metadataanalyzer.GenerateMetadataAnalyzer("./", ComponentFolders, outputfile)
	},
}

func init() {
	generateMetadataAnalyzerAppCmd.PersistentFlags().String("outputfile", "", "The output file for the generated go program.")
	rootCmd.AddCommand(generateMetadataAnalyzerAppCmd)
}
