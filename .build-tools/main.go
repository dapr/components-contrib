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

package main

import (
	_ "embed"

	"gopkg.in/yaml.v3"

	"github.com/dapr/components-contrib/build-tools/cmd"
	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
)

var (
	//go:embed component-folders.yaml
	componentFoldersYAML []byte
	//go:embed builtin-authentication-profiles.yaml
	builtinAuthenticationProfilesYAML []byte
)

func main() {
	// Parse component-folders.json
	parsedComponentFolders := struct {
		ComponentFolders []string `json:"componentFolders" yaml:"componentFolders"`
		ExcludeFolders   []string `json:"excludeFolders" yaml:"excludeFolders"`
	}{}
	err := yaml.Unmarshal(componentFoldersYAML, &parsedComponentFolders)
	if err != nil {
		panic(err)
	}

	cmd.ComponentFolders = parsedComponentFolders.ComponentFolders
	cmd.ExcludeFolders = parsedComponentFolders.ExcludeFolders

	// Parse builtin-authentication-profiles.yaml
	err = yaml.Unmarshal(builtinAuthenticationProfilesYAML, &metadataschema.BuiltinAuthenticationProfiles)
	if err != nil {
		panic(err)
	}

	cmd.Execute()
}
