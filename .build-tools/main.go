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
	"encoding/json"

	"github.com/dapr/components-contrib/build-tools/cmd"
)

//go:embed component-folders.json
var componentFoldersJSON []byte

func init() {
	parsed := struct {
		ComponentFolders []string `json:"componentFolders"`
		ExcludeFolders   []string `json:"excludeFolders"`
	}{}
	err := json.Unmarshal(componentFoldersJSON, &parsed)
	if err != nil {
		panic(err)
	}

	cmd.ComponentFolders = parsed.ComponentFolders
	cmd.ExcludeFolders = parsed.ExcludeFolders
}

func main() {
	cmd.Execute()
}
