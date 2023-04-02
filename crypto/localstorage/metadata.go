/*
Copyright 2023 The Dapr Authors
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

package localstorage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	contribCrypto "github.com/dapr/components-contrib/crypto"
	"github.com/dapr/components-contrib/metadata"
)

type localStorageMetadata struct {
	// Path to a local folder where keys are stored.
	// Keys are loaded from PEM or JSON (each containing an individual JWK) files from this folder.
	Path string `json:"path" mapstructure:"path"`
}

func (m *localStorageMetadata) InitWithMetadata(meta contribCrypto.Metadata) error {
	m.reset()

	// Decode the metadata
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return err
	}

	// Validate the path and make sure it's a directory
	if m.Path == "" {
		return errors.New("metadata property 'path' is required")
	}
	m.Path = filepath.Clean(m.Path)
	info, err := os.Stat(m.Path)
	if err != nil {
		return fmt.Errorf("could not stat path '%s': %w", m.Path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("path '%s' is not a directory", m.Path)
	}

	return nil
}

// Reset the object
func (m *localStorageMetadata) reset() {
	m.Path = ""
}
