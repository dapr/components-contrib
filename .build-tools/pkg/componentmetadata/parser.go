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

package componentmetadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
	"github.com/xeipuuv/gojsonschema"
	"sigs.k8s.io/yaml"
)

// Parser used to parse and validate component metadata YAML or JSON files.
type Parser struct {
	validator *gojsonschema.Schema
}

// NewParserWithSchema creates a new Parser object with the schema read from the file.
func NewParserWithSchemaFile(file string) (*Parser, error) {
	read, err := os.ReadFile("component-metadata-schema.json")
	if err != nil {
		return nil, fmt.Errorf("failed to read component-metadata-schema.json: %w", err)
	}

	return NewParserWithSchema(read)
}

// NewParserWithSchema creates a new Parser object with the schema passed as JSON-encoded data.
func NewParserWithSchema(schema []byte) (*Parser, error) {
	schemaLoader := gojsonschema.NewBytesLoader(schema)
	validator, err := gojsonschema.NewSchema(schemaLoader)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema validator: %w", err)
	}

	return &Parser{
		validator: validator,
	}, nil
}

// LoadForComponent loads the metadata.yaml / metadata.json for the component, parses it, and validates it.
func (p *Parser) LoadForComponent(componentPath string) (*metadataschema.ComponentMetadata, error) {
	var (
		read []byte
		err  error
	)

	// Try with YAML first
	read, err = os.ReadFile(filepath.Join(componentPath, "metadata.yaml"))
	if errors.Is(err, os.ErrNotExist) {
		// IT'S YAML NOT YML
		// (Insert GIF of "It's leviOsa, not levioSA!")
		read, err = os.ReadFile(filepath.Join(componentPath, "metadata.yml"))
		if errors.Is(err, os.ErrNotExist) {
			// JSON anyone?
			read, err = os.ReadFile(filepath.Join(componentPath, "metadata.json"))
		}
	}

	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		panic(err)
	}

	// Parse the YAML (and also JSON)
	componentMetadata, err := p.Parse(read)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse metadata file for component %s: %w", componentPath, err)
	}
	return componentMetadata, nil
}

// Parse a YAML (or JSON) file, already read, and returns the ComponentMetadata object.
// Automatically validates the metadata against the schema
func (p *Parser) Parse(read []byte) (*metadataschema.ComponentMetadata, error) {
	componentMetadata := &metadataschema.ComponentMetadata{}

	// Converts to JSON if it's YAML
	// Since JSON is a subset of YAML, this accepts JSON files too
	readJSON, err := yaml.YAMLToJSONStrict(read)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metadata file: %w", err)
	}
	err = json.Unmarshal(readJSON, componentMetadata)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	// Validate against the schema
	res, err := p.validator.Validate(gojsonschema.NewGoLoader(componentMetadata))
	if err != nil {
		return nil, fmt.Errorf("metadata failed validation: %w", err)
	}
	if !res.Valid() {
		return nil, fmt.Errorf("metadata is not valid: %v", res.Errors())
	}

	// Perform extra validation
	err = componentMetadata.IsValid()
	if err != nil {
		return nil, err
	}

	return componentMetadata, nil
}
