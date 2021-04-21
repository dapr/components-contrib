// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package conformance

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"

	"github.com/ghodss/yaml"
)

const (
	yamlSeparator = "\n---"
	componentKind = "Component"
)

// StandaloneComponents loads components in a standalone mode environment
type StandaloneComponents struct {
	componentsPath string
}

// NewStandaloneComponents returns a new standalone loader
func NewStandaloneComponents(componentPath string) *StandaloneComponents {
	return &StandaloneComponents{
		componentsPath: componentPath,
	}
}

// LoadComponents loads dapr components from a given directory
func (s *StandaloneComponents) LoadComponents() ([]Component, error) {
	dir := s.componentsPath
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	list := []Component{}

	for _, file := range files {
		if !file.IsDir() && s.isYaml(file.Name()) {
			path := filepath.Join(dir, file.Name())

			b, err := ioutil.ReadFile(path)
			if err != nil {
				log.Printf("error reading file %s : %s", path, err)

				continue
			}

			components, _ := s.decodeYaml(path, b)
			list = append(list, components...)
		}
	}

	return list, nil
}

// isYaml checks whether the file is yaml or not
func (s *StandaloneComponents) isYaml(fileName string) bool {
	extension := strings.ToLower(filepath.Ext(fileName))
	if extension == ".yaml" || extension == ".yml" {
		return true
	}

	return false
}

// decodeYaml decodes the yaml document
func (s *StandaloneComponents) decodeYaml(filename string, b []byte) ([]Component, []error) {
	list := []Component{}
	errors := []error{}
	scanner := bufio.NewScanner(bytes.NewReader(b))
	scanner.Split(s.splitYamlDoc)

	for {
		var comp Component
		comp.Spec = ComponentSpec{}
		err := s.decode(scanner, &comp)
		if err == io.EOF {
			break
		}

		if comp.Kind != componentKind {
			continue
		}

		list = append(list, comp)
	}

	return list, errors
}

// decode reads the YAML resource in document
func (s *StandaloneComponents) decode(scanner *bufio.Scanner, c interface{}) error {
	if scanner.Scan() {
		return yaml.Unmarshal(scanner.Bytes(), &c)
	}

	err := scanner.Err()
	if err == nil {
		err = io.EOF
	}

	return err
}

// splitYamlDoc - splits the yaml docs
func (s *StandaloneComponents) splitYamlDoc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	sep := len([]byte(yamlSeparator))
	if i := bytes.Index(data, []byte(yamlSeparator)); i >= 0 {
		i += sep
		after := data[i:]

		if len(after) == 0 {
			if atEOF {
				return len(data), data[:len(data)-sep], nil
			}

			return 0, nil, nil
		}
		if j := bytes.IndexByte(after, '\n'); j >= 0 {
			return i + j + 1, data[0 : i-sep], nil
		}

		return 0, nil, nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}

	// Request more data.
	return 0, nil, nil
}
