package main

import (
	"log"
	"os"
	"strings"

	"gopkg.in/yaml.v2"
)

type Data struct {
	Metadata               []Metadata `yaml:"metadata"`
	AuthenticationProfiles []struct {
		Metadata []Metadata `yaml:"metadata"`
	} `yaml:"authenticationProfiles"`
}

type Metadata struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
}

func getYamlMetadata(basePath string, pkg string) *map[string]string {
	metadatayamlpath := basePath + "/" + pkg + "/metadata.yaml"
	data, err := os.ReadFile(metadatayamlpath)
	if err != nil {
		return nil
	}

	var d Data
	err = yaml.Unmarshal(data, &d)
	if err != nil {
		log.Fatal(err)
	}

	names := make(map[string]string)
	for _, m := range d.Metadata {
		names[strings.ToLower(m.Name)] = "string"
		if m.Type != "" {
			names[strings.ToLower(m.Name)] = m.Type
		}
	}
	for _, ap := range d.AuthenticationProfiles {
		for _, m := range ap.Metadata {
			names[strings.ToLower(m.Name)] = "string"
			if m.Type != "" {
				names[strings.ToLower(m.Name)] = m.Type
			}
		}
	}
	return &names
}

func checkMissingMetadata(yamlMetadataP *map[string]string, componentMetadata map[string]string) map[string]string {
	missingMetadata := make(map[string]string)
	// if there is no yaml metadata, then we are not missing anything yet
	if yamlMetadataP != nil {
		yamlMetadata := *yamlMetadataP
		for key := range componentMetadata {
			lowerKey := strings.ToLower(key)
			if _, ok := yamlMetadata[lowerKey]; !ok {
				missingMetadata[lowerKey] = componentMetadata[key]
			}
			// todo - check if the metadata is the same data type
		}
	}
	return missingMetadata
}
