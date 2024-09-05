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

package metadataschema

import (
	"fmt"
	"strings"
)

// Built-in authentication profiles
var BuiltinAuthenticationProfiles map[string][]AuthenticationProfile

// ParseBuiltinAuthenticationProfile returns an AuthenticationProfile(s) from a given BuiltinAuthenticationProfile.
func ParseBuiltinAuthenticationProfile(bi BuiltinAuthenticationProfile, componentTitle string) ([]AuthenticationProfile, error) {
	profiles, ok := BuiltinAuthenticationProfiles[bi.Name]
	if !ok {
		return nil, fmt.Errorf("built-in authentication profile %s does not exist", bi.Name)
	}

	res := make([]AuthenticationProfile, len(profiles))
	for i, profile := range profiles {
		res[i] = profile

		res[i].Metadata = mergedMetadata(bi.Metadata, res[i].Metadata...)

		// If component is PostgreSQL, filter out duplicated aws profile fields
		if strings.ToLower(componentTitle) == "postgresql" && bi.Name == "aws" {
			res[i].Metadata = filterOutDuplicateFields(res[i].Metadata)
		}

	}
	return res, nil
}

func mergedMetadata(base []Metadata, add ...Metadata) []Metadata {
	if len(base) == 0 {
		return add
	}

	res := make([]Metadata, 0, len(base)+len(add))
	res = append(res, base...)
	res = append(res, add...)
	return res
}

// filterOutDuplicateFields removes specific duplicated fields from the metadata
func filterOutDuplicateFields(metadata []Metadata) []Metadata {
	duplicateFields := map[string]int{
		"awsRegion": 0,
		"accessKey": 0,
		"secretKey": 0,
	}

	filteredMetadata := []Metadata{}

	for _, field := range metadata {
		if _, exists := duplicateFields[field.Name]; !exists {
			filteredMetadata = append(filteredMetadata, field)
		} else {
			if field.Name == "awsRegion" && duplicateFields["awsRegion"] == 0 {
				filteredMetadata = append(filteredMetadata, field)
				duplicateFields["awsRegion"]++
			} else if field.Name != "awsRegion" {
				continue
			}
		}
	}

	return filteredMetadata
}
