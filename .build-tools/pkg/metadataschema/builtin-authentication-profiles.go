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

		// deep copy the metadata slice to avoid side effects when manually updating some req -> non-req fields to deprecate some fields for kafka/postgres
		// TODO: rm all of this manipulation in Dapr 1.17!!
		originalMetadata := profile.Metadata
		metadataCopy := make([]Metadata, len(originalMetadata))
		copy(metadataCopy, originalMetadata)

		if componentTitle == "Apache Kafka" || strings.ToLower(componentTitle) == "postgresql" {
			removeRequiredOnSomeAWSFields(&metadataCopy)
		}

		merged := mergedMetadata(bi.Metadata, metadataCopy...)

		// Note: We must apply the removal of deprecated fields after the merge!!

		// Here, we remove some deprecated fields as we support the transition to a new auth profile
		if profile.Title == "AWS: Assume IAM Role" && componentTitle == "Apache Kafka" || profile.Title == "AWS: Assume IAM Role" && strings.ToLower(componentTitle) == "postgresql" {
			merged = removeSomeDeprecatedFieldsOnUnrelatedAuthProfiles(merged)
		}

		// Here, there are no metadata fields that need deprecating
		if profile.Title == "AWS: Credentials from Environment Variables" && componentTitle == "Apache Kafka" || profile.Title == "AWS: Credentials from Environment Variables" && strings.ToLower(componentTitle) == "postgresql" {
			merged = removeAllDeprecatedFieldsOnUnrelatedAuthProfiles(merged)
		}

		// Here, this is a new auth profile, so rm all deprecating fields as unrelated.
		if profile.Title == "AWS: IAM Roles Anywhere" && componentTitle == "Apache Kafka" || profile.Title == "AWS: IAM Roles Anywhere" && strings.ToLower(componentTitle) == "postgresql" {
			merged = removeAllDeprecatedFieldsOnUnrelatedAuthProfiles(merged)
		}

		res[i].Metadata = merged
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

// removeRequiredOnSomeAWSFields needs to be removed in Dapr 1.17 as duplicated AWS IAM fields get removed,
// and we standardize on these fields.
// Currently, there are: awsAccessKey, accessKey and awsSecretKey, secretKey, and awsRegion and region fields.
// We normally have accessKey, secretKey, and region fields marked required as it is part of the builtin AWS auth profile fields.
// However, as we rm the aws prefixed ones, we need to then mark the normally required ones as not required only for postgres and kafka.
// This way we do not break existing users, and transition them to the standardized fields.
func removeRequiredOnSomeAWSFields(metadata *[]Metadata) {
	if metadata == nil {
		return
	}

	for i := range *metadata {
		field := &(*metadata)[i]

		if field == nil {
			continue
		}

		if field.Name == "accessKey" || field.Name == "secretKey" || field.Name == "region" {
			field.Required = false
		}
	}
}

func removeAllDeprecatedFieldsOnUnrelatedAuthProfiles(metadata []Metadata) []Metadata {
	filteredMetadata := []Metadata{}
	for _, field := range metadata {
		if strings.HasPrefix(field.Name, "aws") {
			continue
		} else {
			filteredMetadata = append(filteredMetadata, field)
		}
	}

	return filteredMetadata
}

func removeSomeDeprecatedFieldsOnUnrelatedAuthProfiles(metadata []Metadata) []Metadata {
	filteredMetadata := []Metadata{}

	for _, field := range metadata {
		// region is required in Assume Role auth profile, so this is needed for now.
		if field.Name == "region" {
			field.Required = true
		}
		if field.Name == "awsAccessKey" || field.Name == "awsSecretKey" || field.Name == "awsSessionToken" || field.Name == "awsRegion" {
			continue
		} else {
			filteredMetadata = append(filteredMetadata, field)
		}
	}

	return filteredMetadata
}
