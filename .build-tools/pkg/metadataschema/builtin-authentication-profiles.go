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

		switch profile.Title {
		case "AWS: Access Key ID and Secret Access Key":
			// If component is PostgreSQL, handle deprecation of aws profile fields that we will remove in Dapr 1.17
			// Postgres has awsAccessKey and accessKey and awsSecretKey and secretKey.
			// Therefore, we mark the non aws prefixed ones as not required as we deprecate the aws prefixed ones.
			if strings.ToLower(componentTitle) == "postgresql" && bi.Name == "aws" {
				res[i].Metadata = removeRequiredOnSomeAWSFields(res[i].Metadata)
			}
			// If component is Kafka, handle deprecation of aws profile fields with aws prefix that we will remove in Dapr 1.17
			if strings.ToLower(componentTitle) == "Apache Kafka" {
				res[i].Metadata = removeRequiredOnSomeAWSFields(res[i].Metadata)
			}

		case "AWS: Credentials from Environment Variables", "AWS: IAM Roles Anywhere":
			// These two auth profiles do not use the fields that we are deprecated, so we can manually remove the unrelated fields
			res[i].Metadata = removeAllDeprecatedFieldsOnUnrelatedAuthProfiles(res[i].Metadata)
		case "AWS: Assume specific IAM Role":
			// This is needed bc to assume a specific IAM role, we must allow for the field of awsStsSessionName to deprecate to sessionName
			res[i].Metadata = removeSomeDeprecatedFieldsOnUnrelatedAuthProfiles(res[i].Metadata)
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

// removeRequiredOnSomeAWSFields needs to be removed in Dapr 1.17 as duplicated AWS IAM fields get removed,
// and we standardize on these fields.
// Currently, there are: awsAccessKey, accessKey and awsSecretKey, secretKey fields.
// We normally have accessKey and secretKey fields marked required as it is part of the builtin AWS auth profile fields.
// However, as we rm the aws prefixed ones, we need to then mark the normally required ones as not required only for postgres and kafka.
// This way we do not break existing users, and transition them to the standardized fields.
func removeRequiredOnSomeAWSFields(metadata []Metadata) []Metadata {
	duplicateFields := map[string]int{
		"accessKey": 0,
		"secretKey": 0,
	}

	filteredMetadata := []Metadata{}

	for _, field := range metadata {
		if field.Name == "accessKey" && duplicateFields["accessKey"] == 0 {
			field.Required = false
			filteredMetadata = append(filteredMetadata, field)
		} else if field.Name == "secretKey" && duplicateFields["secretKey"] == 0 {
			field.Required = false
			filteredMetadata = append(filteredMetadata, field)
		}
	}

	return filteredMetadata
}

func removeAllDeprecatedFieldsOnUnrelatedAuthProfiles(metadata []Metadata) []Metadata {
	filteredMetadata := []Metadata{}

	for _, field := range metadata {
		switch field.Name {
		case "awsAccessKey", "awsSecretKey", "awsSessionToken", "awsIamRoleArn", "awsStsSessionName":
			continue
		default:
			filteredMetadata = append(filteredMetadata, field)
		}

	}

	return filteredMetadata
}

func removeSomeDeprecatedFieldsOnUnrelatedAuthProfiles(metadata []Metadata) []Metadata {
	filteredMetadata := []Metadata{}

	for _, field := range metadata {
		switch field.Name {
		case "awsAccessKey", "awsSecretKey", "awsSessionToken":
			continue
		default:
			filteredMetadata = append(filteredMetadata, field)
		}

	}

	return filteredMetadata
}
