//go:build unit

/*
Copyright 2026 The Dapr Authors
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

package dynamoDBStorage_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestDynamoDBComponentProfiles(t *testing.T) {
	for _, variant := range []string{"basictest", "partition_key"} {
		t.Run(variant, func(t *testing.T) {
			defaultPath := filepath.Join("components", variant)
			profilePath := filepath.Join("components", "floci", variant)
			defaultComponent := loadDynamoDBProfileComponent(t, filepath.Join(defaultPath, "dynamodb.yaml"))
			profileComponent := loadDynamoDBProfileComponent(t, filepath.Join(profilePath, "dynamodb.yaml"))

			profileSpec, ok := profileComponent["spec"].(map[string]any)
			require.True(t, ok)
			profileMetadata, ok := profileSpec["metadata"].([]any)
			require.True(t, ok)
			var endpoint map[string]any
			originalMetadata := make([]any, 0, len(profileMetadata))
			for _, value := range profileMetadata {
				item, ok := value.(map[string]any)
				require.True(t, ok)
				if item["name"] == "endpoint" {
					require.Nil(t, endpoint, "the profile must have exactly one endpoint")
					endpoint = item
				} else {
					originalMetadata = append(originalMetadata, item)
				}
			}
			require.Equal(t, map[string]any{
				"name": "endpoint",
				"secretKeyRef": map[string]any{
					"name": "FLOCI_ENDPOINT",
					"key":  "FLOCI_ENDPOINT",
				},
			}, endpoint)
			profileSpec["metadata"] = originalMetadata
			require.Equal(t, defaultComponent, profileComponent, "the endpoint must be the only component difference")

			require.Equal(t,
				loadDynamoDBProfileComponent(t, filepath.Join(defaultPath, "localsecrets.yaml")),
				loadDynamoDBProfileComponent(t, filepath.Join(profilePath, "localsecrets.yaml")),
				"the profile must retain the existing environment secret store")
		})
	}
}

func loadDynamoDBProfileComponent(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var component map[string]any
	require.NoError(t, yaml.UnmarshalStrict(data, &component))
	require.Equal(t, "dapr.io/v1alpha1", component["apiVersion"])
	require.Equal(t, "Component", component["kind"])
	return component
}
