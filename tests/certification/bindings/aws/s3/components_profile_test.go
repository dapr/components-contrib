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

package awss3binding_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestS3FlociComponentProfiles(t *testing.T) {
	for _, variant := range []string{"basic", "forcePathStyleTrue", "forcePathStyleFalse", "decodeBase64", "encodeBase64"} {
		t.Run(variant, func(t *testing.T) {
			original := s3ReadComponentDocument(t, filepath.Join("components", variant, "s3bindings.yaml"))
			profile := s3ReadComponentDocument(t, filepath.Join("components", "floci", variant, "s3bindings.yaml"))
			spec, ok := profile["spec"].(map[string]any)
			require.True(t, ok)
			items, ok := spec["metadata"].([]any)
			require.True(t, ok)
			var originalItems []any
			endpoints := 0
			for _, item := range items {
				property, ok := item.(map[string]any)
				require.True(t, ok)
				if property["name"] == "endpoint" {
					endpoints++
					require.Equal(t, map[string]any{
						"name": "endpoint",
						"secretKeyRef": map[string]any{
							"name": "AWS_ENDPOINT_URL",
							"key":  "AWS_ENDPOINT_URL",
						},
					}, property)
				} else {
					originalItems = append(originalItems, item)
				}
			}
			require.Equal(t, 1, endpoints)
			spec["metadata"] = originalItems
			require.Equal(t, original, profile, "the endpoint must be the only binding profile difference")
			require.Equal(t,
				s3ReadComponentDocument(t, filepath.Join("components", variant, "localsecrets.yaml")),
				s3ReadComponentDocument(t, filepath.Join("components", "floci", variant, "localsecrets.yaml")),
				"preserve the existing environment secret store",
			)
		})
	}
}

func s3ReadComponentDocument(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var document map[string]any
	require.NoError(t, yaml.UnmarshalStrict(data, &document))
	return document
}
