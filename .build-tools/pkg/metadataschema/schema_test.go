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

package metadataschema

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/invopop/jsonschema"
)

func TestSearchAndVectorMetadata(t *testing.T) {
	reflector := jsonschema.Reflector{ExpandedStruct: true}
	raw, err := json.Marshal(reflector.Reflect(&ComponentMetadata{}))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties map[string]struct {
			Enum []string `json:"enum"`
		} `json:"properties"`
	}
	if err = json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"search", "vector"} {
		t.Run(kind, func(t *testing.T) {
			if !slices.Contains(schema.Properties["type"].Enum, kind) {
				t.Fatalf("generated schema does not accept component type %s", kind)
			}
			md := ComponentMetadata{
				Type: kind, Name: "aws.opensearch",
				URLs: []URL{{Title: "Reference", URL: "https://docs.dapr.io/reference/components-reference/supported-" + kind + "/aws-opensearch/"}},
			}
			if err := md.IsValid(); err != nil {
				t.Fatalf("valid %s metadata rejected: %v", kind, err)
			}
		})
	}
}
