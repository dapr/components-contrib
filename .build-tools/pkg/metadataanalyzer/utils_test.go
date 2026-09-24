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

package metadataanalyzer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateSearchAndVectorAnalyzer(t *testing.T) {
	root := filepath.Join(t.TempDir(), "components-contrib")
	for kind, contract := range map[string]string{"search": "Search", "vector": "Vector"} {
		dir := filepath.Join(root, kind, "aws", "opensearch")
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		source := "package opensearch\nfunc NewOpenSearch() " + kind + "." + contract + " { return nil }\n"
		if err := os.WriteFile(filepath.Join(dir, "opensearch.go"), []byte(source), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	output := filepath.Join(t.TempDir(), "analyzer.go")
	GenerateMetadataAnalyzer(root, []string{"search", "vector"}, output)
	raw, err := os.ReadFile(output) //nolint:gosec // G304: test-owned file inside t.TempDir.
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"search", "vector"} {
		if !strings.Contains(string(raw), "github.com/dapr/components-contrib/"+kind+"/aws/opensearch") {
			t.Errorf("generated analyzer is missing the %s OpenSearch component", kind)
		}
	}
	if strings.Count(string(raw), ".NewOpenSearch(") != 2 {
		t.Error("generated analyzer must instantiate both OpenSearch components")
	}
}
