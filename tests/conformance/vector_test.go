//go:build conftests
// +build conftests

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

package conformance

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/tests/conformance/utils"
	confvector "github.com/dapr/components-contrib/tests/conformance/vector"
	"github.com/dapr/components-contrib/vector"
	vectormeilisearch "github.com/dapr/components-contrib/vector/meilisearch"
)

func TestVectorConformance(t *testing.T) {
	const configPath = "../config/vector/"

	_ = utils.LoadEnvVars(configPath + ".env")
	tc, err := NewTestConfiguration(filepath.Join(configPath, "tests.yml"))
	require.NoError(t, err)
	require.NotNil(t, tc)

	tc.TestFn = func(comp *TestComponent) func(t *testing.T) {
		return func(t *testing.T) {
			if vectorShouldSkipComponent(t, comp.Component) {
				return
			}
			ParseConfigurationMap(t, comp.Config)
			props, err := loadComponentsAndProperties(t, filepath.Join(configPath, convertComponentNameToPath(comp.Component, comp.Profile)))
			require.NoErrorf(t, err, "error running vector conformance test for component %s", comp.Component)
			vectorComponent := loadVectorComponent(comp.Component)
			require.NotNil(t, vectorComponent, "error running vector conformance test for component %s", comp.Component)
			confvector.ConformanceTests(t, props, vectorComponent, comp.Component)
		}
	}

	tc.Run(t)
}

func vectorShouldSkipComponent(t *testing.T, componentName string) bool {
	switch componentName {
	case "meilisearch":
		if os.Getenv("MEILISEARCH_HOST") == "" {
			t.Skip("Skipping Meilisearch vector conformance test: MEILISEARCH_HOST environment variable not set")
			return true
		}
	}
	return false
}

func loadVectorComponent(name string) vector.Vector {
	switch name {
	case "meilisearch":
		return vectormeilisearch.NewMeilisearch(testLogger)
	default:
		return nil
	}
}
