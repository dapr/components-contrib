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

package embedded

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComponentProfilePaths(t *testing.T) {
	t.Run("unset preserves original paths", func(t *testing.T) {
		paths := []string{"./components/basic", "./other/resources", "/not/required/to/exist"}
		selected, err := componentProfilePaths(paths, "")
		require.NoError(t, err)
		require.Equal(t, paths, selected)

		selected, err = componentProfilePaths(nil, "")
		require.NoError(t, err)
		require.Nil(t, selected)
	})

	t.Run("selects the same scenario from another profile", func(t *testing.T) {
		root := t.TempDir()
		components := filepath.Join(root, "components")
		basic := filepath.Join(components, "floci", "basic")
		partition := filepath.Join(components, "floci", "partition_key")
		require.NoError(t, os.MkdirAll(basic, 0o700))
		require.NoError(t, os.MkdirAll(partition, 0o700))

		paths := []string{components, filepath.Join(components, "basic"), filepath.Join(components, "partition_key"), basic}
		original := append([]string(nil), paths...)
		selected, err := componentProfilePaths(paths, "floci")
		require.NoError(t, err)
		require.Equal(t, []string{filepath.Join(components, "floci"), basic, partition, basic}, selected)
		require.Equal(t, original, paths)
	})

	t.Run("relative paths remain relative", func(t *testing.T) {
		t.Chdir(t.TempDir())
		require.NoError(t, os.MkdirAll(filepath.Join("components", "floci", "basic"), 0o700))
		selected, err := componentProfilePaths([]string{"./components/basic"}, "floci")
		require.NoError(t, err)
		require.Equal(t, []string{filepath.Join("components", "floci", "basic")}, selected)
	})

	t.Run("invalid profile names are rejected", func(t *testing.T) {
		for _, profile := range []string{".", "..", "../floci", "floci/basic", `floci\basic`, "floci profile", "-floci", "_floci", "floci\n", "floci.2"} {
			t.Run(profile, func(t *testing.T) {
				_, err := componentProfilePaths([]string{"./components/basic"}, profile)
				require.ErrorContains(t, err, "invalid DAPR_TEST_COMPONENT_PROFILE")
			})
		}
	})

	t.Run("missing profile never falls back to live components", func(t *testing.T) {
		root := t.TempDir()
		base := filepath.Join(root, "components", "basic")
		require.NoError(t, os.MkdirAll(base, 0o700))
		_, err := componentProfilePaths([]string{base}, "floci")
		require.ErrorContains(t, err, "component profile directory")
	})

	t.Run("profile must be a directory", func(t *testing.T) {
		components := filepath.Join(t.TempDir(), "components")
		require.NoError(t, os.MkdirAll(components, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(components, "floci"), []byte("not a directory"), 0o600))
		_, err := componentProfilePaths([]string{components}, "floci")
		require.ErrorContains(t, err, "not a directory")
	})

	t.Run("unrelated resource paths are rejected when selecting a profile", func(t *testing.T) {
		_, err := componentProfilePaths([]string{"./resources/basic"}, "floci")
		require.ErrorContains(t, err, "no components directory")
	})

	t.Run("active profile requires resource paths", func(t *testing.T) {
		_, err := componentProfilePaths(nil, "floci")
		require.ErrorContains(t, err, "without resource paths")
	})
}

func TestComponentProfileInvalidRuntimeConfiguration(t *testing.T) {
	t.Setenv("DAPR_TEST_COMPONENT_PROFILE", "../invalid")
	rt, config, err := NewRuntime(context.Background(), "invalid-profile")
	require.ErrorContains(t, err, "invalid DAPR_TEST_COMPONENT_PROFILE")
	require.Nil(t, rt)
	require.Nil(t, config)
}
