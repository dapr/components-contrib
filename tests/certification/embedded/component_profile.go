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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func componentProfilePaths(paths []string, profile string) ([]string, error) {
	if profile == "" {
		return paths, nil
	}
	for i, char := range profile {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') ||
			(i > 0 && (char == '-' || char == '_')) {
			continue
		}
		return nil, errors.New("invalid DAPR_TEST_COMPONENT_PROFILE: use a single alphanumeric profile name with optional hyphens or underscores")
	}
	if len(paths) == 0 {
		return nil, errors.New("cannot select a component profile without resource paths")
	}

	selected := make([]string, len(paths))
	for i, path := range paths {
		parts := strings.Split(filepath.Clean(path), string(filepath.Separator))
		componentIndex := -1
		for j := len(parts) - 1; j >= 0; j-- {
			if parts[j] == "components" {
				componentIndex = j
				break
			}
		}
		if componentIndex == -1 {
			return nil, fmt.Errorf("cannot select component profile for resource path %q: no components directory", path)
		}
		if componentIndex+1 == len(parts) || parts[componentIndex+1] != profile {
			parts = append(parts[:componentIndex+1], append([]string{profile}, parts[componentIndex+1:]...)...)
		}
		selected[i] = strings.Join(parts, string(filepath.Separator))
		info, err := os.Stat(selected[i])
		if err != nil {
			return nil, fmt.Errorf("component profile directory %q: %w", selected[i], err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("component profile path %q is not a directory", selected[i])
		}
	}
	return selected, nil
}
