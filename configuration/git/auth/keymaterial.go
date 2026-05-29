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

package auth

import (
	"fmt"
	"os"
)

// loadKeyMaterial returns inline PEM bytes if non-empty, else reads them
// from the supplied path. Returns an error if neither is provided.
func loadKeyMaterial(kind, inline, path string) ([]byte, error) {
	if inline != "" {
		return []byte(inline), nil
	}
	if path != "" {
		return os.ReadFile(path)
	}
	return nil, fmt.Errorf("%s: inline key or path is required", kind)
}
