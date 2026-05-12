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

package vector

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataPropertiesNilSafeAccess(t *testing.T) {
	t.Parallel()

	meta := Metadata{}
	assert.Nil(t, meta.Properties)
	assert.Empty(t, meta.Properties["missing"])

	value, ok := meta.GetProperty("missing")
	require.False(t, ok)
	assert.Empty(t, value)
}
