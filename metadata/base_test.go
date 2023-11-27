/*
Copyright 2023 The Dapr Authors
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

package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBaseGetProperty(t *testing.T) {
	properties := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	base := Base{
		Properties: properties,
	}

	t.Run("existing property", func(t *testing.T) {
		value, ok := base.GetProperty("key1")
		assert.True(t, ok)
		assert.Equal(t, "value1", value)
	})

	t.Run("non-existing property", func(t *testing.T) {
		_, ok := base.GetProperty("key4")
		assert.False(t, ok)
	})

	t.Run("case-insensitive property", func(t *testing.T) {
		value, ok := base.GetProperty("KEY2")
		assert.True(t, ok)
		assert.Equal(t, "value2", value)
	})

	t.Run("multiple properties", func(t *testing.T) {
		values := []string{"value1", "value2", "value3"}
		for i, key := range []string{"key1", "key2", "key3"} {
			value, ok := base.GetProperty(key)
			assert.True(t, ok)
			assert.Equal(t, values[i], value)
		}
	})

	t.Run("empty properties", func(t *testing.T) {
		emptyBase := Base{}
		_, ok := emptyBase.GetProperty("key1")
		assert.False(t, ok)
	})

	t.Run("empty names", func(t *testing.T) {
		_, ok := base.GetProperty()
		assert.False(t, ok)
	})

	t.Run("case-insensitive non-existing names", func(t *testing.T) {
		_, ok := base.GetProperty("KEY4", "key5", "KeY6")
		assert.False(t, ok)
	})

	t.Run("case-insensitive empty names", func(t *testing.T) {
		_, ok := base.GetProperty()
		assert.False(t, ok)
	})

	t.Run("case-insensitive empty properties", func(t *testing.T) {
		emptyBase := Base{}
		_, ok := emptyBase.GetProperty("KEY1", "KEY2", "KEY3")
		assert.False(t, ok)
	})

	t.Run("ordering", func(t *testing.T) {
		// Must always return "value1", as it matches "key1" first
		value, ok := base.GetProperty("key1", "key2")
		assert.True(t, ok)
		assert.Equal(t, "value1", value)

		value, ok = base.GetProperty("key2", "key1")
		assert.True(t, ok)
		assert.Equal(t, "value2", value)
	})
}
