/*
Copyright 2021 The Dapr Authors
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

package utils

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONStringify(t *testing.T) {
	t.Run("string values produce valid JSON and round-trip", func(t *testing.T) {
		values := []string{
			`plain`,
			`with "quotes"`,
			`C:\Users\dapr`,
			"line1\nline2",
			"tab\tseparated",
			`back\slash`,
			"ctrl\x00byte",
			"unicode-☺",
		}

		for _, v := range values {
			b, err := JSONStringify(v)
			require.NoError(t, err)
			require.Truef(t, json.Valid(b), "JSONStringify(%q) must be valid JSON, got %s", v, string(b))

			var got string
			require.NoErrorf(t, json.Unmarshal(b, &got), "result %s must unmarshal", string(b))
			assert.Equalf(t, v, got, "value %q must round-trip through JSONStringify", v)
		}
	})

	t.Run("does not HTML-escape", func(t *testing.T) {
		b, err := JSONStringify(`a<b>&c`)
		require.NoError(t, err)
		assert.JSONEq(t, `"a<b>&c"`, string(b))
		assert.Equal(t, `"a<b>&c"`, string(b))
	})

	t.Run("byte slices are returned as-is", func(t *testing.T) {
		b, err := JSONStringify([]byte(`{"already":"json"}`))
		require.NoError(t, err)
		assert.Equal(t, `{"already":"json"}`, string(b))
	})

	t.Run("numbers and bools", func(t *testing.T) {
		b, err := JSONStringify(42)
		require.NoError(t, err)
		assert.Equal(t, "42", string(b))

		b, err = JSONStringify(true)
		require.NoError(t, err)
		assert.Equal(t, "true", string(b))
	})
}
