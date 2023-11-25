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

package postgresql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
)

func TestMetadata(t *testing.T) {
	t.Run("missing connection string", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{}

		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, false)
		require.Error(t, err)
		require.ErrorContains(t, err, "connection string")
	})

	t.Run("has connection string", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
		}

		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, false)
		require.NoError(t, err)
	})

	t.Run("default table prefix", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
		}

		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, false)
		require.NoError(t, err)
		assert.Equal(t, m.TableName(pgTableState), "state")
	})

	t.Run("custom table prefix", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
			"tablePrefix":      "my_",
		}

		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, false)
		require.NoError(t, err)
		assert.Equal(t, m.TableName(pgTableState), "my_state")
	})

	t.Run("default timeout", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
		}

		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, false)
		require.NoError(t, err)
		assert.Equal(t, 20*time.Second, m.Timeout)
	})

	t.Run("invalid timeout", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
			"timeout":          "NaN",
		}

		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, false)
		require.Error(t, err)
	})

	t.Run("positive timeout", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
			"timeout":          "42",
		}

		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, false)
		require.NoError(t, err)
		assert.Equal(t, 42*time.Second, m.Timeout)
	})

	t.Run("zero timeout", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
			"timeout":          "0",
		}

		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, false)
		require.Error(t, err)
	})

	t.Run("default cleanupInterval", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
		}

		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, false)
		require.NoError(t, err)
		_ = assert.NotNil(t, m.CleanupInterval) &&
			assert.Equal(t, time.Hour, *m.CleanupInterval)
	})

	t.Run("invalid cleanupInterval", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
			"cleanupInterval":  "NaN",
		}

		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, false)
		require.Error(t, err)
	})

	t.Run("positive cleanupInterval", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
			"cleanupInterval":  "42",
		}

		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, false)
		require.NoError(t, err)
		_ = assert.NotNil(t, m.CleanupInterval) &&
			assert.Equal(t, 42*time.Second, *m.CleanupInterval)
	})

	t.Run("zero cleanupInterval", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
			"cleanupInterval":  "0",
		}

		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, false)
		require.NoError(t, err)
		assert.Nil(t, m.CleanupInterval)
	})
}
