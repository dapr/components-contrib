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

	"github.com/dapr/components-contrib/common/authentication/postgresql"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
)

func TestMetadata(t *testing.T) {
	t.Run("missing connection string", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{}

		opts := postgresql.InitWithMetadataOpts{}
		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, opts)
		require.Error(t, err)
		require.ErrorContains(t, err, "connection string")
	})

	t.Run("has connection string", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
		}

		opts := postgresql.InitWithMetadataOpts{}
		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, opts)
		require.NoError(t, err)
	})

	t.Run("default table name", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
		}

		opts := postgresql.InitWithMetadataOpts{}
		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, opts)
		require.NoError(t, err)
		assert.Equal(t, defaultTableName, m.TableName)
	})

	t.Run("custom table name", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
			"tableName":        "mytable",
		}

		opts := postgresql.InitWithMetadataOpts{}
		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, opts)
		require.NoError(t, err)
		assert.Equal(t, "mytable", m.TableName)
	})

	t.Run("default timeout", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
		}

		opts := postgresql.InitWithMetadataOpts{}
		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, opts)
		require.NoError(t, err)
		assert.Equal(t, defaultTimeout, m.Timeout)
	})

	t.Run("invalid timeout", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
			"timeout":          "NaN",
		}

		opts := postgresql.InitWithMetadataOpts{}
		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, opts)
		require.Error(t, err)
	})

	t.Run("positive timeout", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
			"timeout":          "42",
		}

		opts := postgresql.InitWithMetadataOpts{}
		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, opts)
		require.NoError(t, err)
		assert.Equal(t, 42*time.Second, m.Timeout)
	})

	t.Run("zero timeout", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
			"timeout":          "0",
		}

		opts := postgresql.InitWithMetadataOpts{}
		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, opts)
		require.Error(t, err)
	})

	t.Run("default cleanupInterval", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
		}

		opts := postgresql.InitWithMetadataOpts{}
		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, opts)
		require.NoError(t, err)
		require.NotNil(t, m.CleanupInterval)
		assert.Equal(t, defaultCleanupInternal, *m.CleanupInterval)
	})

	t.Run("invalid cleanupInterval", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
			"cleanupInterval":  "NaN",
		}

		opts := postgresql.InitWithMetadataOpts{}
		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, opts)
		require.Error(t, err)
	})

	t.Run("positive cleanupInterval", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
			"cleanupInterval":  "42",
		}

		opts := postgresql.InitWithMetadataOpts{}
		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, opts)
		require.NoError(t, err)
		require.NotNil(t, m.CleanupInterval)
		assert.Equal(t, 42*time.Second, *m.CleanupInterval)
	})

	t.Run("positive cleanupIntervalInSeconds", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString":         "foo",
			"cleanupIntervalInSeconds": "42",
		}

		opts := postgresql.InitWithMetadataOpts{}
		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, opts)
		require.NoError(t, err)
		require.NotNil(t, m.CleanupInterval)
		assert.Equal(t, 42*time.Second, *m.CleanupInterval)
	})

	t.Run("positive cleanupInterval as duration", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
			"cleanupInterval":  "42m",
		}

		opts := postgresql.InitWithMetadataOpts{}
		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, opts)
		require.NoError(t, err)
		require.NotNil(t, m.CleanupInterval)
		assert.Equal(t, 42*time.Minute, *m.CleanupInterval)
	})

	t.Run("positive cleanupIntervalInseconds as duration", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString":         "foo",
			"cleanupIntervalInseconds": "42m",
		}

		opts := postgresql.InitWithMetadataOpts{}
		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, opts)
		require.NoError(t, err)
		require.NotNil(t, m.CleanupInterval)
		assert.Equal(t, 42*time.Minute, *m.CleanupInterval)
	})

	t.Run("zero cleanupInterval", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
			"cleanupInterval":  "0",
		}

		opts := postgresql.InitWithMetadataOpts{}
		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, opts)
		require.NoError(t, err)
		assert.Nil(t, m.CleanupInterval)
	})

	t.Run("zero cleanupIntervalInSeconds", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString":         "foo",
			"cleanupIntervalInSeconds": "0",
		}

		opts := postgresql.InitWithMetadataOpts{}
		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, opts)
		require.NoError(t, err)
		assert.Nil(t, m.CleanupInterval)
	})

	t.Run("empty cleanupInterval", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString": "foo",
			"cleanupInterval":  "",
		}

		opts := postgresql.InitWithMetadataOpts{}
		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, opts)
		require.NoError(t, err)
		require.NotNil(t, m.CleanupInterval)
		assert.Equal(t, defaultCleanupInternal, *m.CleanupInterval)
	})

	t.Run("empty cleanupIntervalInSeconds", func(t *testing.T) {
		m := pgMetadata{}
		props := map[string]string{
			"connectionString":         "foo",
			"cleanupIntervalInSeconds": "",
		}

		opts := postgresql.InitWithMetadataOpts{}
		err := m.InitWithMetadata(state.Metadata{Base: metadata.Base{Properties: props}}, opts)
		require.NoError(t, err)
		require.NotNil(t, m.CleanupInterval)
		assert.Equal(t, defaultCleanupInternal, *m.CleanupInterval)
	})
}
