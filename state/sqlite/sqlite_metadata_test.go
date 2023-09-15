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

package sqlite

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authSqlite "github.com/dapr/components-contrib/internal/authentication/sqlite"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
)

func TestSqliteMetadata(t *testing.T) {
	stateMetadata := func(props map[string]string) state.Metadata {
		return state.Metadata{Base: metadata.Base{Properties: props}}
	}

	t.Run("default options", func(t *testing.T) {
		md := &sqliteMetadataStruct{}
		err := md.InitWithMetadata(stateMetadata(map[string]string{
			"connectionString": "file:data.db",
		}))

		require.NoError(t, err)
		assert.Equal(t, "file:data.db", md.ConnectionString)
		assert.Equal(t, defaultTableName, md.TableName)
		assert.Equal(t, defaultMetadataTableName, md.MetadataTableName)
		assert.Equal(t, authSqlite.DefaultTimeout, md.Timeout)
		assert.Equal(t, defaultCleanupInternal, md.CleanupInterval)
		assert.Equal(t, authSqlite.DefaultBusyTimeout, md.BusyTimeout)
		assert.False(t, md.DisableWAL)
	})

	t.Run("empty connection string", func(t *testing.T) {
		md := &sqliteMetadataStruct{}
		err := md.InitWithMetadata(stateMetadata(map[string]string{}))

		require.Error(t, err)
		assert.ErrorContains(t, err, "missing connection string")
	})

	t.Run("invalid state table name", func(t *testing.T) {
		md := &sqliteMetadataStruct{}
		err := md.InitWithMetadata(stateMetadata(map[string]string{
			"connectionstring": "file:data.db",
			"tablename":        "not.valid",
		}))

		require.Error(t, err)
		assert.ErrorContains(t, err, "invalid identifier")
	})

	t.Run("invalid metadata table name", func(t *testing.T) {
		md := &sqliteMetadataStruct{}
		err := md.InitWithMetadata(stateMetadata(map[string]string{
			"connectionstring":  "file:data.db",
			"metadatatablename": "not.valid",
		}))

		require.Error(t, err)
		assert.ErrorContains(t, err, "invalid identifier")
	})

	t.Run("invalid timeout", func(t *testing.T) {
		md := &sqliteMetadataStruct{}
		err := md.InitWithMetadata(stateMetadata(map[string]string{
			"connectionString": "file:data.db",
			"timeout":          "500ms",
		}))

		require.Error(t, err)
		assert.ErrorContains(t, err, "timeout")
	})

	t.Run("aliases", func(t *testing.T) {
		md := &sqliteMetadataStruct{}
		err := md.InitWithMetadata(stateMetadata(map[string]string{
			"url":                      "file:data.db",
			"timeoutinseconds":         "1200",
			"cleanupintervalinseconds": "22",
		}))

		require.NoError(t, err)
		assert.Equal(t, "file:data.db", md.ConnectionString)
		assert.Equal(t, 20*time.Minute, md.Timeout)
		assert.Equal(t, 22*time.Second, md.CleanupInterval)
	})
}
