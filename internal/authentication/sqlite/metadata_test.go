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

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
)

func TestSqliteMetadata(t *testing.T) {
	stateMetadata := func(props map[string]string) state.Metadata {
		return state.Metadata{Base: metadata.Base{Properties: props}}
	}

	t.Run("default options", func(t *testing.T) {
		md := &SqliteAuthMetadata{}
		md.Reset()

		err := metadata.DecodeMetadata(stateMetadata(map[string]string{
			"connectionString": "file:data.db",
		}), &md)
		require.NoError(t, err)

		err = md.Validate()

		require.NoError(t, err)
		assert.Equal(t, "file:data.db", md.ConnectionString)
		assert.Equal(t, DefaultTimeout, md.Timeout)
		assert.Equal(t, DefaultBusyTimeout, md.BusyTimeout)
		assert.False(t, md.DisableWAL)
	})

	t.Run("empty connection string", func(t *testing.T) {
		md := &SqliteAuthMetadata{}
		md.Reset()

		err := metadata.DecodeMetadata(stateMetadata(map[string]string{}), &md)
		require.NoError(t, err)

		err = md.Validate()

		require.Error(t, err)
		assert.ErrorContains(t, err, "missing connection string")
	})

	t.Run("invalid timeout", func(t *testing.T) {
		md := &SqliteAuthMetadata{}
		md.Reset()

		err := metadata.DecodeMetadata(stateMetadata(map[string]string{
			"connectionString": "file:data.db",
			"timeout":          "500ms",
		}), &md)
		require.NoError(t, err)

		err = md.Validate()

		require.Error(t, err)
		assert.ErrorContains(t, err, "timeout")
	})

	t.Run("aliases", func(t *testing.T) {
		md := &SqliteAuthMetadata{}
		md.Reset()

		err := metadata.DecodeMetadata(stateMetadata(map[string]string{
			"url":              "file:data.db",
			"timeoutinseconds": "1200",
		}), &md)
		require.NoError(t, err)

		err = md.Validate()

		require.NoError(t, err)
		assert.Equal(t, "file:data.db", md.ConnectionString)
		assert.Equal(t, 20*time.Minute, md.Timeout)
	})
}
