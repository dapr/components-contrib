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

package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadata(t *testing.T) {
	t.Run("missing connection string", func(t *testing.T) {
		m := psqlMetadata{}
		props := map[string]string{}

		err := m.InitWithMetadata(props)
		require.Error(t, err)
		require.ErrorContains(t, err, "connection string")
	})

	t.Run("has connection string", func(t *testing.T) {
		m := psqlMetadata{}
		props := map[string]string{
			"connectionString": "foo=bar",
		}

		err := m.InitWithMetadata(props)
		require.NoError(t, err)
	})

	t.Run("default timeout", func(t *testing.T) {
		m := psqlMetadata{}
		props := map[string]string{
			"connectionString": "foo=bar",
		}

		err := m.InitWithMetadata(props)
		require.NoError(t, err)
		assert.Equal(t, 20*time.Second, m.Timeout)
	})

	t.Run("invalid timeout", func(t *testing.T) {
		m := psqlMetadata{}
		props := map[string]string{
			"connectionString": "foo=bar",
			"timeout":          "NaN",
		}

		err := m.InitWithMetadata(props)
		require.Error(t, err)
	})

	t.Run("positive timeout", func(t *testing.T) {
		m := psqlMetadata{}
		props := map[string]string{
			"connectionString": "foo=bar",
			"timeout":          "42",
		}

		err := m.InitWithMetadata(props)
		require.NoError(t, err)
		assert.Equal(t, 42*time.Second, m.Timeout)
	})

	t.Run("zero timeout", func(t *testing.T) {
		m := psqlMetadata{}
		props := map[string]string{
			"connectionString": "foo=bar",
			"timeout":          "0",
		}

		err := m.InitWithMetadata(props)
		require.Error(t, err)
	})
}
