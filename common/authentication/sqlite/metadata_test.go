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
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

func TestValidate(t *testing.T) {
	t.Run("default options", func(t *testing.T) {
		md := initTestMetadata(t, map[string]string{
			"connectionString": "file:data.db",
		})

		err := md.Validate()
		require.NoError(t, err)

		assert.Equal(t, "file:data.db", md.ConnectionString)
		assert.Equal(t, DefaultTimeout, md.Timeout)
		assert.Equal(t, DefaultBusyTimeout, md.BusyTimeout)
		assert.False(t, md.DisableWAL)
	})

	t.Run("empty connection string", func(t *testing.T) {
		md := initTestMetadata(t, map[string]string{})

		err := md.Validate()
		require.Error(t, err)
		require.ErrorContains(t, err, "missing connection string")
	})

	t.Run("invalid timeout", func(t *testing.T) {
		md := initTestMetadata(t, map[string]string{
			"connectionString": "file:data.db",
			"timeout":          "500ms",
		})

		err := md.Validate()
		require.Error(t, err)
		require.ErrorContains(t, err, "timeout")
	})

	t.Run("aliases", func(t *testing.T) {
		md := initTestMetadata(t, map[string]string{
			"url":              "file:data.db",
			"timeoutinseconds": "1200",
		})

		err := md.Validate()
		require.NoError(t, err)

		assert.Equal(t, "file:data.db", md.ConnectionString)
		assert.Equal(t, 20*time.Minute, md.Timeout)
	})
}

func TestGetConnectionStringe(t *testing.T) {
	log := logger.NewLogger("test")
	log.SetOutput(io.Discard)

	t.Run("file name without prefix", func(t *testing.T) {
		md := initTestMetadata(t, map[string]string{
			"connectionString": "data.db",
		})

		err := md.Validate()
		require.NoError(t, err)

		connString, err := md.GetConnectionString(log, GetConnectionStringOpts{})
		require.NoError(t, err)

		u, err := url.Parse(connString)
		require.NoError(t, err)

		assert.True(t, strings.HasPrefix(connString, "file:data.db?"))
		assert.EqualValues(t, url.Values{
			"_pragma": {
				"busy_timeout(2000)",
				"journal_mode(WAL)",
			},
			"_txlock": {"immediate"},
		}, u.Query())
	})

	t.Run("file name with prefix", func(t *testing.T) {
		md := initTestMetadata(t, map[string]string{
			"connectionString": "file:data.db",
		})

		err := md.Validate()
		require.NoError(t, err)

		connString, err := md.GetConnectionString(log, GetConnectionStringOpts{})
		require.NoError(t, err)

		u, err := url.Parse(connString)
		require.NoError(t, err)

		assert.True(t, strings.HasPrefix(connString, "file:data.db?"))
		assert.EqualValues(t, url.Values{
			"_pragma": {
				"busy_timeout(2000)",
				"journal_mode(WAL)",
			},
			"_txlock": {"immediate"},
		}, u.Query())
	})

	t.Run("in-memory without prefix", func(t *testing.T) {
		md := initTestMetadata(t, map[string]string{
			"connectionString": ":memory:",
		})

		err := md.Validate()
		require.NoError(t, err)

		connString, err := md.GetConnectionString(log, GetConnectionStringOpts{})
		require.NoError(t, err)

		u, err := url.Parse(connString)
		require.NoError(t, err)

		assert.True(t, strings.HasPrefix(connString, "file::memory:?"))
		assert.EqualValues(t, url.Values{
			"_pragma": {
				"busy_timeout(2000)",
				"journal_mode(MEMORY)",
			},
			"_txlock": {"immediate"},
			"cache":   {"shared"},
		}, u.Query())
	})

	t.Run("in-memory with prefix", func(t *testing.T) {
		md := initTestMetadata(t, map[string]string{
			"connectionString": "file::memory:",
		})

		err := md.Validate()
		require.NoError(t, err)

		connString, err := md.GetConnectionString(log, GetConnectionStringOpts{})
		require.NoError(t, err)

		u, err := url.Parse(connString)
		require.NoError(t, err)

		assert.True(t, strings.HasPrefix(connString, "file::memory:?"))
		assert.EqualValues(t, url.Values{
			"_pragma": {
				"busy_timeout(2000)",
				"journal_mode(MEMORY)",
			},
			"_txlock": {"immediate"},
			"cache":   {"shared"},
		}, u.Query())
	})

	t.Run("enable foreign keys", func(t *testing.T) {
		md := initTestMetadata(t, map[string]string{
			"connectionString": "data.db",
		})

		err := md.Validate()
		require.NoError(t, err)

		connString, err := md.GetConnectionString(log, GetConnectionStringOpts{
			EnableForeignKeys: true,
		})
		require.NoError(t, err)

		u, err := url.Parse(connString)
		require.NoError(t, err)

		assert.True(t, strings.HasPrefix(connString, "file:data.db?"))
		assert.EqualValues(t, url.Values{
			"_pragma": {
				"busy_timeout(2000)",
				"journal_mode(WAL)",
				"foreign_keys(1)",
			},
			"_txlock": {"immediate"},
		}, u.Query())
	})

	t.Run("set busy timeout", func(t *testing.T) {
		md := initTestMetadata(t, map[string]string{
			"connectionString": "data.db",
			"busyTimeout":      "1m",
		})

		err := md.Validate()
		require.NoError(t, err)

		connString, err := md.GetConnectionString(log, GetConnectionStringOpts{})
		require.NoError(t, err)

		u, err := url.Parse(connString)
		require.NoError(t, err)

		assert.True(t, strings.HasPrefix(connString, "file:data.db?"))
		assert.EqualValues(t, url.Values{
			"_pragma": {
				"busy_timeout(60000)",
				"journal_mode(WAL)",
			},
			"_txlock": {"immediate"},
		}, u.Query())
	})

	t.Run("disable WAL", func(t *testing.T) {
		md := initTestMetadata(t, map[string]string{
			"connectionString": "data.db",
			"disableWAL":       "true",
		})

		err := md.Validate()
		require.NoError(t, err)

		connString, err := md.GetConnectionString(log, GetConnectionStringOpts{})
		require.NoError(t, err)

		u, err := url.Parse(connString)
		require.NoError(t, err)

		assert.True(t, strings.HasPrefix(connString, "file:data.db?"))
		assert.EqualValues(t, url.Values{
			"_pragma": {
				"busy_timeout(2000)",
				"journal_mode(DELETE)",
			},
			"_txlock": {"immediate"},
		}, u.Query())
	})

	t.Run("read-only", func(t *testing.T) {
		md := initTestMetadata(t, map[string]string{
			"connectionString": "file:data.db?mode=ro",
		})

		err := md.Validate()
		require.NoError(t, err)

		connString, err := md.GetConnectionString(log, GetConnectionStringOpts{})
		require.NoError(t, err)

		u, err := url.Parse(connString)
		require.NoError(t, err)

		assert.True(t, strings.HasPrefix(connString, "file:data.db?"))
		assert.EqualValues(t, url.Values{
			"_pragma": {
				"busy_timeout(2000)",
				"journal_mode(DELETE)",
			},
			"_txlock": {"immediate"},
			"mode":    {"ro"},
		}, u.Query())
	})

	t.Run("immutable", func(t *testing.T) {
		md := initTestMetadata(t, map[string]string{
			"connectionString": "file:data.db?immutable=1",
		})

		err := md.Validate()
		require.NoError(t, err)

		connString, err := md.GetConnectionString(log, GetConnectionStringOpts{})
		require.NoError(t, err)

		u, err := url.Parse(connString)
		require.NoError(t, err)

		assert.True(t, strings.HasPrefix(connString, "file:data.db?"))
		assert.EqualValues(t, url.Values{
			"_pragma": {
				"busy_timeout(2000)",
				"journal_mode(DELETE)",
			},
			"_txlock":   {"immediate"},
			"immutable": {"1"},
		}, u.Query())
	})

	t.Run("do not override txlock", func(t *testing.T) {
		md := initTestMetadata(t, map[string]string{
			"connectionString": "file:data.db?_txlock=deferred",
		})

		err := md.Validate()
		require.NoError(t, err)

		connString, err := md.GetConnectionString(log, GetConnectionStringOpts{})
		require.NoError(t, err)

		u, err := url.Parse(connString)
		require.NoError(t, err)

		assert.True(t, strings.HasPrefix(connString, "file:data.db?"))
		assert.EqualValues(t, url.Values{
			"_pragma": {
				"busy_timeout(2000)",
				"journal_mode(WAL)",
			},
			"_txlock": {"deferred"},
		}, u.Query())
	})

	t.Run("error if busy_timeout is set", func(t *testing.T) {
		md := initTestMetadata(t, map[string]string{
			"connectionString": "file:data.db?_pragma=busy_timeout(1000)",
		})

		err := md.Validate()
		require.NoError(t, err)

		_, err = md.GetConnectionString(log, GetConnectionStringOpts{})
		require.Error(t, err)
		require.ErrorContains(t, err, "_pragma=busy_timeout")
	})

	t.Run("error if journal_mode is set", func(t *testing.T) {
		md := initTestMetadata(t, map[string]string{
			"connectionString": "file:data.db?_pragma=journal_mode(DELETE)",
		})

		err := md.Validate()
		require.NoError(t, err)

		_, err = md.GetConnectionString(log, GetConnectionStringOpts{})
		require.Error(t, err)
		require.ErrorContains(t, err, "_pragma=journal_mode")
	})

	t.Run("error if foreign_keys is set", func(t *testing.T) {
		md := initTestMetadata(t, map[string]string{
			"connectionString": "file:data.db?_pragma=foreign_keys(1)",
		})

		err := md.Validate()
		require.NoError(t, err)

		_, err = md.GetConnectionString(log, GetConnectionStringOpts{})
		require.Error(t, err)
		require.ErrorContains(t, err, "_pragma=foreign_keys")
	})
}

func initTestMetadata(t *testing.T, props map[string]string) *SqliteAuthMetadata {
	t.Helper()

	md := &SqliteAuthMetadata{}
	md.Reset()

	err := kitmd.DecodeMetadata(props, &md)
	require.NoError(t, err)

	return md
}
