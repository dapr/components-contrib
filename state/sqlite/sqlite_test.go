/*
Copyright 2022 The Dapr Authors
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
	"bytes"
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	fakeConnectionString = "not a real connection"
)

func TestGetConnectionString(t *testing.T) {
	logDest := &bytes.Buffer{}
	log := logger.NewLogger("test")
	log.SetOutput(logDest)
	log.SetOutputLevel(logger.DebugLevel)

	db := &sqliteDBAccess{
		logger: log,
	}

	t.Run("append default options", func(t *testing.T) {
		logDest.Reset()
		db.metadata.reset()
		db.metadata.ConnectionString = "file:test.db"

		connString, err := db.metadata.GetConnectionString(log)
		require.NoError(t, err)

		values := url.Values{
			"_txlock": []string{"immediate"},
			"_pragma": []string{"busy_timeout(2000)", "journal_mode(WAL)"},
		}
		assert.Equal(t, "file:test.db?"+values.Encode(), connString)
	})

	t.Run("add file prefix if missing", func(t *testing.T) {
		t.Run("database on file", func(t *testing.T) {
			logDest.Reset()
			db.metadata.reset()
			db.metadata.ConnectionString = "test.db"

			connString, err := db.metadata.GetConnectionString(log)
			require.NoError(t, err)

			values := url.Values{
				"_txlock": []string{"immediate"},
				"_pragma": []string{"busy_timeout(2000)", "journal_mode(WAL)"},
			}
			assert.Equal(t, "file:test.db?"+values.Encode(), connString)

			logs := logDest.String()
			assert.Contains(t, logs, "prefix 'file:' added to the connection string")
		})

		t.Run("in-memory database also adds cache=shared", func(t *testing.T) {
			logDest.Reset()
			db.metadata.reset()
			db.metadata.ConnectionString = ":memory:"

			connString, err := db.metadata.GetConnectionString(log)
			require.NoError(t, err)

			values := url.Values{
				"_txlock": []string{"immediate"},
				"_pragma": []string{"busy_timeout(2000)", "journal_mode(MEMORY)"},
				"cache":   []string{"shared"},
			}
			assert.Equal(t, "file::memory:?"+values.Encode(), connString)

			logs := logDest.String()
			assert.Contains(t, logs, "prefix 'file:' added to the connection string")
		})
	})

	t.Run("warn if _txlock is not immediate", func(t *testing.T) {
		t.Run("value is immediate", func(t *testing.T) {
			logDest.Reset()
			db.metadata.reset()
			db.metadata.ConnectionString = "file:test.db?_txlock=immediate"

			connString, err := db.metadata.GetConnectionString(log)
			require.NoError(t, err)

			values := url.Values{
				"_txlock": []string{"immediate"},
				"_pragma": []string{"busy_timeout(2000)", "journal_mode(WAL)"},
			}
			assert.Equal(t, "file:test.db?"+values.Encode(), connString)

			logs := logDest.String()
			assert.NotContains(t, logs, "_txlock")
		})

		t.Run("value is not immediate", func(t *testing.T) {
			logDest.Reset()
			db.metadata.reset()
			db.metadata.ConnectionString = "file:test.db?_txlock=deferred"

			connString, err := db.metadata.GetConnectionString(log)
			require.NoError(t, err)

			values := url.Values{
				"_txlock": []string{"deferred"},
				"_pragma": []string{"busy_timeout(2000)", "journal_mode(WAL)"},
			}
			assert.Equal(t, "file:test.db?"+values.Encode(), connString)

			logs := logDest.String()
			assert.Contains(t, logs, "Database connection is being created with a _txlock different from the recommended value 'immediate'")
		})
	})

	t.Run("forbidden _pragma URI options", func(t *testing.T) {
		t.Run("busy_timeout", func(t *testing.T) {
			logDest.Reset()
			db.metadata.reset()
			db.metadata.ConnectionString = "file:test.db?_pragma=busy_timeout(50)"

			_, err := db.metadata.GetConnectionString(log)
			require.Error(t, err)
			assert.ErrorContains(t, err, "found forbidden option '_pragma=busy_timeout' in the connection string")
		})
		t.Run("journal_mode", func(t *testing.T) {
			logDest.Reset()
			db.metadata.reset()
			db.metadata.ConnectionString = "file:test.db?_pragma=journal_mode(WAL)"

			_, err := db.metadata.GetConnectionString(log)
			require.Error(t, err)
			assert.ErrorContains(t, err, "found forbidden option '_pragma=journal_mode' in the connection string")
		})
	})

	t.Run("set busyTimeout", func(t *testing.T) {
		logDest.Reset()
		db.metadata.reset()
		db.metadata.ConnectionString = "file:test.db"
		db.metadata.BusyTimeout = time.Second

		connString, err := db.metadata.GetConnectionString(log)
		require.NoError(t, err)

		values := url.Values{
			"_txlock": []string{"immediate"},
			"_pragma": []string{"busy_timeout(1000)", "journal_mode(WAL)"},
		}
		assert.Equal(t, "file:test.db?"+values.Encode(), connString)
	})

	t.Run("set journal mode", func(t *testing.T) {
		t.Run("default to use WAL", func(t *testing.T) {
			logDest.Reset()
			db.metadata.reset()
			db.metadata.ConnectionString = "file:test.db"
			db.metadata.DisableWAL = false

			connString, err := db.metadata.GetConnectionString(log)
			require.NoError(t, err)

			values := url.Values{
				"_txlock": []string{"immediate"},
				"_pragma": []string{"busy_timeout(2000)", "journal_mode(WAL)"},
			}
			assert.Equal(t, "file:test.db?"+values.Encode(), connString)
		})

		t.Run("disable WAL", func(t *testing.T) {
			logDest.Reset()
			db.metadata.reset()
			db.metadata.ConnectionString = "file:test.db"
			db.metadata.DisableWAL = true

			connString, err := db.metadata.GetConnectionString(log)
			require.NoError(t, err)

			values := url.Values{
				"_txlock": []string{"immediate"},
				"_pragma": []string{"busy_timeout(2000)", "journal_mode(DELETE)"},
			}
			assert.Equal(t, "file:test.db?"+values.Encode(), connString)
		})

		t.Run("default to use MEMORY for in-memory databases", func(t *testing.T) {
			logDest.Reset()
			db.metadata.reset()
			db.metadata.ConnectionString = "file::memory:"

			connString, err := db.metadata.GetConnectionString(log)
			require.NoError(t, err)

			values := url.Values{
				"_txlock": []string{"immediate"},
				"_pragma": []string{"busy_timeout(2000)", "journal_mode(MEMORY)"},
				"cache":   []string{"shared"},
			}
			assert.Equal(t, "file::memory:?"+values.Encode(), connString)
		})

		t.Run("default to use DELETE for read-only databases", func(t *testing.T) {
			logDest.Reset()
			db.metadata.reset()
			db.metadata.ConnectionString = "file:test.db?mode=ro"

			connString, err := db.metadata.GetConnectionString(log)
			require.NoError(t, err)

			values := url.Values{
				"_txlock": []string{"immediate"},
				"_pragma": []string{"busy_timeout(2000)", "journal_mode(DELETE)"},
				"mode":    []string{"ro"},
			}
			assert.Equal(t, "file:test.db?"+values.Encode(), connString)
		})

		t.Run("default to use DELETE for immutable databases", func(t *testing.T) {
			logDest.Reset()
			db.metadata.reset()
			db.metadata.ConnectionString = "file:test.db?immutable=1"

			connString, err := db.metadata.GetConnectionString(log)
			require.NoError(t, err)

			values := url.Values{
				"_txlock":   []string{"immediate"},
				"_pragma":   []string{"busy_timeout(2000)", "journal_mode(DELETE)"},
				"immutable": []string{"1"},
			}
			assert.Equal(t, "file:test.db?"+values.Encode(), connString)
		})
	})
}

// Proves that the Init method runs the init method.
func TestInitRunsDBAccessInit(t *testing.T) {
	t.Parallel()
	ods, fake := createSqliteWithFake(t)
	ods.Ping(context.Background())
	assert.True(t, fake.initExecuted)
}

func TestMultiWithNoRequestsReturnsNil(t *testing.T) {
	t.Parallel()
	var operations []state.TransactionalStateOperation
	ods := createSqlite(t)
	err := ods.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.NoError(t, err)
}

func TestValidSetRequest(t *testing.T) {
	t.Parallel()

	ods := createSqlite(t)
	err := ods.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{createSetRequest()},
	})
	assert.NoError(t, err)
}

func TestValidMultiDeleteRequest(t *testing.T) {
	t.Parallel()

	ods := createSqlite(t)
	err := ods.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{createDeleteRequest()},
	})
	assert.NoError(t, err)
}

// Proves that the Ping method runs the ping method.
func TestPingRunsDBAccessPing(t *testing.T) {
	t.Parallel()
	odb, fake := createSqliteWithFake(t)
	odb.Ping(context.Background())
	assert.True(t, fake.pingExecuted)
}

// Fake implementation of interface dbaccess.
type fakeDBaccess struct {
	logger       logger.Logger
	pingExecuted bool
	initExecuted bool
	setExecuted  bool
	getExecuted  bool
}

func (m *fakeDBaccess) Ping(ctx context.Context) error {
	m.pingExecuted = true
	return nil
}

func (m *fakeDBaccess) Init(ctx context.Context, metadata state.Metadata) error {
	m.initExecuted = true

	return nil
}

func (m *fakeDBaccess) Set(ctx context.Context, req *state.SetRequest) error {
	m.setExecuted = true

	return nil
}

func (m *fakeDBaccess) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	m.getExecuted = true

	return nil, nil
}

func (m *fakeDBaccess) BulkGet(parentCtx context.Context, req []state.GetRequest) ([]state.BulkGetResponse, error) {
	return nil, nil
}

func (m *fakeDBaccess) Delete(ctx context.Context, req *state.DeleteRequest) error {
	return nil
}

func (m *fakeDBaccess) ExecuteMulti(ctx context.Context, reqs []state.TransactionalStateOperation) error {
	return nil
}

func (m *fakeDBaccess) Close() error {
	return nil
}

func createSqlite(t *testing.T) *SQLiteStore {
	logger := logger.NewLogger("test")

	dba := &fakeDBaccess{
		logger: logger,
	}

	odb := newSQLiteStateStore(logger, dba)
	assert.NotNil(t, odb)

	metadata := &state.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"connectionString": fakeConnectionString,
			},
		},
	}

	err := odb.Init(context.Background(), *metadata)

	assert.NoError(t, err)
	assert.NotNil(t, odb.dbaccess)

	return odb
}

func createSetRequest() state.SetRequest {
	return state.SetRequest{
		Key:   randomKey(),
		Value: randomJSON(),
	}
}

func createDeleteRequest() state.DeleteRequest {
	return state.DeleteRequest{
		Key: randomKey(),
	}
}

func createSqliteWithFake(t *testing.T) (*SQLiteStore, *fakeDBaccess) {
	ods := createSqlite(t)
	fake := ods.dbaccess.(*fakeDBaccess)
	return ods, fake
}

func randomKey() string {
	return uuid.New().String()
}

type fakeItem struct {
	Color string
}

func randomJSON() *fakeItem {
	return &fakeItem{Color: randomKey()}
}
