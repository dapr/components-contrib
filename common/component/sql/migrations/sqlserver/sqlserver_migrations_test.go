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

package sqlservermigrations

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	// Blank import for the SQL Server driver
	_ "github.com/microsoft/go-mssqldb"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonsql "github.com/dapr/components-contrib/common/component/sql"
	commonsqlserver "github.com/dapr/components-contrib/common/component/sqlserver"
	"github.com/dapr/kit/logger"
)

// connectionStringEnvKey defines the env key containing the integration test connection string
// To use Docker: `server=localhost;user id=sa;password=Pass@Word1;port=1433;database=dapr_test;`
// To use Azure SQL: `server=<your-db-server-name>.database.windows.net;user id=<your-db-user>;port=1433;password=<your-password>;database=dapr_test;`
const connectionStringEnvKey = "DAPR_TEST_SQL_CONNSTRING"

// Disable gosec in this test as we use string concatenation a lot with queries
func TestMigration(t *testing.T) {
	connectionString := os.Getenv(connectionStringEnvKey)
	if connectionString == "" {
		t.Skipf(`SQLServer migration test skipped. To enable this test, define the connection string using environment variable '%[1]s' (example 'export %[1]s="server=localhost;user id=sa;password=Pass@Word1;port=1433;database=dapr_test;")'`, connectionStringEnvKey)
	}

	log := logger.NewLogger("migration-test")
	log.SetOutputLevel(logger.DebugLevel)

	// Connect to the database
	db, err := sql.Open("sqlserver", connectionString)
	require.NoError(t, err, "Failed to connect to database")
	t.Cleanup(func() {
		_ = db.Close()
	})

	// Create a new schema for testing
	schema := getUniqueDBSchema(t)
	_, err = db.Exec(fmt.Sprintf("CREATE SCHEMA [%s]", schema))
	require.NoError(t, err, "Failed to create schema")
	t.Cleanup(func() {
		err = commonsqlserver.DropSchema(t.Context(), db, schema)
		require.NoError(t, err, "Failed to drop schema")
	})

	t.Run("Metadata table", func(t *testing.T) {
		m := &Migrations{
			DB:                db,
			Logger:            log,
			Schema:            schema,
			MetadataTableName: "metadata_1",
			MetadataKey:       "migrations",
		}

		t.Run("Create new", func(t *testing.T) {
			err = m.Perform(t.Context(), []commonsql.MigrationFn{})
			require.NoError(t, err)

			assertTableExists(t, db, schema, "metadata_1")
		})

		t.Run("Already exists", func(t *testing.T) {
			err = m.Perform(t.Context(), []commonsql.MigrationFn{})
			require.NoError(t, err)

			assertTableExists(t, db, schema, "metadata_1")
		})
	})

	t.Run("Perform migrations", func(t *testing.T) {
		m := &Migrations{
			DB:                db,
			Logger:            log,
			Schema:            schema,
			MetadataTableName: "metadata_2",
			MetadataKey:       "migrations",
		}

		fn1 := func(ctx context.Context) error {
			_, err = m.DB.Exec(fmt.Sprintf("CREATE TABLE [%s].[TestTable] ([Key] INTEGER NOT NULL PRIMARY KEY)", schema))
			return err
		}

		t.Run("First migration", func(t *testing.T) {
			err = m.Perform(t.Context(), []commonsql.MigrationFn{fn1})
			require.NoError(t, err)

			assertTableExists(t, db, schema, "TestTable")
			assertMigrationsLevel(t, db, schema, "metadata_2", "migrations", "1")
		})

		t.Run("Second migration", func(t *testing.T) {
			var called bool
			fn2 := func(ctx context.Context) error {
				// We don't actually have to do anything here, we just care that the migration level has increased
				called = true
				return nil
			}

			err = m.Perform(t.Context(), []commonsql.MigrationFn{fn1, fn2})
			require.NoError(t, err)

			assert.True(t, called)
			assertMigrationsLevel(t, db, schema, "metadata_2", "migrations", "2")
		})

		t.Run("Already has migrated", func(t *testing.T) {
			var called bool
			fn2 := func(ctx context.Context) error {
				// We don't actually have to do anything here, we just care that the migration level has increased
				called = true
				return nil
			}

			err = m.Perform(t.Context(), []commonsql.MigrationFn{fn1, fn2})
			require.NoError(t, err)

			assert.False(t, called)
			assertMigrationsLevel(t, db, schema, "metadata_2", "migrations", "2")
		})
	})

	t.Run("Perform migrations concurrently", func(t *testing.T) {
		counter := atomic.Uint32{}
		fn := func(ctx context.Context) error {
			// This migration doesn't actually do anything
			counter.Add(1)
			return nil
		}

		const parallel = 5
		errs := make(chan error, parallel)
		hasLogs := atomic.Uint32{}
		for i := range parallel {
			go func(i int) {
				// Collect logs
				collectLog := logger.NewLogger("concurrent-" + strconv.Itoa(i))
				collectLog.SetOutputLevel(logger.DebugLevel)
				buf := &bytes.Buffer{}
				collectLog.SetOutput(io.MultiWriter(buf, os.Stdout))

				m := &Migrations{
					DB:                db,
					Logger:            collectLog,
					Schema:            schema,
					MetadataTableName: "metadata_2",
					MetadataKey:       "migrations_concurrent",
				}

				migrateErr := m.Perform(t.Context(), []commonsql.MigrationFn{fn})
				if migrateErr != nil {
					errs <- fmt.Errorf("migration failed in handler %d: %w", i, migrateErr)
				}

				// One and only one of the loggers should have any message including "Performing migration"
				if strings.Contains(buf.String(), "Performing migration") {
					hasLogs.Add(1)
				}

				errs <- nil
			}(i)
		}

		for range parallel {
			select {
			case err := <-errs:
				assert.NoError(t, err) //nolint:testifylint
			case <-time.After(30 * time.Second):
				t.Fatal("timed out waiting for migrations to complete")
			}
		}
		if t.Failed() {
			// Short-circuit
			t.FailNow()
		}

		// Handler should have been invoked just once
		assert.Equal(t, uint32(1), counter.Load(), "Migrations handler invoked more than once")
		assert.Equal(t, uint32(1), hasLogs.Load(), "More than one logger indicated a migration")
	})
}

func getUniqueDBSchema(t *testing.T) string {
	t.Helper()

	b := make([]byte, 4)
	_, err := io.ReadFull(rand.Reader, b)
	require.NoError(t, err)
	return "m%s" + hex.EncodeToString(b)
}

func assertTableExists(t *testing.T, db *sql.DB, schema, table string) {
	t.Helper()

	var found int
	err := db.QueryRow(
		fmt.Sprintf("SELECT 1 WHERE OBJECT_ID('[%s].[%s]', 'U') IS NOT NULL", schema, table),
	).Scan(&found)
	require.NoErrorf(t, err, "Table %s not found", table)
	require.Equalf(t, 1, found, "Table %s not found", table)
}

func assertMigrationsLevel(t *testing.T, db *sql.DB, schema, table, key, expectLevel string) {
	t.Helper()

	var foundLevel string
	err := db.QueryRow(
		fmt.Sprintf("SELECT [Value] FROM [%s].[%s] WHERE [Key] = @Key", schema, table),
		sql.Named("Key", key),
	).Scan(&foundLevel)
	require.NoError(t, err, "Failed to load migrations level")
	require.Equal(t, expectLevel, foundLevel, "Migration level does not match")
}

// logRecorder is a logger that records errors, and records fatals instead of exiting the process.
type logRecorder struct {
	logger.Logger
	errors []string
	fatals []string
}

func (r *logRecorder) Error(args ...any) {
	r.errors = append(r.errors, fmt.Sprint(args...))
}

func (r *logRecorder) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

func (r *logRecorder) Fatal(args ...any) {
	r.fatals = append(r.fatals, fmt.Sprint(args...))
}

func (r *logRecorder) Fatalf(format string, args ...any) {
	r.fatals = append(r.fatals, fmt.Sprintf(format, args...))
}

func newMockMigrations(t *testing.T) (*Migrations, *sql.DB, sqlmock.Sqlmock, *logRecorder) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	rec := &logRecorder{Logger: logger.NewLogger("test")}
	return &Migrations{
		DB:                db,
		Logger:            rec,
		Schema:            "dbo",
		MetadataTableName: "metadata",
		MetadataKey:       "migrations",
	}, db, mock, rec
}

// expectLockedMigration sets up the expectations of Perform up to and including reading the migration level.
func expectLockedMigration(mock sqlmock.Sqlmock) {
	mock.ExpectExec(`IF OBJECT_ID\('\[dbo\]\.\[metadata\]', 'U'\) IS NULL`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`INSERT INTO \[dbo\]\.\[metadata\]`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectBegin()
	mock.ExpectQuery(`WITH \(XLOCK, ROWLOCK\)`).WillReturnRows(sqlmock.NewRows([]string{"Value"}).AddRow("lock"))
	mock.ExpectQuery(`SELECT \[Value\] FROM \[dbo\]\.\[metadata\] WHERE \[Key\] = 'migrations'`).WillReturnError(sql.ErrNoRows)
}

func TestPerformReleasesLockWithoutExiting(t *testing.T) {
	t.Run("context cancelled mid-migration: the lock is still released", func(t *testing.T) {
		m, _, mock, rec := newMockMigrations(t)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		// Shutdown arrives while the first migration runs.
		fns := []commonsql.MigrationFn{func(ctx context.Context) error {
			cancel()
			return ctx.Err()
		}}

		expectLockedMigration(mock)
		mock.ExpectRollback()

		err := m.Perform(ctx, fns)
		require.ErrorIs(t, err, context.Canceled)
		require.NoError(t, mock.ExpectationsWereMet())
		assert.Empty(t, rec.errors, "the rollback must succeed despite the cancelled context")
		assert.Empty(t, rec.fatals)
	})

	t.Run("rollback fails: logged, not fatal, and the lock connection is discarded", func(t *testing.T) {
		m, db, mock, rec := newMockMigrations(t)
		fns := []commonsql.MigrationFn{func(context.Context) error {
			return errors.New("migration failed")
		}}

		expectLockedMigration(mock)
		mock.ExpectRollback().WillReturnError(errors.New("connection reset"))

		err := m.Perform(t.Context(), fns)
		require.ErrorContains(t, err, "migration failed")
		require.NoError(t, mock.ExpectationsWereMet())
		assert.Len(t, rec.errors, 1)
		assert.Empty(t, rec.fatals)
		// The only connection left is the idle one that ran the other queries; the lock connection was closed.
		assert.Equal(t, 1, db.Stats().OpenConnections)
	})
}
