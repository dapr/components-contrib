/*
Copyright 2026 The Dapr Authors
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

package sqlitemigrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Blank import for the SQLite driver
	_ "modernc.org/sqlite"

	commonsql "github.com/dapr/components-contrib/common/component/sql"
	"github.com/dapr/kit/logger"
)

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

func newTestMigrations(t *testing.T) (*Migrations, *sql.DB, *logRecorder) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "test.db")+"?_pragma=busy_timeout(100)")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	rec := &logRecorder{Logger: logger.NewLogger("test")}
	return &Migrations{
		Pool:              db,
		Logger:            rec,
		MetadataTableName: "metadata",
		MetadataKey:       "migrations",
	}, db, rec
}

func TestPerformRollsBackWithoutExiting(t *testing.T) {
	t.Run("context cancelled mid-migration: the transaction is still rolled back", func(t *testing.T) {
		m, db, rec := newTestMigrations(t)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		// Shutdown arrives while the first migration runs.
		fns := []commonsql.MigrationFn{func(ctx context.Context) error {
			_, err := m.GetConn().ExecContext(ctx, "CREATE TABLE migrated (id integer)")
			if err != nil {
				return err
			}
			cancel()
			return ctx.Err()
		}}

		err := m.Perform(ctx, fns)
		require.ErrorIs(t, err, context.Canceled)
		assert.Empty(t, rec.errors, "the rollback must succeed despite the cancelled context")
		assert.Empty(t, rec.fatals)

		// Rolled back: the migration's table is gone and the database is not left locked.
		var n int
		require.NoError(t, db.QueryRowContext(t.Context(), "SELECT count(*) FROM sqlite_master WHERE name = 'migrated'").Scan(&n))
		assert.Equal(t, 0, n)
		_, err = db.ExecContext(t.Context(), "CREATE TABLE after_shutdown (id integer)")
		require.NoError(t, err)
	})

	t.Run("rollback fails: logged, not fatal, and the connection is discarded", func(t *testing.T) {
		m, db, rec := newTestMigrations(t)

		// Ending the transaction early makes the deferred ROLLBACK fail.
		fns := []commonsql.MigrationFn{func(ctx context.Context) error {
			_, err := m.GetConn().ExecContext(ctx, "COMMIT")
			if err != nil {
				return err
			}
			return errors.New("migration failed")
		}}

		err := m.Perform(t.Context(), fns)
		require.ErrorContains(t, err, "migration failed")
		assert.Len(t, rec.errors, 1)
		assert.Empty(t, rec.fatals)
		assert.Equal(t, 0, db.Stats().OpenConnections, "a connection whose rollback failed must not go back to the pool")
	})
}
