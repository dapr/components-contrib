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

package pgmigrations

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	pgxmock "github.com/pashagolub/pgxmock/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonsql "github.com/dapr/components-contrib/common/component/sql"
	"github.com/dapr/kit/logger"
)

const (
	testMetadataTable = "test_metadata"
	testMetadataKey   = "migrations-test"
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

func newTestMigrations(t *testing.T) (Migrations, pgxmock.PgxPoolIface, *logRecorder) {
	t.Helper()
	db, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(db.Close)

	rec := &logRecorder{Logger: logger.NewLogger("test")}
	return Migrations{
		DB:                db,
		Logger:            rec,
		MetadataTableName: testMetadataTable,
		MetadataKey:       testMetadataKey,
	}, db, rec
}

// expectLockedMigration sets up the expectations of Perform up to and including reading the migration level.
func expectLockedMigration(db pgxmock.PgxPoolIface) {
	db.ExpectExec("CREATE TABLE IF NOT EXISTS " + testMetadataTable).WillReturnResult(pgxmock.NewResult("CREATE TABLE", 0))
	db.ExpectExec("INSERT INTO " + testMetadataTable).WithArgs("lock").WillReturnResult(pgxmock.NewResult("INSERT", 1))
	db.ExpectBegin()
	db.ExpectQuery("SELECT value FROM " + testMetadataTable + " WHERE key = \\$1 FOR UPDATE").WithArgs("lock").
		WillReturnRows(pgxmock.NewRows([]string{"value"}).AddRow("lock"))
	db.ExpectQuery("SELECT value FROM " + testMetadataTable + " WHERE key = '" + testMetadataKey + "'").
		WillReturnError(pgx.ErrNoRows)
}

func TestPerformReleasesLockWithoutExiting(t *testing.T) {
	t.Run("context cancelled mid-migration: the lock is still released", func(t *testing.T) {
		m, db, rec := newTestMigrations(t)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		// Shutdown arrives while the first migration runs.
		fns := []commonsql.MigrationFn{func(ctx context.Context) error {
			cancel()
			return ctx.Err()
		}}

		expectLockedMigration(db)
		// pgxmock's Rollback returns ctx.Err(), so this fails if the rollback reuses the cancelled context.
		db.ExpectRollback()

		err := m.Perform(ctx, fns)
		require.ErrorIs(t, err, context.Canceled)
		require.NoError(t, db.ExpectationsWereMet())
		assert.Empty(t, rec.errors, "the rollback must succeed despite the cancelled context")
		assert.Empty(t, rec.fatals)
	})

	t.Run("rollback fails: logged, not fatal", func(t *testing.T) {
		m, db, rec := newTestMigrations(t)
		ran := 0
		fns := []commonsql.MigrationFn{func(context.Context) error {
			ran++
			return nil
		}}

		expectLockedMigration(db)
		db.ExpectExec("INSERT INTO " + testMetadataTable).WithArgs("1").WillReturnResult(pgxmock.NewResult("INSERT", 1))
		db.ExpectRollback().WillReturnError(errors.New("connection reset"))

		// The migration was applied; a failed rollback closes the connection, which releases the lock.
		require.NoError(t, m.Perform(t.Context(), fns))
		assert.Equal(t, 1, ran)
		require.NoError(t, db.ExpectationsWereMet())
		assert.Len(t, rec.errors, 1)
		assert.Empty(t, rec.fatals)
	})
}
