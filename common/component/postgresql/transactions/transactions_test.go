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

package transactions

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	pgxmock "github.com/pashagolub/pgxmock/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/kit/logger"
)

// logRecorder is a logger that records errors.
type logRecorder struct {
	logger.Logger
	errors []string
}

func (r *logRecorder) Error(args ...any) {
	r.errors = append(r.errors, fmt.Sprint(args...))
}

func (r *logRecorder) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

func newTestDB(t *testing.T) (pgxmock.PgxPoolIface, *logRecorder) {
	t.Helper()
	db, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(db.Close)
	return db, &logRecorder{Logger: logger.NewLogger("test")}
}

func TestExecuteInTransaction(t *testing.T) {
	t.Run("success commits", func(t *testing.T) {
		db, rec := newTestDB(t)
		db.ExpectBegin()
		db.ExpectCommit()

		res, err := ExecuteInTransaction(t.Context(), rec, db, time.Minute, func(context.Context, pgx.Tx) (int, error) {
			return 42, nil
		})
		require.NoError(t, err)
		assert.Equal(t, 42, res)
		require.NoError(t, db.ExpectationsWereMet())
		assert.Empty(t, rec.errors)
	})

	t.Run("context cancelled mid-transaction: the rollback still succeeds", func(t *testing.T) {
		db, rec := newTestDB(t)
		db.ExpectBegin()
		// pgxmock's Rollback returns ctx.Err(), so this fails if the rollback reuses the cancelled context.
		db.ExpectRollback()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		_, err := ExecuteInTransaction(ctx, rec, db, time.Minute, func(ctx context.Context, _ pgx.Tx) (int, error) {
			cancel()
			return 0, ctx.Err()
		})
		require.ErrorIs(t, err, context.Canceled)
		require.NoError(t, db.ExpectationsWereMet())
		assert.Empty(t, rec.errors, "the rollback must succeed despite the cancelled context")
	})

	t.Run("rollback fails: logged and the callback's error is returned", func(t *testing.T) {
		db, rec := newTestDB(t)
		db.ExpectBegin()
		db.ExpectRollback().WillReturnError(errors.New("connection reset"))

		_, err := ExecuteInTransaction(t.Context(), rec, db, time.Minute, func(context.Context, pgx.Tx) (int, error) {
			return 0, errors.New("callback failed")
		})
		require.ErrorContains(t, err, "callback failed")
		require.NoError(t, db.ExpectationsWereMet())
		require.Len(t, rec.errors, 1)
		assert.Contains(t, rec.errors[0], "connection reset")
	})

	t.Run("commit fails: the error is returned and the rollback is attempted", func(t *testing.T) {
		db, rec := newTestDB(t)
		db.ExpectBegin()
		db.ExpectCommit().WillReturnError(errors.New("serialization failure"))
		db.ExpectRollback()

		_, err := ExecuteInTransaction(t.Context(), rec, db, time.Minute, func(context.Context, pgx.Tx) (int, error) {
			return 1, nil
		})
		require.ErrorContains(t, err, "failed to commit transaction: serialization failure")
		require.NoError(t, db.ExpectationsWereMet())
		assert.Empty(t, rec.errors)
	})
}
