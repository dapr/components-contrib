/*
Copyright 2021 The Dapr Authors
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
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"

	pginterfaces "github.com/dapr/components-contrib/common/component/postgresql/interfaces"
	commonsql "github.com/dapr/components-contrib/common/component/sql"
	"github.com/dapr/kit/logger"
)

// Migrations performs migrations for the database schema
type Migrations struct {
	DB                pginterfaces.PGXPoolConn
	Logger            logger.Logger
	MetadataTableName string
	MetadataKey       string
}

// Perform the required migrations
func (m Migrations) Perform(ctx context.Context, migrationFns []commonsql.MigrationFn) error {
	// Ensure the metadata table exists
	// This query uses an "IF NOT EXISTS" so it's safe to be created concurrently
	err := m.EnsureMetadataTable(ctx)
	if err != nil {
		return fmt.Errorf("failed to ensure metadata table exists: %w", err)
	}

	// Normally, the way to acquire an exclusive lock in PostgreSQL (commonly used for migrations by other frameworks too) is to use advisory locks
	// However, advisory locks aren't supported in all PostgreSQL-compatible databases, for example CockroachDB
	// So, we're going to write a row in there (not using a transaction, as that causes a table-level lock to be created), ignoring duplicates
	const lockKey = "lock"
	m.Logger.Debugf("Ensuring lock row '%s' exists in metadata table", lockKey)
	queryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	_, err = m.DB.Exec(queryCtx, fmt.Sprintf("INSERT INTO %s (key, value) VALUES ($1, 'lock') ON CONFLICT (key) DO NOTHING", m.MetadataTableName), lockKey)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to ensure lock row '%s' exists: %w", lockKey, err)
	}

	// Now, let's use a transaction on a row in the metadata table as a lock
	m.Logger.Debug("Starting transaction pre-migration")
	queryCtx, cancel = context.WithTimeout(ctx, 15*time.Second)
	tx, err := m.DB.Begin(queryCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Always rollback the transaction at the end to release the lock, since the value doesn't really matter
	defer func() {
		m.Logger.Debug("Releasing migration lock")
		queryCtx, cancel = context.WithTimeout(ctx, 15*time.Second)
		rollbackErr := tx.Rollback(queryCtx)
		cancel()
		if rollbackErr != nil {
			// Panicking here, as this forcibly closes the session and thus ensures we are not leaving locks hanging around
			m.Logger.Fatalf("Failed to roll back transaction: %v", rollbackErr)
		}
	}()

	// Now, perform a SELECT with FOR UPDATE to lock the row used for locking, and only that row
	// We use a long timeout here as this query may block
	m.Logger.Debug("Acquiring migration lock")
	queryCtx, cancel = context.WithTimeout(ctx, time.Minute)
	var lock string
	err = tx.QueryRow(queryCtx, fmt.Sprintf("SELECT value FROM %s WHERE key = $1 FOR UPDATE", m.MetadataTableName), lockKey).Scan(&lock)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to acquire migration lock (row-level lock on key '%s'): %w", lockKey, err)
	}
	m.Logger.Debug("Migration lock acquired")

	return commonsql.Migrate(ctx, commonsql.AdaptPgxConn(m.DB), commonsql.MigrationOptions{
		Logger: m.Logger,
		// Yes, we are using fmt.Sprintf for adding a value in a query.
		// This comes from a constant hardcoded at development-time, and cannot be influenced by users. So, no risk of SQL injections here.
		GetVersionQuery: fmt.Sprintf(`SELECT value FROM %s WHERE key = '%s'`, m.MetadataTableName, m.MetadataKey),
		UpdateVersionQuery: func(version string) (string, any) {
			return fmt.Sprintf(`INSERT INTO %s (key, value) VALUES ('%s', $1) ON CONFLICT (key) DO UPDATE SET value = $1`, m.MetadataTableName, m.MetadataKey),
				version
		},
		Migrations: migrationFns,
	})
}

func (m Migrations) EnsureMetadataTable(ctx context.Context) (err error) {
	m.Logger.Infof("Creating metadata table '%s'", m.MetadataTableName)
	// Add an "IF NOT EXISTS" in case another Dapr sidecar is creating the same table at the same time
	// In the next step we'll acquire a lock so there won't be issues with concurrency
	// Note that this query can fail with error `23505` on constraint `pg_type_typname_nsp_index` if ran in parallel; we will just retry that up to 3 times
	for range 3 {
		_, err = m.DB.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s (
				key text NOT NULL PRIMARY KEY,
				value text NOT NULL
			)`,
			m.MetadataTableName,
		))
		if err == nil {
			break
		}

		// If the error is not a UniqueViolation (23505), abort
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != pgerrcode.UniqueViolation {
			return fmt.Errorf("failed to create metadata table: %w", err)
		}

		// Retry after a delay
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		return fmt.Errorf("failed to create metadata table: %w", err)
	}
	return nil
}
