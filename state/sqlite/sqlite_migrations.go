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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dapr/kit/logger"
)

// Performs migrations for the database schema
type migrations struct {
	Logger            logger.Logger
	Conn              *sql.DB
	StateTableName    string
	MetadataTableName string
}

// Perform the required migrations
func (m *migrations) Perform(ctx context.Context) error {
	// Begin a transaction
	tx, err := m.Conn.Begin()
	if err != nil {
		return fmt.Errorf("faild to begin transaction: %w", err)
	}

	// Rollback the transaction in a deferred statement to catch errors
	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			// Panicking here, as this forcibly closes the session and thus ensures we are not leaving locks hanging around
			m.Logger.Fatalf("Failed to rollback transaction: %v", err)
		}
	}()

	// Check if the metadata table exists, which we also use to store the migration level
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	exists, err := m.tableExists(queryCtx, tx, m.MetadataTableName)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to check if the metadata table exists: %w", err)
	}

	// If the table doesn't exist, create it
	if !exists {
		queryCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
		err = m.createMetadataTable(queryCtx, tx)
		cancel()
		if err != nil {
			return fmt.Errorf("failed to create metadata table: %w", err)
		}
	}

	// Select the migration level
	var (
		migrationLevelStr string
		migrationLevel    int
	)
	queryCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
	err = tx.QueryRowContext(queryCtx,
		fmt.Sprintf(`SELECT value FROM %s WHERE key = 'migrations'`, m.MetadataTableName),
	).Scan(&migrationLevelStr)
	cancel()
	if errors.Is(err, sql.ErrNoRows) {
		// If there's no row...
		migrationLevel = 0
	} else if err != nil {
		return fmt.Errorf("failed to read migration level: %w", err)
	} else {
		migrationLevel, err = strconv.Atoi(migrationLevelStr)
		if err != nil || migrationLevel < 0 {
			return fmt.Errorf("invalid migration level found in metadata table: %s", migrationLevelStr)
		}
	}

	// Perform the migrations
	for i := migrationLevel; i < len(allMigrations); i++ {
		m.Logger.Infof("Performing migration %d", i+1)
		err = allMigrations[i](ctx, tx, m)
		if err != nil {
			return fmt.Errorf("failed to perform migration %d: %w", i+1, err)
		}

		queryCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
		_, err = tx.ExecContext(queryCtx,
			fmt.Sprintf(`REPLACE INTO %s (key, value) VALUES ('migrations', ?)`, m.MetadataTableName),
			strconv.Itoa(i+1),
		)
		cancel()
		if err != nil {
			return fmt.Errorf("failed to update migration level in metadata table: %w", err)
		}
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction")
	}

	return nil
}

// Returns true if a table exists
func (m migrations) tableExists(parentCtx context.Context, db querier, tableName string) (bool, error) {
	ctx, cancel := context.WithTimeout(parentCtx, 30*time.Second)
	defer cancel()

	var exists string
	// Returns 1 or 0 as a string if the table exists or not.
	const q = `SELECT EXISTS (
		SELECT name FROM sqlite_master WHERE type='table' AND name = ?
	) AS 'exists'`
	err := db.QueryRowContext(ctx, q, m.MetadataTableName).
		Scan(&exists)
	return exists == "1", err
}

func (m migrations) createMetadataTable(ctx context.Context, db querier) error {
	m.Logger.Infof("Creating metadata table '%s' if it doesn't exist", m.MetadataTableName)
	// Add an "IF NOT EXISTS" in case another Dapr sidecar is creating the same table at the same time
	// In the next step we'll acquire a lock so there won't be issues with concurrency
	_, err := db.ExecContext(ctx, fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
			key text NOT NULL PRIMARY KEY,
			value text NOT NULL
		)`,
		m.MetadataTableName,
	))
	if err != nil {
		return fmt.Errorf("failed to create metadata table: %w", err)
	}
	return nil
}

var allMigrations = [1]func(ctx context.Context, db querier, m *migrations) error{
	// Migration 0: create the state table
	func(ctx context.Context, db querier, m *migrations) error {
		// We need to add an "IF NOT EXISTS" because we may be migrating from when we did not use a metadata table
		m.Logger.Infof("Creating state table '%s'", m.StateTableName)
		_, err := db.ExecContext(
			ctx,
			fmt.Sprintf(
				`CREATE TABLE %s (
					key TEXT NOT NULL PRIMARY KEY,
					value TEXT NOT NULL,
					is_binary BOOLEAN NOT NULL,
					etag TEXT NOT NULL,
					expiration_time TIMESTAMP DEFAULT NULL,
					update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
				)`,
				m.StateTableName,
			),
		)
		if err != nil {
			return fmt.Errorf("failed to create state table: %w", err)
		}
		return nil
	},
}
