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

package postgresql

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dapr/components-contrib/internal/component/postgresql"
	"github.com/dapr/kit/logger"
)

// Performs migrations for the database schema
type migrations struct {
	logger            logger.Logger
	stateTableName    string
	metadataTableName string
}

// performMigration the required migrations
func performMigration(ctx context.Context, db postgresql.PGXPoolConn, opts postgresql.MigrateOptions) error {
	m := &migrations{
		logger:            opts.Logger,
		stateTableName:    opts.StateTableName,
		metadataTableName: opts.MetadataTableName,
	}

	// Use an advisory lock (with an arbitrary number) to ensure that no one else is performing migrations at the same time
	// This is the only way to also ensure we are not running multiple "CREATE TABLE IF NOT EXISTS" at the exact same time
	// See: https://www.postgresql.org/message-id/CA+TgmoZAdYVtwBfp1FL2sMZbiHCWT4UPrzRLNnX1Nb30Ku3-gg@mail.gmail.com
	const lockID = 42

	// Long timeout here as this query may block
	queryCtx, cancel := context.WithTimeout(ctx, time.Minute)
	_, err := db.Exec(queryCtx, "SELECT pg_advisory_lock($1)", lockID)
	cancel()
	if err != nil {
		return fmt.Errorf("faild to acquire advisory lock: %w", err)
	}

	// Release the lock
	defer func() {
		queryCtx, cancel = context.WithTimeout(ctx, time.Minute)
		_, err = db.Exec(queryCtx, "SELECT pg_advisory_unlock($1)", lockID)
		cancel()
		if err != nil {
			// Panicking here, as this forcibly closes the session and thus ensures we are not leaving locks hanging around
			m.logger.Fatalf("Failed to release advisory lock: %v", err)
		}
	}()

	// Check if the metadata table exists, which we also use to store the migration level
	queryCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
	exists, _, _, err := m.tableExists(queryCtx, db, m.metadataTableName)
	cancel()
	if err != nil {
		return err
	}

	// If the table doesn't exist, create it
	if !exists {
		queryCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
		err = m.createMetadataTable(queryCtx, db)
		cancel()
		if err != nil {
			return err
		}
	}

	// Select the migration level
	var (
		migrationLevelStr string
		migrationLevel    int
	)
	queryCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
	err = db.QueryRow(queryCtx,
		fmt.Sprintf(`SELECT value FROM %s WHERE key = 'migrations'`, m.metadataTableName),
	).Scan(&migrationLevelStr)
	cancel()
	if errors.Is(err, pgx.ErrNoRows) {
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
		m.logger.Infof("Performing migration %d", i)
		err = allMigrations[i](ctx, db, m)
		if err != nil {
			return fmt.Errorf("failed to perform migration %d: %w", i, err)
		}

		queryCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
		_, err = db.Exec(queryCtx,
			fmt.Sprintf(`INSERT INTO %s (key, value) VALUES ('migrations', $1) ON CONFLICT (key) DO UPDATE SET value = $1`, m.metadataTableName),
			strconv.Itoa(i+1),
		)
		cancel()
		if err != nil {
			return fmt.Errorf("failed to update migration level in metadata table: %w", err)
		}
	}

	return nil
}

func (m migrations) createMetadataTable(ctx context.Context, db postgresql.PGXPoolConn) error {
	m.logger.Infof("Creating metadata table '%s'", m.metadataTableName)
	// Add an "IF NOT EXISTS" in case another Dapr sidecar is creating the same table at the same time
	// In the next step we'll acquire a lock so there won't be issues with concurrency
	_, err := db.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
			key text NOT NULL PRIMARY KEY,
			value text NOT NULL
		)`,
		m.metadataTableName,
	))
	if err != nil {
		return fmt.Errorf("failed to create metadata table: %w", err)
	}
	return nil
}

// If the table exists, returns true and the name of the table and schema
func (m migrations) tableExists(ctx context.Context, db postgresql.PGXPoolConn, tableName string) (exists bool, schema string, table string, err error) {
	table, schema, err = m.tableSchemaName(tableName)
	if err != nil {
		return false, "", "", err
	}

	if schema == "" {
		err = db.QueryRow(
			ctx,
			`SELECT table_name, table_schema
				FROM information_schema.tables 
				WHERE table_name = $1`,
			table,
		).
			Scan(&table, &schema)
	} else {
		err = db.QueryRow(
			ctx,
			`SELECT table_name, table_schema
				FROM information_schema.tables 
				WHERE table_schema = $1 AND table_name = $2`,
			schema, table,
		).
			Scan(&table, &schema)
	}

	if err != nil && errors.Is(err, pgx.ErrNoRows) {
		return false, "", "", nil
	} else if err != nil {
		return false, "", "", fmt.Errorf("failed to check if table '%s' exists: %w", tableName, err)
	}
	return true, schema, table, nil
}

// If the table name includes a schema (e.g. `schema.table`, returns the two parts separately)
func (m migrations) tableSchemaName(tableName string) (table string, schema string, err error) {
	parts := strings.Split(tableName, ".")
	switch len(parts) {
	case 1:
		return parts[0], "", nil
	case 2:
		return parts[1], parts[0], nil
	default:
		return "", "", errors.New("invalid table name: must be in the format 'table' or 'schema.table'")
	}
}

var allMigrations = [2]func(ctx context.Context, db postgresql.PGXPoolConn, m *migrations) error{
	// Migration 0: create the state table
	func(ctx context.Context, db postgresql.PGXPoolConn, m *migrations) error {
		// We need to add an "IF NOT EXISTS" because we may be migrating from when we did not use a metadata table
		m.logger.Infof("Creating state table '%s'", m.stateTableName)
		_, err := db.Exec(
			ctx,
			fmt.Sprintf(
				`CREATE TABLE IF NOT EXISTS %s (
					key text NOT NULL PRIMARY KEY,
					value jsonb NOT NULL,
					isbinary boolean NOT NULL,
					insertdate TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
					updatedate TIMESTAMP WITH TIME ZONE NULL
				)`,
				m.stateTableName,
			),
		)
		if err != nil {
			return fmt.Errorf("failed to create state table: %w", err)
		}
		return nil
	},

	// Migration 1: add the "expiredate" column
	func(ctx context.Context, db postgresql.PGXPoolConn, m *migrations) error {
		m.logger.Infof("Adding expiredate column to state table '%s'", m.stateTableName)
		_, err := db.Exec(ctx, fmt.Sprintf(
			`ALTER TABLE %s ADD expiredate TIMESTAMP WITH TIME ZONE`,
			m.stateTableName,
		))
		if err != nil {
			return fmt.Errorf("failed to update state table: %w", err)
		}
		return nil
	},
}
