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
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	// Check if the metadata table exists, which we also use to store the migration level
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	exists, _, _, err := m.tableExists(queryCtx, m.MetadataTableName)
	cancel()
	if err != nil {
		return err
	}

	// If the table doesn't exist, create it
	if !exists {
		queryCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
		err = m.createMetadataTable(queryCtx)
		cancel()
		if err != nil {
			return err
		}
	}

	// Acquire an exclusive lock on the metadata table, which we will use to ensure no one else is performing migrations
	metadataTx, err := m.Conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer metadataTx.Rollback()

	// Long timeout here to wait for other processes to complete the migrations, since this query blocks
	queryCtx, cancel = context.WithTimeout(ctx, 2*time.Minute)
	_, err = metadataTx.ExecContext(queryCtx, fmt.Sprintf("LOCK TABLE %s IN SHARE MODE", m.MetadataTableName))
	cancel()
	if err != nil {
		return fmt.Errorf("failed to acquire lock on metadata table: %w", err)
	}

	// Select the migration level
	var (
		migrationLevelStr string
		migrationLevel    int
	)
	queryCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
	err = metadataTx.
		QueryRowContext(queryCtx,
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
		m.Logger.Infof("Performing migration %d", i)
		err = allMigrations[i](ctx, m)
		if err != nil {
			return fmt.Errorf("failed to perform migration %d: %w", i, err)
		}

		queryCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
		_, err = metadataTx.ExecContext(queryCtx,
			fmt.Sprintf(`UPDATE %s SET value = $1 WHERE key = 'migrations'`, m.MetadataTableName),
			strconv.Itoa(i+1),
		)
		cancel()
	}

	// Commit changes to the metadata table, which also releases the lock
	err = metadataTx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (m migrations) createMetadataTable(ctx context.Context) error {
	m.Logger.Infof("Creating metadata table '%s'", m.MetadataTableName)
	// Add an "IF NOT EXISTS" in case another Dapr sidecar is creating the same table at the same time
	// In the next step we'll acquire a lock so there won't be issues with concurrency
	_, err := m.Conn.Exec(fmt.Sprintf(
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

// If the table exists, returns true and the name of the table and schema
func (m migrations) tableExists(ctx context.Context, tableName string) (exists bool, schema string, table string, err error) {
	table, schema, err = m.tableSchemaName(tableName)
	if err != nil {
		return false, "", "", err
	}

	if schema == "" {
		err = m.Conn.
			QueryRowContext(
				ctx,
				`SELECT table_name, table_schema
				FROM information_schema.tables 
				WHERE table_name = $1`,
				table,
			).
			Scan(&table, &schema)
	} else {
		err = m.Conn.
			QueryRowContext(
				ctx,
				`SELECT table_name, table_schema
				FROM information_schema.tables 
				WHERE table_schema = $1 AND table_name = $2`,
				schema, table,
			).
			Scan(&table, &schema)
	}

	if err != nil && errors.Is(err, sql.ErrNoRows) {
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

var allMigrations = [2]func(ctx context.Context, m *migrations) error{
	// Migration 0: create the state table
	func(ctx context.Context, m *migrations) error {
		// We need to add an "IF NOT EXISTS" because we may be migrating from when we did not use a metadata table
		m.Logger.Infof("Creating state table '%s'", m.StateTableName)
		_, err := m.Conn.Exec(
			fmt.Sprintf(
				`CREATE TABLE IF NOT EXISTS %s (
					key text NOT NULL PRIMARY KEY,
					value jsonb NOT NULL,
					isbinary boolean NOT NULL,
					insertdate TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
					updatedate TIMESTAMP WITH TIME ZONE NULL
				)`,
				m.StateTableName,
			),
		)
		if err != nil {
			return fmt.Errorf("failed to create state table: %w", err)
		}
		return nil
	},

	// Migration 1: add the "expiredate" column
	func(ctx context.Context, m *migrations) error {
		m.Logger.Infof("Adding expiredate column to state table '%s'", m.StateTableName)
		_, err := m.Conn.Exec(fmt.Sprintf(
			`ALTER TABLE %s ADD expiredate TIMESTAMP WITH TIME ZONE`,
			m.StateTableName,
		))
		if err != nil {
			return fmt.Errorf("failed to update state table: %w", err)
		}
		return nil
	},
}
