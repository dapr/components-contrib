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
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	pginterfaces "github.com/dapr/components-contrib/internal/component/postgresql/interfaces"
	sqlinternal "github.com/dapr/components-contrib/internal/component/sql"
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
func (m Migrations) Perform(ctx context.Context, migrationFns []sqlinternal.MigrationFn) error {
	// Use an advisory lock (with an arbitrary number) to ensure that no one else is performing migrations at the same time
	// This is the only way to also ensure we are not running multiple "CREATE TABLE IF NOT EXISTS" at the exact same time
	// See: https://www.postgresql.org/message-id/CA+TgmoZAdYVtwBfp1FL2sMZbiHCWT4UPrzRLNnX1Nb30Ku3-gg@mail.gmail.com
	const lockID = 42

	// Long timeout here as this query may block
	m.Logger.Debug("Acquiring advisory lock pre-migration")
	queryCtx, cancel := context.WithTimeout(ctx, time.Minute)
	_, err := m.DB.Exec(queryCtx, "SELECT pg_advisory_lock($1)", lockID)
	cancel()
	if err != nil {
		return fmt.Errorf("faild to acquire advisory lock: %w", err)
	}
	m.Logger.Debug("Successfully acquired advisory lock")

	// Release the lock
	defer func() {
		m.Logger.Debug("Releasing advisory lock")
		queryCtx, cancel = context.WithTimeout(ctx, time.Minute)
		_, err = m.DB.Exec(queryCtx, "SELECT pg_advisory_unlock($1)", lockID)
		cancel()
		if err != nil {
			// Panicking here, as this forcibly closes the session and thus ensures we are not leaving locks hanging around
			m.Logger.Fatalf("Failed to release advisory lock: %v", err)
		}
	}()

	return sqlinternal.Migrate(ctx, sqlinternal.AdaptPgxConn(m.DB), sqlinternal.MigrationOptions{
		Logger: m.Logger,
		// Yes, we are using fmt.Sprintf for adding a value in a query.
		// This comes from a constant hardcoded at development-time, and cannot be influenced by users. So, no risk of SQL injections here.
		GetVersionQuery: fmt.Sprintf(`SELECT value FROM %s WHERE key = '%s'`, m.MetadataTableName, m.MetadataKey),
		UpdateVersionQuery: func(version string) (string, any) {
			return fmt.Sprintf(`INSERT INTO %s (key, value) VALUES ('%s', $1) ON CONFLICT (key) DO UPDATE SET value = $1`, m.MetadataTableName, m.MetadataKey),
				version
		},
		EnsureMetadataTable: func(ctx context.Context) error {
			// Check if the metadata table exists, which we also use to store the migration level
			queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			exists, _, _, err := m.TableExists(queryCtx, m.MetadataTableName)
			cancel()
			if err != nil {
				return err
			}

			// If the table doesn't exist, create it
			if !exists {
				queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				err = m.CreateMetadataTable(queryCtx)
				cancel()
				if err != nil {
					return err
				}
			}

			return nil
		},
		Migrations: migrationFns,
	})
}

func (m Migrations) CreateMetadataTable(ctx context.Context) error {
	m.Logger.Infof("Creating metadata table '%s'", m.MetadataTableName)
	// Add an "IF NOT EXISTS" in case another Dapr sidecar is creating the same table at the same time
	// In the next step we'll acquire a lock so there won't be issues with concurrency
	_, err := m.DB.Exec(ctx, fmt.Sprintf(
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

// TableExists checks if the table exists, and returns true and the name of the table and schema.
func (m Migrations) TableExists(ctx context.Context, tableName string) (exists bool, schema string, table string, err error) {
	table, schema, err = m.TableSchemaName(tableName)
	if err != nil {
		return false, "", "", err
	}

	if schema == "" {
		err = m.DB.QueryRow(
			ctx,
			`SELECT table_name, table_schema
				FROM information_schema.tables 
				WHERE table_name = $1`,
			table,
		).
			Scan(&table, &schema)
	} else {
		err = m.DB.QueryRow(
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

// TableSchemaName parses the table name.
// If the table name includes a schema (e.g. `schema.table`, returns the two parts separately).
func (m Migrations) TableSchemaName(tableName string) (table string, schema string, err error) {
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
