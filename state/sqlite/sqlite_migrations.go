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
	"fmt"

	commonsql "github.com/dapr/components-contrib/common/component/sql"
	sqlitemigrations "github.com/dapr/components-contrib/common/component/sql/migrations/sqlite"
	"github.com/dapr/kit/logger"
)

type migrationOptions struct {
	StateTableName    string
	MetadataTableName string
}

// Perform the required migrations
func performMigrations(ctx context.Context, db *sql.DB, logger logger.Logger, opts migrationOptions) error {
	m := sqlitemigrations.Migrations{
		Pool:              db,
		Logger:            logger,
		MetadataTableName: opts.MetadataTableName,
		MetadataKey:       "migrations",
	}

	return m.Perform(ctx, []commonsql.MigrationFn{
		// Migration 0: create the state table
		func(ctx context.Context) error {
			// We need to add an "IF NOT EXISTS" because we may be migrating from when we did not use a metadata table
			logger.Infof("Creating state table '%s'", opts.StateTableName)
			_, err := m.GetConn().ExecContext(
				ctx,
				fmt.Sprintf(
					`CREATE TABLE IF NOT EXISTS %s (
							key TEXT NOT NULL PRIMARY KEY,
							value TEXT NOT NULL,
							is_binary BOOLEAN NOT NULL,
							etag TEXT NOT NULL,
							expiration_time TIMESTAMP DEFAULT NULL,
							update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
						)`,
					opts.StateTableName,
				),
			)
			if err != nil {
				return fmt.Errorf("failed to create state table: %w", err)
			}
			return nil
		},
		// Migration 1: add the "prefix" column
		func(ctx context.Context) error {
			// We add this virtual prefix column to enable us to delete an actor's state since this prefix will tell us which actor created it
			logger.Infof("Creating virtual collumn for table '%s'", opts.StateTableName)
			_, err := m.GetConn().ExecContext(
				ctx,
				fmt.Sprintf(
					`ALTER TABLE %s ADD COLUMN prefix TEXT GENERATED ALWAYS AS (SUBSTR(key, 1, LENGTH(key) - INSTR(SUBSTR(key, '||', -1), '||') - 1)) VIRTUAL;`,
					opts.StateTableName,
				),
			)
			if err != nil {
				return fmt.Errorf("failed to create state table: %w", err)
			}
			return nil
		},
	},
	)
}
