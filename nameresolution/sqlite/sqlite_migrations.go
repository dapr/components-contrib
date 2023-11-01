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

	sqlinternal "github.com/dapr/components-contrib/internal/component/sql"
	sqlitemigrations "github.com/dapr/components-contrib/internal/component/sql/migrations/sqlite"
	"github.com/dapr/kit/logger"
)

type migrationOptions struct {
	HostsTableName    string
	MetadataTableName string
}

// Perform the required migrations
func performMigrations(ctx context.Context, db *sql.DB, logger logger.Logger, opts migrationOptions) error {
	m := sqlitemigrations.Migrations{
		Pool:              db,
		Logger:            logger,
		MetadataTableName: opts.MetadataTableName,
		MetadataKey:       "nr-migrations",
	}

	return m.Perform(ctx, []sqlinternal.MigrationFn{
		// Migration 0: create the hosts table
		func(ctx context.Context) error {
			logger.Infof("Creating hosts table '%s'", opts.HostsTableName)
			_, err := m.GetConn().ExecContext(
				ctx,
				fmt.Sprintf(
					`CREATE TABLE %[1]s (
						registration_id TEXT NOT NULL PRIMARY KEY,
						address TEXT NOT NULL,
						app_id TEXT NOT NULL,
						namespace TEXT NOT NULL,
						last_update INTEGER NOT NULL
					);
					CREATE UNIQUE INDEX %[1]s_address_idx ON %[1]s (address);
					CREATE INDEX %[1]s_last_update_idx ON %[1]s (last_update);`,
					opts.HostsTableName,
				),
			)
			if err != nil {
				return fmt.Errorf("failed to create hosts table: %w", err)
			}
			return nil
		},
	})
}
