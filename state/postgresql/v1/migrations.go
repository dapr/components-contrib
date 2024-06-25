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

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"

	pginterfaces "github.com/dapr/components-contrib/common/component/postgresql/interfaces"
	postgresql "github.com/dapr/components-contrib/common/component/postgresql/v1"
	commonsql "github.com/dapr/components-contrib/common/component/sql"
	pgmigrations "github.com/dapr/components-contrib/common/component/sql/migrations/postgres"
)

// Performs the required migrations
func performMigrations(ctx context.Context, db pginterfaces.PGXPoolConn, opts postgresql.MigrateOptions) error {
	m := pgmigrations.Migrations{
		DB:                db,
		Logger:            opts.Logger,
		MetadataTableName: opts.MetadataTableName,
		MetadataKey:       "migrations",
	}

	return m.Perform(ctx, []commonsql.MigrationFn{
		// Migration 0: create the state table
		func(ctx context.Context) error {
			opts.Logger.Infof("Creating state table '%s'", opts.StateTableName)
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
					opts.StateTableName,
				),
			)
			if err != nil {
				// Check if the error is about a duplicate key constraint violation.
				// Note: This can occur due to a race of multiple sidecars trying to run the table creation within their own transactions.
				// It's then a race to see who actually gets to create the table, and who gets the unique constraint violation error.
				// If the error is not a UniqueViolation (23505), abort
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
					opts.Logger.Debugf("ignoring PostgreSQL duplicate key error for table '%s'", opts.StateTableName)
				} else {
					return fmt.Errorf("failed to create state table: '%s', %v", opts.StateTableName, err)
				}
			}

			return nil
		},

		// Migration 1: add the "expiredate" column
		func(ctx context.Context) error {
			opts.Logger.Infof("Adding expiredate column to state table '%s'", opts.StateTableName)
			_, err := db.Exec(ctx, fmt.Sprintf(
				`ALTER TABLE %s ADD expiredate TIMESTAMP WITH TIME ZONE`,
				opts.StateTableName,
			))
			if err != nil {
				return fmt.Errorf("failed to update state table: %w", err)
			}
			return nil
		},
	},
	)
}
