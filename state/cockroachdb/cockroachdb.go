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

package cockroachdb

import (
	"context"
	"fmt"

	"github.com/dapr/components-contrib/internal/component/postgresql"
	pginterfaces "github.com/dapr/components-contrib/internal/component/postgresql/interfaces"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

// New creates a new instance of CockroachDB state store.
func New(logger logger.Logger) state.Store {
	return postgresql.NewPostgreSQLStateStore(logger, postgresql.Options{
		ETagColumn: "etag",
		MigrateFn:  ensureTables,
		SetQueryFn: func(req *state.SetRequest, opts postgresql.SetQueryOptions) string {
			// Sprintf is required for table name because the driver does not substitute parameters for table names.
			if !req.HasETag() {
				// We do an upsert in both cases, even when concurrency is first-write, because the row may exist but be expired (and not yet garbage collected)
				// The difference is that with concurrency as first-write, we'll update the row only if it's expired
				var whereClause string
				if req.Options.Concurrency == state.FirstWrite {
					whereClause = " WHERE (t.expiredate IS NOT NULL AND t.expiredate < CURRENT_TIMESTAMP)"
				}

				return `
INSERT INTO ` + opts.TableName + ` AS t
  (key, value, isbinary, etag, expiredate)
VALUES
  ($1, $2, $3, 1, ` + opts.ExpireDateValue + `)
ON CONFLICT (key) DO UPDATE SET
  value = $2,
  isbinary = $3,
  updatedate = NOW(),
  etag = EXCLUDED.etag + 1,
  expiredate = ` + opts.ExpireDateValue +
					whereClause
			}

			// When an etag is provided do an update - no insert.
			return `
UPDATE ` + opts.TableName + `
SET
  value = $2,
  isbinary = $3,
  updatedate = NOW(),
  etag = etag + 1,
  expiredate = ` + opts.ExpireDateValue + `
WHERE
  key = $1
  AND etag = $4
  AND (expiredate IS NULL OR expiredate >= CURRENT_TIMESTAMP);`
		},
	})
}

func ensureTables(ctx context.Context, db pginterfaces.PGXPoolConn, opts postgresql.MigrateOptions) error {
	exists, err := tableExists(ctx, db, opts.StateTableName)
	if err != nil {
		return err
	}

	if !exists {
		opts.Logger.Info("Creating CockroachDB state table")
		_, err = db.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
  key text NOT NULL PRIMARY KEY,
  value jsonb NOT NULL,
  isbinary boolean NOT NULL,
  etag INT,
  insertdate TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  updatedate TIMESTAMP WITH TIME ZONE NULL,
  expiredate TIMESTAMP WITH TIME ZONE NULL,
	INDEX expiredate_idx (expiredate)
);`, opts.StateTableName))
		if err != nil {
			return err
		}
	}

	// If table was created before v1.11.
	_, err = db.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE %s ADD COLUMN IF NOT EXISTS expiredate TIMESTAMP WITH TIME ZONE NULL;`, opts.StateTableName))
	if err != nil {
		return err
	}
	_, err = db.Exec(ctx, fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS expiredate_idx ON %s (expiredate);`, opts.StateTableName))
	if err != nil {
		return err
	}

	exists, err = tableExists(ctx, db, opts.MetadataTableName)
	if err != nil {
		return err
	}

	if !exists {
		opts.Logger.Info("Creating CockroachDB metadata table")
		_, err = db.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
			key text NOT NULL PRIMARY KEY,
			value text NOT NULL
);`, opts.MetadataTableName))
		if err != nil {
			return err
		}
	}

	return nil
}

func tableExists(ctx context.Context, db pginterfaces.PGXPoolConn, tableName string) (bool, error) {
	exists := false
	err := db.QueryRow(ctx, "SELECT EXISTS (SELECT * FROM pg_tables where tablename = $1)", tableName).Scan(&exists)
	return exists, err
}
