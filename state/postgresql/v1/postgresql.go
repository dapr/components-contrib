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
	postgresql "github.com/dapr/components-contrib/common/component/postgresql/v1"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

// NewPostgreSQLStateStore creates a new instance of PostgreSQL state store.
func NewPostgreSQLStateStore(logger logger.Logger) state.Store {
	return postgresql.NewPostgreSQLQueryStateStore(logger, postgresql.Options{
		ETagColumn:    "xmin",
		EnableAzureAD: true,
		EnableAWSIAM:  true,
		MigrateFn:     performMigrations,
		SetQueryFn: func(req *state.SetRequest, opts postgresql.SetQueryOptions) string {
			// Sprintf is required for table name because the driver does not substitute parameters for table names.
			if !req.HasETag() {
				// We do an upsert in both cases, even when concurrency is first-write, because the row may exist but be expired (and not yet garbage collected)
				// The difference is that with concurrency as first-write, we'll update the row only if it's expired
				var whereClause string
				if req.Options.Concurrency == state.FirstWrite {
					whereClause = " WHERE (t.expiredate IS NOT NULL AND t.expiredate < CURRENT_TIMESTAMP)"
				}

				return `INSERT INTO ` + opts.TableName + ` AS t
					(key, value, isbinary, expiredate)
				VALUES
					($1, $2, $3, ` + opts.ExpireDateValue + `)
				ON CONFLICT (key)
				DO UPDATE SET
					value = excluded.value,
					isbinary = excluded.isBinary,
					updatedate = CURRENT_TIMESTAMP,
					expiredate = ` + opts.ExpireDateValue +
					whereClause
			}

			return `UPDATE ` + opts.TableName + `
			SET
				value = $2,
				isbinary = $3,
				updatedate = CURRENT_TIMESTAMP,
				expiredate = ` + opts.ExpireDateValue + `
			WHERE
				key = $1
				AND xmin = $4
				AND (expiredate IS NULL OR expiredate > CURRENT_TIMESTAMP)`
		},
	})
}
