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
	"github.com/dapr/components-contrib/internal/component/postgresql"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

// NewPostgreSQLStateStore creates a new instance of PostgreSQL state store.
func NewPostgreSQLStateStore(logger logger.Logger) state.Store {
	return postgresql.NewPostgreSQLStateStore(logger, postgresql.Options{
		ETagColumn: "xmin",
		MigrateFn:  performMigration,
		SetQueryFn: func(req *state.SetRequest, opts postgresql.SetQueryOptions) string {
			// Sprintf is required for table name because sql.DB does not
			// substitute parameters for table names.
			// Other parameters use sql.DB parameter substitution.
			if req.ETag == nil || *req.ETag == "" {
				if req.Options.Concurrency == state.FirstWrite {
					return `INSERT INTO ` + opts.TableName + `
					(key, value, isbinary, expiredate)
				VALUES
					($1, $2, $3, ` + opts.ExpireDateValue + `);`
				}

				return `INSERT INTO ` + opts.TableName + `
					(key, value, isbinary, expiredate)
				VALUES
					($1, $2, $3, ` + opts.ExpireDateValue + `)
				ON CONFLICT (key)
				DO UPDATE SET
					value = $2,
					isbinary = $3,
					updatedate = CURRENT_TIMESTAMP,
					expiredate = ` + opts.ExpireDateValue + `;`
			}

			return `UPDATE ` + opts.TableName + `
			SET
				value = $2,
				isbinary = $3,
				updatedate = CURRENT_TIMESTAMP,
				expiredate = ` + opts.ExpireDateValue + `
			WHERE
				key = $1
				AND xmin = $4;`
		},
	})
}
