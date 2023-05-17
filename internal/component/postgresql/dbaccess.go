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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dapr/components-contrib/state"
)

// dbAccess is a private interface which enables unit testing of PostgreSQL.
type dbAccess interface {
	Init(ctx context.Context, metadata state.Metadata) error
	Set(ctx context.Context, req *state.SetRequest) error
	Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error)
	BulkGet(ctx context.Context, req []state.GetRequest) ([]state.BulkGetResponse, error)
	Delete(ctx context.Context, req *state.DeleteRequest) error
	ExecuteMulti(ctx context.Context, req *state.TransactionalStateRequest) error
	Query(ctx context.Context, req *state.QueryRequest) (*state.QueryResponse, error)
	Close() error // io.Closer
}

// Interface that contains methods for querying.
// Applies to *pgx.Conn, *pgxpool.Pool, and pgx.Tx
type dbquerier interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
}
