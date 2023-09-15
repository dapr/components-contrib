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

package sql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// AdaptDatabaseSQLConn returns a databaseConn based on a database/sql connection.
//
// Note: when using transactions with database/sql, the context bassed to Begin impacts the entire transaction.
// Canceling the context automatically rolls back the transaction.
func AdaptDatabaseSQLConn(db DatabaseSQLConn) DatabaseConn {
	return &DatabaseSQLAdapter{db}
}

// AdaptPgxConn returns a databaseConn based on a pgx connection.
//
// Note: when using transactions with pgx, the context bassed to Begin impacts the creation of the transaction only.
func AdaptPgxConn(db PgxConn) DatabaseConn {
	return &PgxAdapter{db}
}

// DatabaseSQLConn is the interface for connections that use database/sql.
type DatabaseSQLConn interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// PgxConn is the interface for connections that use pgx.
type PgxConn interface {
	Begin(context.Context) (pgx.Tx, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

// DatabaseConn is the interface matched by all adapters.
type DatabaseConn interface {
	Begin(context.Context) (databaseConnTx, error)
	QueryRow(context.Context, string, ...any) databaseConnRow
	Exec(context.Context, string, ...any) (int64, error)
	IsNoRowsError(err error) bool
}

type databaseConnRow interface {
	Scan(...any) error
}

type databaseConnTx interface {
	Commit(context.Context) error
	Rollback(context.Context) error
	QueryRow(context.Context, string, ...any) databaseConnRow
	Exec(context.Context, string, ...any) (int64, error)
}

// DatabaseSQLAdapter is an adapter for database/sql connections.
type DatabaseSQLAdapter struct {
	conn DatabaseSQLConn
}

func (sqla *DatabaseSQLAdapter) Begin(ctx context.Context) (databaseConnTx, error) {
	tx, err := sqla.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &databaseSQLTxAdapter{tx}, nil
}

func (sqla *DatabaseSQLAdapter) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := sqla.conn.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (sqla *DatabaseSQLAdapter) QueryRow(ctx context.Context, query string, args ...any) databaseConnRow {
	return sqla.conn.QueryRowContext(ctx, query, args...)
}

func (sqla *DatabaseSQLAdapter) IsNoRowsError(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

type databaseSQLTxAdapter struct {
	tx *sql.Tx
}

func (sqltx *databaseSQLTxAdapter) Rollback(_ context.Context) error {
	return sqltx.tx.Rollback()
}

func (sqltx *databaseSQLTxAdapter) Commit(_ context.Context) error {
	return sqltx.tx.Commit()
}

func (sqltx *databaseSQLTxAdapter) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := sqltx.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (sqltx *databaseSQLTxAdapter) QueryRow(ctx context.Context, query string, args ...any) databaseConnRow {
	return sqltx.tx.QueryRowContext(ctx, query, args...)
}

// PgxAdapter is an adapter for pgx connections.
type PgxAdapter struct {
	conn PgxConn
}

func (pga *PgxAdapter) Begin(ctx context.Context) (databaseConnTx, error) {
	tx, err := pga.conn.Begin(ctx)
	if err != nil {
		return nil, err
	}

	return &pgxTxAdapter{tx}, nil
}

func (pga *PgxAdapter) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := pga.conn.Exec(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected(), nil
}

func (pga *PgxAdapter) QueryRow(ctx context.Context, query string, args ...any) databaseConnRow {
	return pga.conn.QueryRow(ctx, query, args...)
}

func (pga *PgxAdapter) IsNoRowsError(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

type pgxTxAdapter struct {
	tx pgx.Tx
}

func (pgtx *pgxTxAdapter) Rollback(ctx context.Context) error {
	return pgtx.tx.Rollback(ctx)
}

func (pgtx *pgxTxAdapter) Commit(ctx context.Context) error {
	return pgtx.tx.Commit(ctx)
}

func (pgtx *pgxTxAdapter) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := pgtx.tx.Exec(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected(), nil
}

func (pgtx *pgxTxAdapter) QueryRow(ctx context.Context, query string, args ...any) databaseConnRow {
	return pgtx.tx.QueryRow(ctx, query, args...)
}
