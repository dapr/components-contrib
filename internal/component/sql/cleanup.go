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
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dapr/kit/logger"
)

type GarbageCollector interface {
	CleanupExpired() error
	io.Closer
}

type GCOptions struct {
	Logger logger.Logger

	// Query that must atomically update the "last cleanup time" in the metadata table, but only if the garbage collector hasn't run already.
	// The caller will check the nuber of affected rows. If zero, it assumes that the GC has ran too recently, and will not proceed to delete expired records.
	// The query receives one parameter that is the last cleanup interval, in milliseconds.
	UpdateLastCleanupQuery string
	// Query that performs the cleanup of all expired rows.
	DeleteExpiredValuesQuery string

	// Interval to perfm the cleanup.
	CleanupInterval time.Duration

	// Database connection when using pgx.
	DBPgx PgxConn
	// Database connection when using database/sql.
	DBSql DatabaseSQLConn
}

// Interface for connections that use pgx.
type PgxConn interface {
	Begin(context.Context) (pgx.Tx, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

// Interface for connections that use database/sql.
type DatabaseSQLConn interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type gc struct {
	log                      logger.Logger
	updateLastCleanupQuery   string
	deleteExpiredValuesQuery string
	cleanupInterval          time.Duration
	dbPgx                    PgxConn
	dbSQL                    DatabaseSQLConn

	closed   atomic.Bool
	closedCh chan struct{}
	wg       sync.WaitGroup
}

func ScheduleGarbageCollector(opts GCOptions) (GarbageCollector, error) {
	if opts.CleanupInterval <= 0 {
		return new(gcNoOp), nil
	}

	if opts.DBPgx == nil && opts.DBSql == nil {
		return nil, errors.New("either DBPgx or DBSql must be provided")
	}
	if opts.DBPgx != nil && opts.DBSql != nil {
		return nil, errors.New("only one of DBPgx or DBSql must be provided")
	}

	gc := &gc{
		log:                      opts.Logger,
		updateLastCleanupQuery:   opts.UpdateLastCleanupQuery,
		deleteExpiredValuesQuery: opts.DeleteExpiredValuesQuery,
		cleanupInterval:          opts.CleanupInterval,
		dbPgx:                    opts.DBPgx,
		dbSQL:                    opts.DBSql,
		closedCh:                 make(chan struct{}),
	}

	gc.wg.Add(1)
	go func() {
		defer gc.wg.Done()
		gc.scheduleCleanup()
	}()

	return gc, nil
}

func (g *gc) scheduleCleanup() {
	g.log.Infof("Schedule expired data clean up every %v", g.cleanupInterval)

	ticker := time.NewTicker(g.cleanupInterval)
	defer ticker.Stop()

	var err error
	for {
		select {
		case <-ticker.C:
			err = g.CleanupExpired()
			if err != nil {
				g.log.Errorf("Error removing expired data: %v", err)
			}
		case <-g.closedCh:
			g.log.Debug("Stopping background cleanup of expired data")
			return
		}
	}
}

// Exposed for testing.
func (g *gc) CleanupExpired() error {
	// Deletion can take a long time to complete so we have a long background context. Still catch closing of the GC.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	g.wg.Add(1)
	go func() {
		// Wait for context cancellation or closing
		select {
		case <-ctx.Done():
		case <-g.closedCh:
		}
		cancel()
		g.wg.Done()
	}()

	// Check if the last iteration was too recent
	// This performs an atomic operation, so allows coordination with other daprd processes too
	// We do this before beginning the transaction
	canContinue, err := g.updateLastCleanup(ctx)
	if err != nil {
		return fmt.Errorf("failed to read last cleanup time from database: %w", err)
	}
	if !canContinue {
		g.log.Debug("Last cleanup was performed too recently")
		return nil
	}

	var (
		tx   pgx.Tx
		txwc *sql.Tx
	)

	if g.dbPgx != nil {
		tx, err = g.dbPgx.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}
		defer tx.Rollback(ctx)
	} else {
		txwc, err = g.dbSQL.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}
		defer txwc.Rollback()
	}

	var rowsAffected int64
	if tx != nil {
		var res pgconn.CommandTag
		res, err = tx.Exec(ctx, g.deleteExpiredValuesQuery)
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}
		rowsAffected = res.RowsAffected()
	} else {
		var res sql.Result
		res, err = txwc.ExecContext(ctx, g.deleteExpiredValuesQuery)
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}
		rowsAffected, err = res.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		}
	}

	// Commit
	if tx != nil {
		err = tx.Commit(ctx)
	} else {
		err = txwc.Commit()
	}
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	g.log.Infof("Removed %d expired rows", rowsAffected)
	return nil
}

// updateLastCleanup sets the 'last-cleanup' value only if it's less than cleanupInterval.
// Returns true if the row was updated, which means that the cleanup can proceed.
func (g *gc) updateLastCleanup(ctx context.Context) (bool, error) {
	var n int64
	// Subtract 100ms for some buffer
	if g.dbPgx != nil {
		res, err := g.dbPgx.Exec(ctx, g.updateLastCleanupQuery, g.cleanupInterval.Milliseconds()-100)
		if err != nil {
			return false, fmt.Errorf("error updating last cleanup time: %w", err)
		}
		n = res.RowsAffected()
	} else {
		res, err := g.dbSQL.ExecContext(ctx, g.updateLastCleanupQuery, g.cleanupInterval.Milliseconds()-100)
		if err != nil {
			return false, fmt.Errorf("error updating last cleanup time: %w", err)
		}

		n, err = res.RowsAffected()
		if err != nil {
			return false, fmt.Errorf("failed to retrieve affected row count: %w", err)
		}
	}

	return n > 0, nil
}

func (g *gc) Close() error {
	defer g.wg.Wait()

	if g.closed.CompareAndSwap(false, true) {
		close(g.closedCh)
	}

	return nil
}

type gcNoOp struct{}

func (g *gcNoOp) CleanupExpired() error { return nil }
func (g *gcNoOp) Close() error          { return nil }
