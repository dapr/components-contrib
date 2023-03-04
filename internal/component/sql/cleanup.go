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
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dapr/kit/logger"
)

type GarbageCollector interface {
	CleanupExpired() error
	io.Closer
}

type GCOptions struct {
	Logger                   logger.Logger
	UpdateLastCleanupQuery   string
	DeleteExpiredValuesQuery string
	CleanupInterval          time.Duration
	SQLDB                    SQLDB
	SQLDBWithContext         SQLDBWithContext
}

type SQLDB interface {
	Begin(context.Context) (pgx.Tx, error)
}

type SQLDBWithContext interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

type gc struct {
	log                      logger.Logger
	updateLastCleanupQuery   string
	deleteExpiredValuesQuery string
	cleanupInterval          time.Duration
	db                       SQLDB
	dbWC                     SQLDBWithContext

	closed   atomic.Bool
	closedCh chan struct{}
	wg       sync.WaitGroup
}

func ScheduleGarbageCollector(opts GCOptions) (GarbageCollector, error) {
	if opts.CleanupInterval <= 0 {
		return new(gcNoOp), nil
	}

	if opts.SQLDB == nil && opts.SQLDBWithContext == nil {
		return nil, fmt.Errorf("either SQLDB or SQLDBWithContext must be provided")
	}
	if opts.SQLDB != nil && opts.SQLDBWithContext != nil {
		return nil, fmt.Errorf("only one of SQLDB or SQLDBWithContext must be provided")
	}

	gc := &gc{
		log:                      opts.Logger,
		updateLastCleanupQuery:   opts.UpdateLastCleanupQuery,
		deleteExpiredValuesQuery: opts.DeleteExpiredValuesQuery,
		cleanupInterval:          opts.CleanupInterval,
		db:                       opts.SQLDB,
		dbWC:                     opts.SQLDBWithContext,
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
	var (
		tx   pgx.Tx
		txwc *sql.Tx
		err  error
	)

	// Deletion can take a long time to complete so we have a long background
	// context. Still catch closing of the GC.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		defer cancel()
		select {
		case <-ctx.Done():
		case <-g.closedCh:
		}
	}()

	if g.db != nil {
		tx, err = g.db.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}
		defer tx.Rollback(ctx)
	} else {
		txwc, err = g.dbWC.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}
		defer txwc.Rollback()
	}

	// Check if the last iteration was too recent
	// This performs an atomic operation, so allows coordination with other daprd processes too
	// We do this before beginning the transaction
	canContinue, err := g.updateLastCleanup(ctx, tx, txwc)
	if err != nil {
		return fmt.Errorf("failed to read last cleanup time from database: %w", err)
	}
	if !canContinue {
		g.log.Debug("Last cleanup was performed too recently")
		return nil
	}

	var rowsAffected int64
	if tx != nil {
		res, err := tx.Exec(ctx, g.deleteExpiredValuesQuery)
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}
		rowsAffected = res.RowsAffected()
	} else {
		res, err := txwc.ExecContext(ctx, g.deleteExpiredValuesQuery)
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

// updateLastCleanup sets the 'last-cleanup' value only if it's less than
// cleanupInterval.
// Returns true if the row was updated, which means that the cleanup can proceed.
func (g *gc) updateLastCleanup(ctx context.Context, tx pgx.Tx, txwc *sql.Tx) (bool, error) {
	var n int64
	// Subtract 100ms for some buffer
	if tx != nil {
		res, err := tx.Exec(ctx, g.updateLastCleanupQuery, g.cleanupInterval.Milliseconds()-100)
		if err != nil {
			return false, fmt.Errorf("error updating last cleanup time: %w", err)
		}
		n = res.RowsAffected()

	} else {
		res, err := txwc.ExecContext(ctx, g.updateLastCleanupQuery, g.cleanupInterval.Milliseconds()-100)
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
