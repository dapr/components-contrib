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
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

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
	// The function must return both the query and the argument.
	UpdateLastCleanupQuery func(arg any) (string, any)

	// Query that performs the cleanup of all expired rows.
	DeleteExpiredValuesQuery string

	// Interval to perfm the cleanup.
	CleanupInterval time.Duration

	// Database connection.
	// Must be adapted using AdaptDatabaseSQLConn or AdaptPgxConn.
	DB DatabaseConn
}

type gc struct {
	log                      logger.Logger
	updateLastCleanupQuery   func(arg any) (string, any)
	deleteExpiredValuesQuery string
	cleanupInterval          time.Duration
	db                       DatabaseConn

	closed   atomic.Bool
	closedCh chan struct{}
	wg       sync.WaitGroup
}

func ScheduleGarbageCollector(opts GCOptions) (GarbageCollector, error) {
	if opts.CleanupInterval <= 0 {
		return new(gcNoOp), nil
	}

	if opts.DB == nil {
		return nil, errors.New("property DB must be provided")
	}

	gc := &gc{
		log:                      opts.Logger,
		updateLastCleanupQuery:   opts.UpdateLastCleanupQuery,
		deleteExpiredValuesQuery: opts.DeleteExpiredValuesQuery,
		cleanupInterval:          opts.CleanupInterval,
		db:                       opts.DB,
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

	tx, err := g.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	rowsAffected, err := tx.Exec(ctx, g.deleteExpiredValuesQuery)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	// Commit
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	g.log.Infof("Removed %d expired rows", rowsAffected)
	return nil
}

// updateLastCleanup sets the 'last-cleanup' value only if it's less than cleanupInterval.
// Returns true if the row was updated, which means that the cleanup can proceed.
func (g *gc) updateLastCleanup(ctx context.Context) (bool, error) {
	// Query parameter: interval in ms
	// Subtract 100ms for some buffer
	query, param := g.updateLastCleanupQuery(g.cleanupInterval.Milliseconds() - 100)

	n, err := g.db.Exec(ctx, query, param)
	if err != nil {
		return false, fmt.Errorf("error updating last cleanup time: %w", err)
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
