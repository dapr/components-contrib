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
	"fmt"
	"strconv"
	"time"

	"github.com/dapr/kit/logger"
)

// MigrationOptions contains options for the Migrate function.
type MigrationOptions struct {
	// Logger
	Logger logger.Logger

	// List of migrations to execute.
	// Each item is a function that receives a context and the database connection, and can execute queries.
	Migrations []MigrationFn

	// EnsureMetadataTable ensures that the metadata table exists.
	EnsureMetadataTable func(ctx context.Context) error

	// GetVersionQuery is the query to execute to load the latest migration version.
	GetVersionQuery string

	// UpdateVersionQuery is a function that returns the query to update the migration version, and the arg.
	UpdateVersionQuery func(version string) (string, any)
}

type (
	MigrationFn         = func(ctx context.Context) error
	MigrationTeardownFn = func() error
)

// Migrate performs database migrations.
func Migrate(ctx context.Context, db DatabaseConn, opts MigrationOptions) error {
	opts.Logger.Debug("Migrate: start")

	// Ensure that the metadata table exists
	opts.Logger.Debug("Migrate: ensure metadata table exists")
	err := opts.EnsureMetadataTable(ctx)
	if err != nil {
		return fmt.Errorf("failed to ensure metadata table exists: %w", err)
	}

	// Select the migration level
	opts.Logger.Debug("Migrate: load current migration level")
	var (
		migrationLevelStr string
		migrationLevel    int
	)
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = db.QueryRow(queryCtx, opts.GetVersionQuery).Scan(&migrationLevelStr)
	cancel()
	if db.IsNoRowsError(err) {
		// If there's no row...
		migrationLevel = 0
	} else if err != nil {
		return fmt.Errorf("failed to read migration level: %w", err)
	} else {
		migrationLevel, err = strconv.Atoi(migrationLevelStr)
		if err != nil || migrationLevel < 0 {
			return fmt.Errorf("invalid migration level found in metadata table: %s", migrationLevelStr)
		}
	}
	opts.Logger.Debug("Migrate: current migration level: %d", migrationLevel)

	// Perform the migrations
	for i := migrationLevel; i < len(opts.Migrations); i++ {
		opts.Logger.Infof("Performing migration %d", i+1)
		err = opts.Migrations[i](ctx)
		if err != nil {
			return fmt.Errorf("failed to perform migration %d: %w", i, err)
		}

		query, arg := opts.UpdateVersionQuery(strconv.Itoa(i + 1))
		queryCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
		_, err = db.Exec(queryCtx, query, arg)
		cancel()
		if err != nil {
			return fmt.Errorf("failed to update migration level in metadata table: %w", err)
		}
	}

	return nil
}
