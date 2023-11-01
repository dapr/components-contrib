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

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"

	internalsql "github.com/dapr/components-contrib/internal/component/sql"
	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

// ErrNoHost is returned by ResolveID when no host can be found.
var ErrNoHost = errors.New("no host found with the given ID")

// Internally-used error to indicate the registration was lost
var errRegistrationLost = errors.New("host registration lost")

type resolver struct {
	logger         logger.Logger
	metadata       sqliteMetadata
	db             *sql.DB
	gc             internalsql.GarbageCollector
	registrationID string
	closed         atomic.Bool
	closeCh        chan struct{}
	wg             sync.WaitGroup
}

// NewResolver creates a name resolver that is based on a SQLite DB.
func NewResolver(logger logger.Logger) nameresolution.Resolver {
	return &resolver{
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

// Init initializes the name resolver.
func (s *resolver) Init(ctx context.Context, md nameresolution.Metadata) error {
	if s.closed.Load() {
		return errors.New("component is closed")
	}

	err := s.metadata.InitWithMetadata(md)
	if err != nil {
		return err
	}

	connString, err := s.metadata.GetConnectionString(s.logger)
	if err != nil {
		// Already logged
		return err
	}

	// Show a warning if SQLite is configured with an in-memory DB
	if s.metadata.SqliteAuthMetadata.IsInMemoryDB() {
		s.logger.Warn("Configuring name resolution with an in-memory SQLite database. Service invocation across different apps will not work.")
	} else {
		s.logger.Infof("Configuring SQLite name resolution with path %s", connString[len("file:"):strings.Index(connString, "?")])
	}

	s.db, err = sql.Open("sqlite", connString)
	if err != nil {
		return fmt.Errorf("failed to create connection: %w", err)
	}

	// Performs migrations
	err = performMigrations(ctx, s.db, s.logger, migrationOptions{
		HostsTableName:    s.metadata.TableName,
		MetadataTableName: s.metadata.MetadataTableName,
	})
	if err != nil {
		return fmt.Errorf("failed to perform migrations: %w", err)
	}

	// Init the background GC
	err = s.initGC()
	if err != nil {
		return err
	}

	// Register the host and update in background
	err = s.registerHost(ctx)
	if err != nil {
		return err
	}

	s.wg.Add(1)
	go s.renewRegistration()

	return nil
}

func (s *resolver) initGC() (err error) {
	s.gc, err = internalsql.ScheduleGarbageCollector(internalsql.GCOptions{
		Logger: s.logger,
		UpdateLastCleanupQuery: func(arg any) (string, any) {
			return fmt.Sprintf(`INSERT INTO %s (key, value)
				VALUES ('nr-last-cleanup', CURRENT_TIMESTAMP)
				ON CONFLICT (key)
				DO UPDATE SET value = CURRENT_TIMESTAMP
					WHERE unixepoch(CURRENT_TIMESTAMP) - unixepoch(value)  > ?;`,
				s.metadata.MetadataTableName,
			), arg
		},
		DeleteExpiredValuesQuery: fmt.Sprintf(
			`DELETE FROM %s WHERE unixepoch(CURRENT_TIMESTAMP) - last_update < %d`,
			s.metadata.TableName,
			int(s.metadata.UpdateInterval.Seconds()),
		),
		CleanupInterval: s.metadata.CleanupInterval,
		DB:              internalsql.AdaptDatabaseSQLConn(s.db),
	})
	return err
}

// Registers the host
func (s *resolver) registerHost(ctx context.Context) error {
	// Get the registration ID
	u, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("failed to generate registration ID: %w", err)
	}
	s.registrationID = u.String()

	queryCtx, queryCancel := context.WithTimeout(ctx, s.metadata.Timeout)
	defer queryCancel()

	// There's a unique index on address
	// We use REPLACE to take over any previous registration for that address
	// TODO: Add support for namespacing. See https://github.com/dapr/components-contrib/issues/3179
	_, err = s.db.ExecContext(queryCtx,
		fmt.Sprintf("REPLACE INTO %s (registration_id, address, app_id, namespace, last_update) VALUES (?, ?, ?, ?, unixepoch(CURRENT_TIMESTAMP))", s.metadata.TableName),
		s.registrationID, s.metadata.GetAddress(), s.metadata.appID, "",
	)
	if err != nil {
		return fmt.Errorf("failed to register host: %w", err)
	}

	return nil
}

// In backgrounds, periodically renews the host's registration
// Should be invoked in a background goroutine
func (s *resolver) renewRegistration() {
	defer s.wg.Done()

	addr := s.metadata.GetAddress()

	// Update every UpdateInterval - Timeout (+ 1 second buffer)
	// This is because the record has to be updated every UpdateInterval, but we allow up to "timeout" for it to be performed
	d := s.metadata.UpdateInterval - s.metadata.Timeout - 1
	s.logger.Debugf("Started renewing host registration in background with interval %v", s.metadata.UpdateInterval)
	t := time.NewTicker(d)
	defer t.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		select {
		case <-s.closeCh:
			// Component is closing
			s.logger.Debug("Stopped renewing host registration: component is closing")
			return

		case <-t.C:
			// Renew on the ticker
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				err := s.doRenewRegistration(ctx, addr)
				if err != nil {
					// Log errors
					s.logger.Errorf("Failed to update host registration: %v", err)

					if errors.Is(err, errRegistrationLost) {
						// This means that our registration has been taken over by another host
						// It should never happen unless there's something really bad going on
						// Panicking here to force a restart of Dapr
						s.logger.Fatalf("Host registration lost")
					}
				}
			}()
		}
	}
}

func (s *resolver) doRenewRegistration(ctx context.Context, addr string) error {
	// We retry this query in case of database error, up to the timeout
	queryCtx, queryCancel := context.WithTimeout(ctx, s.metadata.Timeout)
	defer queryCancel()

	// We use string formatting here for the table name only
	//nolint:gosec
	query := fmt.Sprintf("UPDATE %s SET last_update = unixepoch(CURRENT_TIMESTAMP) WHERE registration_id = ? AND address = ?", s.metadata.TableName)

	b := backoff.WithContext(backoff.NewConstantBackOff(50*time.Millisecond), queryCtx)
	return backoff.Retry(func() error {
		res, err := s.db.ExecContext(queryCtx, query, s.registrationID, addr)
		if err != nil {
			return fmt.Errorf("database error: %w", err)
		}

		n, _ := res.RowsAffected()
		if n == 0 {
			// This is a permanent error
			return backoff.Permanent(errRegistrationLost)
		}

		return nil
	}, b)
}

// ResolveID resolves name to address.
func (s *resolver) ResolveID(ctx context.Context, req nameresolution.ResolveRequest) (addr string, err error) {
	queryCtx, queryCancel := context.WithTimeout(ctx, s.metadata.Timeout)
	defer queryCancel()

	//nolint:gosec
	q := fmt.Sprintf(
		// See: https://stackoverflow.com/a/24591696
		`SELECT address
		FROM %[1]s
		WHERE
			ROWID = (
				SELECT ROWID
				FROM %[1]s
				WHERE
					app_id = ?
					AND unixepoch(CURRENT_TIMESTAMP) - last_update < %[2]d
				ORDER BY RANDOM()
				LIMIT 1
			)`,
		s.metadata.TableName,
		int(s.metadata.UpdateInterval.Seconds()),
	)

	err = s.db.QueryRowContext(queryCtx, q, req.ID).Scan(&addr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrNoHost
		}
		return "", fmt.Errorf("failed to look up address: %w", err)
	}

	return addr, nil
}

// Removes the registration for the host
func (s *resolver) deregisterHost(ctx context.Context) error {
	if s.registrationID == "" {
		// We never registered
		return nil
	}

	queryCtx, queryCancel := context.WithTimeout(ctx, s.metadata.Timeout)
	defer queryCancel()

	res, err := s.db.ExecContext(queryCtx,
		fmt.Sprintf("DELETE FROM %s WHERE registration_id = ? AND address = ?", s.metadata.TableName),
		s.registrationID, s.metadata.GetAddress(),
	)
	if err != nil {
		return fmt.Errorf("failed to unregister host: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errors.New("failed to unregister host: no row deleted")
	}

	return nil
}

// Close implements io.Closer.
func (s *resolver) Close() (err error) {
	if !s.closed.CompareAndSwap(false, true) {
		s.wg.Wait()
		return nil
	}

	close(s.closeCh)
	s.wg.Wait()

	errs := make([]error, 0)

	if s.gc != nil {
		err = s.gc.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}

	if s.db != nil {
		err := s.deregisterHost(context.Background())
		if err != nil {
			errs = append(errs, err)
		}

		err = s.db.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
