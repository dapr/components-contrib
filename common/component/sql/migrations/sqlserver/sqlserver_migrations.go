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

package sqlservermigrations

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	commonsql "github.com/dapr/components-contrib/common/component/sql"
	"github.com/dapr/kit/logger"
)

// Migrations performs migrations for the database schema
type Migrations struct {
	DB                *sql.DB
	Logger            logger.Logger
	Schema            string
	MetadataTableName string
	MetadataKey       string

	tableName string
}

// Perform the required migrations
func (m *Migrations) Perform(ctx context.Context, migrationFns []commonsql.MigrationFn) (err error) {
	// Setting a short-hand since it's going to be used a lot
	m.tableName = fmt.Sprintf("[%s].[%s]", m.Schema, m.MetadataTableName)

	// Ensure the metadata table exists
	err = m.ensureMetadataTable(ctx)
	if err != nil {
		return fmt.Errorf("failed to ensure metadata table exists: %w", err)
	}

	// In order to acquire a row-level lock, we need to have a row in the metadata table
	// So, we're going to write a row in there (not using a transaction, as that causes a table-level lock to be created), ignoring duplicates
	const lockKey = "lock"
	m.Logger.Debugf("Ensuring lock row '%s' exists in metadata table", lockKey)
	queryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	_, err = m.DB.ExecContext(queryCtx, fmt.Sprintf(`
INSERT INTO %[1]s
  ([Key], [Value])
SELECT @Key, @Value
WHERE NOT EXISTS (
  SELECT 1
  FROM %[1]s
  WHERE [Key] = @Key
);
`, m.tableName), sql.Named("Key", lockKey), sql.Named("Value", lockKey))
	cancel()
	if err != nil {
		return fmt.Errorf("failed to ensure lock row '%s' exists: %w", lockKey, err)
	}

	// Now, let's use a transaction on a row in the metadata table as a lock
	m.Logger.Debug("Starting transaction pre-migration")
	tx, err := m.DB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Always rollback the transaction at the end to release the lock, since the value doesn't really matter
	defer func() {
		m.Logger.Debug("Releasing migration lock")
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			// Panicking here, as this forcibly closes the session and thus ensures we are not leaving locks hanging around
			m.Logger.Fatalf("Failed to roll back transaction: %v", rollbackErr)
		}
	}()

	// Now, perform a SELECT with FOR UPDATE to lock the row used for locking, and only that row
	// We use a long timeout here as this query may block
	m.Logger.Debug("Acquiring migration lock")
	queryCtx, cancel = context.WithTimeout(ctx, time.Minute)
	var lock string
	//nolint:gosec
	q := fmt.Sprintf(`
SELECT [Value]
FROM %s
WITH (XLOCK, ROWLOCK)
WHERE [key] = @Key
`, m.tableName)
	err = tx.QueryRowContext(queryCtx, q, sql.Named("Key", lockKey)).Scan(&lock)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to acquire migration lock (row-level lock on key '%s'): %w", lockKey, err)
	}
	m.Logger.Debug("Migration lock acquired")

	// Perform the migrations
	// Here we pass the database connection and not the transaction, since the transaction is only used to acquire the lock
	err = commonsql.Migrate(ctx, commonsql.AdaptDatabaseSQLConn(m.DB), commonsql.MigrationOptions{
		Logger: m.Logger,
		// Yes, we are using fmt.Sprintf for adding a value in a query.
		// This comes from a constant hardcoded at development-time, and cannot be influenced by users. So, no risk of SQL injections here.
		GetVersionQuery: fmt.Sprintf(`SELECT [Value] FROM %s WHERE [Key] = '%s'`, m.tableName, m.MetadataKey),
		UpdateVersionQuery: func(version string) (string, any) {
			return fmt.Sprintf(`
MERGE
  %[1]s WITH (HOLDLOCK) AS t
USING (SELECT '%[2]s' AS [Key]) AS s
  ON [t].[Key] = [s].[Key]
WHEN MATCHED THEN
  UPDATE SET [Value] = @Value
WHEN NOT MATCHED THEN
  INSERT ([Key], [Value]) VALUES ('%[2]s', @Value)
;
`,
				m.tableName, m.MetadataKey,
			), sql.Named("Value", version)
		},
		Migrations: migrationFns,
	})
	if err != nil {
		return err
	}

	return nil
}

func (m Migrations) ensureMetadataTable(ctx context.Context) error {
	m.Logger.Infof("Ensuring metadata table '%s' exists", m.tableName)
	_, err := m.DB.ExecContext(ctx, fmt.Sprintf(`
IF OBJECT_ID('%[1]s', 'U') IS NULL
CREATE TABLE %[1]s (
  [Key] VARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY,
  [Value] VARCHAR(max) COLLATE Latin1_General_100_BIN2 NOT NULL
)`,
		m.tableName,
	))
	if err != nil {
		return fmt.Errorf("failed to create metadata table: %w", err)
	}
	return nil
}
