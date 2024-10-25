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

package sqlserver

import (
	"context"
	"database/sql"
	"fmt"
)

type migrator interface {
	executeMigrations(context.Context) (migrationResult, error)
}

type migration struct {
	metadata *sqlServerMetadata
}

type migrationResult struct {
	itemRefTableTypeName     string
	upsertProcName           string
	upsertProcFullName       string
	pkColumnType             string
	getCommand               string
	deleteWithETagCommand    string
	deleteWithoutETagCommand string
}

func newMigration(metadata *sqlServerMetadata) migrator {
	return &migration{
		metadata: metadata,
	}
}

func (m *migration) newMigrationResult() migrationResult {
	r := migrationResult{
		itemRefTableTypeName:     fmt.Sprintf("[%s].%s_Table", m.metadata.SchemaName, m.metadata.TableName),
		upsertProcName:           "sp_Upsert_v5_" + m.metadata.TableName,
		getCommand:               fmt.Sprintf("SELECT [Data], [RowVersion], [ExpireDate] FROM [%s].[%s] WHERE [Key] = @Key AND ([ExpireDate] IS NULL OR [ExpireDate] > GETDATE())", m.metadata.SchemaName, m.metadata.TableName),
		deleteWithETagCommand:    fmt.Sprintf(`DELETE [%s].[%s] WHERE [Key]=@Key AND [RowVersion]=@RowVersion`, m.metadata.SchemaName, m.metadata.TableName),
		deleteWithoutETagCommand: fmt.Sprintf(`DELETE [%s].[%s] WHERE [Key]=@Key`, m.metadata.SchemaName, m.metadata.TableName),
	}

	r.upsertProcFullName = fmt.Sprintf("[%s].%s", m.metadata.SchemaName, r.upsertProcName)

	//nolint:exhaustive
	switch m.metadata.keyTypeParsed {
	case StringKeyType:
		r.pkColumnType = fmt.Sprintf("NVARCHAR(%d)", m.metadata.keyLengthParsed)

	case UUIDKeyType:
		r.pkColumnType = "uniqueidentifier"

	case IntegerKeyType:
		r.pkColumnType = "int"
	}

	return r
}

/* #nosec. */
func (m *migration) executeMigrations(ctx context.Context) (migrationResult, error) {
	r := m.newMigrationResult()

	conn, hasDatabase, err := m.metadata.GetConnector(false)
	if err != nil {
		return r, err
	}
	db := sql.OpenDB(conn)

	// If the user provides a database in the connection string do not attempt
	// to create the database. This work as the component did before adding the
	// support to create the db.
	if hasDatabase {
		// Schedule close of connection
		defer db.Close()
	} else {
		err = m.ensureDatabaseExists(ctx, db)
		if err != nil {
			return r, fmt.Errorf("failed to create database: %w", err)
		}

		// Close the existing connection
		db.Close()

		// Re connect with a database-specific connection
		conn, _, err = m.metadata.GetConnector(true)
		if err != nil {
			return r, err
		}
		db = sql.OpenDB(conn)

		// Schedule close of new connection
		defer db.Close()
	}

	err = m.ensureSchemaExists(ctx, db)
	if err != nil {
		return r, fmt.Errorf("failed to create db schema: %w", err)
	}

	err = m.ensureTableExists(ctx, db, r)
	if err != nil {
		return r, fmt.Errorf("failed to create db table: %w", err)
	}

	err = m.ensureStoredProcedureExists(ctx, db, r)
	if err != nil {
		return r, fmt.Errorf("failed to create stored procedures: %w", err)
	}

	for _, ix := range m.metadata.indexedPropertiesParsed {
		err = m.ensureIndexedPropertyExists(ctx, db, ix)
		if err != nil {
			return r, err
		}
	}

	return r, nil
}

func runCommand(ctx context.Context, db *sql.DB, tsql string) error {
	if _, err := db.ExecContext(ctx, tsql); err != nil {
		return err
	}

	return nil
}

/* #nosec. */
func (m *migration) ensureIndexedPropertyExists(ctx context.Context, db *sql.DB, ix IndexedProperty) error {
	indexName := "IX_" + ix.ColumnName

	tsql := fmt.Sprintf(`
	IF (NOT EXISTS(SELECT object_id
				   FROM sys.indexes
				   WHERE object_id = OBJECT_ID('[%s].%s')
    					AND name='%s'))
		CREATE INDEX %s ON [%s].[%s]([%s])`,
		m.metadata.SchemaName,
		m.metadata.TableName,
		indexName,
		indexName,
		m.metadata.SchemaName,
		m.metadata.TableName,
		ix.ColumnName)

	return runCommand(ctx, db, tsql)
}

/* #nosec. */
func (m *migration) ensureDatabaseExists(ctx context.Context, db *sql.DB) error {
	tsql := fmt.Sprintf(`
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'%s')
	CREATE DATABASE [%s]`,
		m.metadata.DatabaseName, m.metadata.DatabaseName)

	return runCommand(ctx, db, tsql)
}

/* #nosec. */
func (m *migration) ensureSchemaExists(ctx context.Context, db *sql.DB) error {
	tsql := fmt.Sprintf(`
	IF NOT EXISTS(SELECT * FROM sys.schemas WHERE name = N'%s')
		EXEC('CREATE SCHEMA [%s]')`,
		m.metadata.SchemaName, m.metadata.SchemaName)

	return runCommand(ctx, db, tsql)
}

/* #nosec. */
func (m *migration) ensureTableExists(ctx context.Context, db *sql.DB, r migrationResult) error {
	tsql := fmt.Sprintf(`
	IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s')
    	CREATE TABLE [%s].[%s] (
			[Key] 			%s CONSTRAINT PK_%s PRIMARY KEY,
			[Data]			NVARCHAR(MAX) NOT NULL,
			[InsertDate] 	DateTime2 NOT NULL DEFAULT(GETDATE()),
			[UpdateDate] 	DateTime2 NULL,
			[ExpireDate] 	DateTime2 NULL,`,
		m.metadata.SchemaName, m.metadata.TableName, m.metadata.SchemaName, m.metadata.TableName, r.pkColumnType, m.metadata.TableName)

	for _, prop := range m.metadata.indexedPropertiesParsed {
		if prop.Type != "" {
			tsql += fmt.Sprintf("\n		[%s] AS CONVERT(%s, JSON_VALUE(Data, '$.%s')) PERSISTED,", prop.ColumnName, prop.Type, prop.Property)
		} else {
			tsql += fmt.Sprintf("\n		[%s] AS JSON_VALUE(Data, '$.%s') PERSISTED,", prop.ColumnName, prop.Property)
		}
	}

	tsql += `
		[RowVersion] 	ROWVERSION NOT NULL)
	`

	if err := runCommand(ctx, db, tsql); err != nil {
		return err
	}

	// If table was created before v1.11
	tsql = fmt.Sprintf(`IF NOT EXISTS (SELECT column_name
    FROM INFORMATION_SCHEMA.COLUMNS
	  WHERE TABLE_SCHEMA = '%[1]s' AND TABLE_NAME = '%[2]s'
	   AND COLUMN_NAME = 'ExpireDate')
  ALTER TABLE [%[1]s].[%[2]s] ADD [ExpireDate] DateTime2 NULL`, m.metadata.SchemaName, m.metadata.TableName)
	if err := runCommand(ctx, db, tsql); err != nil {
		return fmt.Errorf("failed to ensure ExpireDate column: %w", err)
	}

	tsql = fmt.Sprintf(`
	IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%[1]s' AND TABLE_NAME = '%[2]s')
			CREATE TABLE [%[1]s].[%[2]s] (
			[Key] 			%[3]s CONSTRAINT PK_%[4]s PRIMARY KEY,
			[Value]			NVARCHAR(MAX) NOT NULL
		)`, m.metadata.SchemaName, m.metadata.MetadataTableName, r.pkColumnType, m.metadata.MetadataTableName)
	if err := runCommand(ctx, db, tsql); err != nil {
		return err
	}

	return nil
}

/* #nosec. */
func (m *migration) ensureTypeExists(ctx context.Context, db *sql.DB, mr migrationResult) error {
	tsql := fmt.Sprintf(`
	IF type_id('[%s].%s_Table') IS NULL
		CREATE TYPE [%s].%s_Table AS TABLE
		(
			[Key]           		%s NOT NULL,
			[RowVersion]			BINARY(8)
		)
	`, m.metadata.SchemaName, m.metadata.TableName, m.metadata.SchemaName, m.metadata.TableName, mr.pkColumnType)

	return runCommand(ctx, db, tsql)
}

func (m *migration) ensureStoredProcedureExists(ctx context.Context, db *sql.DB, mr migrationResult) error {
	err := m.ensureTypeExists(ctx, db, mr)
	if err != nil {
		return err
	}

	err = m.ensureUpsertStoredProcedureExists(ctx, db, mr)
	if err != nil {
		return err
	}

	return nil
}

/* #nosec. */
func (m *migration) createStoredProcedureIfNotExists(ctx context.Context, db *sql.DB, name string, escapedDefinition string) error {
	tsql := fmt.Sprintf(`
	IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[%s].[%s]') AND type in (N'P', N'PC'))
	BEGIN
		execute ('%s')
	END`,
		m.metadata.SchemaName,
		name,
		escapedDefinition)

	return runCommand(ctx, db, tsql)
}

/* #nosec. */
//nolint:dupword
func (m *migration) ensureUpsertStoredProcedureExists(ctx context.Context, db *sql.DB, mr migrationResult) error {
	tsql := fmt.Sprintf(`
			CREATE PROCEDURE %[1]s (
				@Key 			%[2]s,
				@Data 			NVARCHAR(MAX),
				@TTL INT,
				@RowVersion	BINARY(8),
				@FirstWrite	BIT
			) AS
			BEGIN
				IF (@FirstWrite=1)
					BEGIN
						IF (@RowVersion IS NOT NULL)
							BEGIN
								BEGIN TRANSACTION;
								IF NOT EXISTS (SELECT * FROM [%[3]s] WHERE [Key]=@Key AND RowVersion = @RowVersion)
									BEGIN
										THROW 2601, ''FIRST-WRITE: COMPETING RECORD ALREADY WRITTEN.'', 1
									END
								BEGIN
									UPDATE [%[3]s]
									SET [Data]=@Data, UpdateDate=GETDATE(), ExpireDate=CASE WHEN @TTL IS NULL THEN NULL ELSE DATEADD(SECOND, @TTL, GETDATE()) END
									WHERE [Key]=@Key AND RowVersion = @RowVersion
								END
								COMMIT;
							END
						ELSE
							BEGIN
								BEGIN TRANSACTION;
								IF EXISTS (SELECT * FROM [%[3]s] WHERE [Key]=@Key)
									BEGIN
										THROW 2601, ''FIRST-WRITE: COMPETING RECORD ALREADY WRITTEN.'', 1
									END
								BEGIN
									BEGIN TRY
										INSERT INTO [%[3]s] ([Key], [Data], ExpireDate) VALUES (@Key, @Data, CASE WHEN @TTL IS NULL THEN NULL ELSE DATEADD(SECOND, @TTL, GETDATE()) END)
									END TRY

									BEGIN CATCH
										IF ERROR_NUMBER() IN (2601, 2627)
											UPDATE [%[3]s]
											SET [Data]=@Data, UpdateDate=GETDATE(), ExpireDate=CASE WHEN @TTL IS NULL THEN NULL ELSE DATEADD(SECOND, @TTL, GETDATE()) END
											WHERE [Key]=@Key AND RowVersion = ISNULL(@RowVersion, RowVersion)
									END CATCH
								END
								COMMIT;
							END
					END
				ELSE
					BEGIN
						IF (@RowVersion IS NOT NULL)
							BEGIN
								UPDATE [%[3]s]
								SET [Data]=@Data, UpdateDate=GETDATE(), ExpireDate=CASE WHEN @TTL IS NULL THEN NULL ELSE DATEADD(SECOND, @TTL, GETDATE()) END
								WHERE [Key]=@Key AND RowVersion = @RowVersion
								RETURN
							END
						ELSE
							BEGIN
								BEGIN TRY
									INSERT INTO [%[3]s] ([Key], [Data], ExpireDate) VALUES (@Key, @Data, CASE WHEN @TTL IS NULL THEN NULL ELSE DATEADD(SECOND, @TTL, GETDATE()) END)
								END TRY

								BEGIN CATCH
									IF ERROR_NUMBER() IN (2601, 2627)
										UPDATE [%[3]s]
										SET [Data]=@Data, UpdateDate=GETDATE(), ExpireDate=CASE WHEN @TTL IS NULL THEN NULL ELSE DATEADD(SECOND, @TTL, GETDATE()) END
										WHERE [Key]=@Key AND RowVersion = ISNULL(@RowVersion, RowVersion)
								END CATCH
							END
					END
			END
	`,
		mr.upsertProcFullName,
		mr.pkColumnType,
		m.metadata.TableName,
	)

	return m.createStoredProcedureIfNotExists(ctx, db, mr.upsertProcName, tsql)
}
