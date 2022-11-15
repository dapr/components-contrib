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
	"database/sql"
	"fmt"
	"strings"
)

type migrator interface {
	executeMigrations() (migrationResult, error)
}

type migration struct {
	store *SQLServer
}

type migrationResult struct {
	bulkDeleteProcName       string
	bulkDeleteProcFullName   string
	itemRefTableTypeName     string
	upsertProcName           string
	upsertProcFullName       string
	pkColumnType             string
	getCommand               string
	deleteWithETagCommand    string
	deleteWithoutETagCommand string
}

func newMigration(store *SQLServer) migrator {
	return &migration{
		store: store,
	}
}

func (m *migration) newMigrationResult() migrationResult {
	r := migrationResult{
		bulkDeleteProcName:       fmt.Sprintf("sp_BulkDelete_%s", m.store.tableName),
		itemRefTableTypeName:     fmt.Sprintf("[%s].%s_Table", m.store.schema, m.store.tableName),
		upsertProcName:           fmt.Sprintf("sp_Upsert_v2_%s", m.store.tableName),
		getCommand:               fmt.Sprintf("SELECT [Data], [RowVersion] FROM [%s].[%s] WHERE [Key] = @Key", m.store.schema, m.store.tableName),
		deleteWithETagCommand:    fmt.Sprintf(`DELETE [%s].[%s] WHERE [Key]=@Key AND [RowVersion]=@RowVersion`, m.store.schema, m.store.tableName),
		deleteWithoutETagCommand: fmt.Sprintf(`DELETE [%s].[%s] WHERE [Key]=@Key`, m.store.schema, m.store.tableName),
	}

	r.bulkDeleteProcFullName = fmt.Sprintf("[%s].%s", m.store.schema, r.bulkDeleteProcName)
	r.upsertProcFullName = fmt.Sprintf("[%s].%s", m.store.schema, r.upsertProcName)

	//nolint:exhaustive
	switch m.store.keyType {
	case StringKeyType:
		r.pkColumnType = fmt.Sprintf("NVARCHAR(%d)", m.store.keyLength)

	case UUIDKeyType:
		r.pkColumnType = "uniqueidentifier"

	case IntegerKeyType:
		r.pkColumnType = "int"
	}

	return r
}

/* #nosec. */
func (m *migration) executeMigrations() (migrationResult, error) {
	r := m.newMigrationResult()

	db, err := sql.Open("sqlserver", m.store.connectionString)
	if err != nil {
		return r, err
	}

	// If the user provides a database in the connection string to not attempt
	// to create the database. This work as the component did before adding the
	// support to create the db.
	if strings.Contains(m.store.connectionString, "database=") {
		// Schedule close of connection
		defer db.Close()
	} else {
		err = m.ensureDatabaseExists(db)
		if err != nil {
			return r, fmt.Errorf("failed to create db database: %v", err)
		}

		// Close the existing connection
		db.Close()

		// Re connect with a database specific connection
		m.store.connectionString = fmt.Sprintf("%s;database=%s;", m.store.connectionString, m.store.databaseName)
		db, err = sql.Open("sqlserver", m.store.connectionString)
		if err != nil {
			return r, err
		}

		// Schedule close of new connection
		defer db.Close()
	}

	err = m.ensureSchemaExists(db)
	if err != nil {
		return r, fmt.Errorf("failed to create db schema: %v", err)
	}

	err = m.ensureTableExists(db, r)
	if err != nil {
		return r, fmt.Errorf("failed to create db table: %v", err)
	}

	err = m.ensureStoredProcedureExists(db, r)
	if err != nil {
		return r, fmt.Errorf("failed to create stored procedures: %v", err)
	}

	for _, ix := range m.store.indexedProperties {
		err = m.ensureIndexedPropertyExists(ix, db)
		if err != nil {
			return r, err
		}
	}

	return r, nil
}

func runCommand(tsql string, db *sql.DB) error {
	if _, err := db.Exec(tsql); err != nil {
		return err
	}

	return nil
}

/* #nosec. */
func (m *migration) ensureIndexedPropertyExists(ix IndexedProperty, db *sql.DB) error {
	indexName := "IX_" + ix.ColumnName

	tsql := fmt.Sprintf(`
	IF (NOT EXISTS(SELECT object_id
				   FROM sys.indexes
				   WHERE object_id = OBJECT_ID('[%s].%s')
    					AND name='%s'))
		CREATE INDEX %s ON [%s].[%s]([%s])`,
		m.store.schema,
		m.store.tableName,
		indexName,
		indexName,
		m.store.schema,
		m.store.tableName,
		ix.ColumnName)

	return runCommand(tsql, db)
}

/* #nosec. */
func (m *migration) ensureDatabaseExists(db *sql.DB) error {
	tsql := fmt.Sprintf(`
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'%s')
	CREATE DATABASE [%s]`,
		m.store.databaseName, m.store.databaseName)

	return runCommand(tsql, db)
}

/* #nosec. */
func (m *migration) ensureSchemaExists(db *sql.DB) error {
	tsql := fmt.Sprintf(`
	IF NOT EXISTS(SELECT * FROM sys.schemas WHERE name = N'%s')
		EXEC('CREATE SCHEMA [%s]')`,
		m.store.schema, m.store.schema)

	return runCommand(tsql, db)
}

/* #nosec. */
func (m *migration) ensureTableExists(db *sql.DB, r migrationResult) error {
	tsql := fmt.Sprintf(`
	IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s')
    	CREATE TABLE [%s].[%s] (
			[Key] 			%s CONSTRAINT PK_%s PRIMARY KEY,
			[Data]			NVARCHAR(MAX) NOT NULL,
			[InsertDate] 	DateTime2 NOT NULL DEFAULT(GETDATE()),
			[UpdateDate] 	DateTime2 NULL,`,
		m.store.schema, m.store.tableName, m.store.schema, m.store.tableName, r.pkColumnType, m.store.tableName)

	if m.store.indexedProperties != nil {
		for _, prop := range m.store.indexedProperties {
			if prop.Type != "" {
				tsql += fmt.Sprintf("\n		[%s] AS CONVERT(%s, JSON_VALUE(Data, '$.%s')) PERSISTED,", prop.ColumnName, prop.Type, prop.Property)
			} else {
				tsql += fmt.Sprintf("\n		[%s] AS JSON_VALUE(Data, '$.%s') PERSISTED,", prop.ColumnName, prop.Property)
			}
		}
	}

	tsql += `
		[RowVersion] 	ROWVERSION NOT NULL)
	`

	return runCommand(tsql, db)
}

/* #nosec. */
func (m *migration) ensureTypeExists(db *sql.DB, mr migrationResult) error {
	tsql := fmt.Sprintf(`
	IF type_id('[%s].%s_Table') IS NULL
		CREATE TYPE [%s].%s_Table AS TABLE
		(
			[Key]           		%s NOT NULL,
			[RowVersion]			BINARY(8)
		)
	`, m.store.schema, m.store.tableName, m.store.schema, m.store.tableName, mr.pkColumnType)

	return runCommand(tsql, db)
}

/* #nosec. */
func (m *migration) ensureBulkDeleteStoredProcedureExists(db *sql.DB, mr migrationResult) error {
	tsql := fmt.Sprintf(`
		CREATE PROCEDURE %s
			@itemsToDelete %s READONLY
		AS
			DELETE [%s].[%s]
			FROM [%s].[%s] x
			JOIN @itemsToDelete i ON i.[Key] = x.[Key] AND (i.[RowVersion] IS NULL OR i.[RowVersion] = x.[RowVersion])`,
		mr.bulkDeleteProcFullName,
		mr.itemRefTableTypeName,
		m.store.schema,
		m.store.tableName,
		m.store.schema,
		m.store.tableName)

	return m.createStoredProcedureIfNotExists(db, mr.bulkDeleteProcName, tsql)
}

func (m *migration) ensureStoredProcedureExists(db *sql.DB, mr migrationResult) error {
	err := m.ensureTypeExists(db, mr)
	if err != nil {
		return err
	}

	err = m.ensureBulkDeleteStoredProcedureExists(db, mr)
	if err != nil {
		return err
	}

	err = m.ensureUpsertStoredProcedureExists(db, mr)
	if err != nil {
		return err
	}

	return nil
}

/* #nosec. */
func (m *migration) createStoredProcedureIfNotExists(db *sql.DB, name string, escapedDefinition string) error {
	tsql := fmt.Sprintf(`
	IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[%s].[%s]') AND type in (N'P', N'PC'))
	BEGIN
		execute ('%s')
	END`,
		m.store.schema,
		name,
		escapedDefinition)

	return runCommand(tsql, db)
}

/* #nosec. */
//nolint:dupword
func (m *migration) ensureUpsertStoredProcedureExists(db *sql.DB, mr migrationResult) error {
	tsql := fmt.Sprintf(`
			CREATE PROCEDURE %s (
				@Key 			%s,
				@Data 			NVARCHAR(MAX),
				@RowVersion		BINARY(8),
				@FirstWrite		BIT)
			AS
				IF (@FirstWrite=1)
					BEGIN
						IF (@RowVersion IS NOT NULL)
							BEGIN
								BEGIN TRANSACTION;
								IF NOT EXISTS (SELECT * FROM [%s] WHERE [KEY]=@KEY AND RowVersion = @RowVersion)
									BEGIN
										THROW 2601, ''FIRST-WRITE: COMPETING RECORD ALREADY WRITTEN.'', 1
									END
								BEGIN
									UPDATE [%s]
									SET [Data]=@Data, UpdateDate=GETDATE()
									WHERE [Key]=@Key AND RowVersion = @RowVersion
								END
								COMMIT;
							END
						ELSE
							BEGIN
								BEGIN TRANSACTION;
								IF EXISTS (SELECT * FROM [%s] WHERE [KEY]=@KEY)
									BEGIN
										THROW 2601, ''FIRST-WRITE: COMPETING RECORD ALREADY WRITTEN.'', 1
									END
								BEGIN
									BEGIN TRY
										INSERT INTO [%s] ([Key], [Data]) VALUES (@Key, @Data);
									END TRY
						
									BEGIN CATCH
										IF ERROR_NUMBER() IN (2601, 2627)
											UPDATE [%s]
											SET [Data]=@Data, UpdateDate=GETDATE()
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
								UPDATE [%s]
								SET [Data]=@Data, UpdateDate=GETDATE()
								WHERE [Key]=@Key AND RowVersion = @RowVersion
								RETURN
							END
						ELSE
							BEGIN
								BEGIN TRY
									INSERT INTO [%s] ([Key], [Data]) VALUES (@Key, @Data);
								END TRY
					
								BEGIN CATCH
									IF ERROR_NUMBER() IN (2601, 2627)
										UPDATE [%s]
										SET [Data]=@Data, UpdateDate=GETDATE()
										WHERE [Key]=@Key AND RowVersion = ISNULL(@RowVersion, RowVersion)
								END CATCH
							END
					END
	`,
		mr.upsertProcFullName,
		mr.pkColumnType,
		m.store.tableName,
		m.store.tableName,
		m.store.tableName,
		m.store.tableName,
		m.store.tableName,
		m.store.tableName,
		m.store.tableName,
		m.store.tableName,
	)

	return m.createStoredProcedureIfNotExists(db, mr.upsertProcName, tsql)
}
