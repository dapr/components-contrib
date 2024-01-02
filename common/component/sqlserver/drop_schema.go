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

package sqlserver

import (
	"context"
	"database/sql"
)

// DropSchema drops a schema from a SQL Server database, including all resources that were created inside
// Adapted from: https://stackoverflow.com/a/76742742/192024
func DropSchema(ctx context.Context, db *sql.DB, schema string) error {
	_, err := db.ExecContext(ctx, `
DECLARE @command NVARCHAR(MAX) = '';

WITH Schemas AS (
    SELECT 
        s.schema_id, 
        s.name AS schema_name,
        IIF(s.Name = 'dbo', 1, 0) schema_predefined
    FROM sys.schemas s
    INNER JOIN sys.sysusers u ON u.uid = s.principal_id
    WHERE u.issqlrole = 0
	    AND s.Name = @Schema
	    AND u.name NOT IN ('sys', 'guest', 'INFORMATION_SCHEMA')
),
Commands(Command) AS (
    -- Procedures
    SELECT 'DROP PROCEDURE [' + schema_name + '].[' + name + ']'
    FROM sys.procedures o
    JOIN Schemas schemas ON o.schema_id = schemas.schema_id

    -- Functions
    UNION ALL
    SELECT 'DROP FUNCTION [' + schema_name + '].[' + name + ']'
    FROM sys.objects o
    JOIN Schemas schemas ON o.schema_id = schemas.schema_id
    WHERE type IN ('FN', 'IF', 'TF')

    -- Views
    UNION ALL
    SELECT 'DROP VIEW [' + schema_name + '].[' + name + ']'
    FROM sys.views o
    JOIN Schemas schemas ON o.schema_id = schemas.schema_id

    -- Check constraints
    UNION ALL
    SELECT 
        'ALTER TABLE [' + schema_name + '].[' + object_name(parent_object_id) + '] ' +
        'DROP CONSTRAINT [' + name + ']'
    FROM sys.check_constraints o
    JOIN Schemas schemas ON o.schema_id = schemas.schema_id

    -- Foreign keys
    UNION ALL
    SELECT 
        'ALTER TABLE [' + schema_name + '].[' + object_name(parent_object_id) + '] ' +
        'DROP CONSTRAINT [' + name + ']'
    FROM sys.foreign_keys o
    JOIN Schemas schemas ON o.schema_id = schemas.schema_id
    
    -- Tables
    UNION ALL
    SELECT 'DROP TABLE [' + schema_name + '].[' + name + ']'
    FROM sys.tables o
    JOIN Schemas schemas ON o.schema_id = schemas.schema_id

    -- Sequences
    UNION ALL
    SELECT 'DROP SEQUENCE [' + schema_name + '].[' + name + ']'
    FROM sys.sequences o
    JOIN Schemas schemas ON o.schema_id = schemas.schema_id

    -- User defined types
    UNION ALL
    SELECT 'DROP TYPE [' + schema_name + '].[' + name + ']'
    FROM sys.types o
    JOIN Schemas schemas ON o.schema_id = schemas.schema_id
    WHERE is_user_defined = 1

    -- Schemas
    UNION ALL
    SELECT 'DROP SCHEMA [' + schema_name + ']'
    FROM Schemas
    WHERE schema_predefined = 0
)
SELECT @command = STRING_AGG(Command, CHAR(10))
FROM Commands

PRINT @command
EXEC sp_executesql @command
`, sql.Named("Schema", schema))
	return err
}
