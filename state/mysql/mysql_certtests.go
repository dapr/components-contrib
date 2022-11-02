//go:build certtests
// +build certtests

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

// This file is only built for certification tests and it exposes internal properties of the object
package mysql

import (
	"database/sql"
)

// GetConnection returns the database connection.
func (m *MySQL) GetConnection() *sql.DB {
	return m.db
}

// SchemaName returns the value of the schemaName property.
func (m *MySQL) SchemaName() string {
	return m.schemaName
}

// TableName returns the value of the tableName property.
func (m *MySQL) TableName() string {
	return m.tableName
}
