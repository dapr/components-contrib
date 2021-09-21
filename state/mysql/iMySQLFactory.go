// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mysql

import "database/sql"

// This interface is used to help improve testing.
type iMySQLFactory interface {
	Open(connectionString string) (*sql.DB, error)
	RegisterTLSConfig(pemPath string) error
}
