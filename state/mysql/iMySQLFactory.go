// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mysql

import "database/sql"

// This interface is used to help improve testing
type iMySQLFactory interface {
	Open(connectionString string) (*sql.DB, error)

	// When you are not running in k8s you can just provide the full path
	// to the pem file.
	RegisterTLSConfigWithFile(pemPath string) error

	// You can't mount volumes to the sidecar in k8s so we need an alternative
	// way to get the pem information to the component. It is down with a
	// k8s secret.
	RegisterTLSConfigWithString(pemContents string) error
}
