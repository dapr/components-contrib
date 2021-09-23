// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io/ioutil"

	"github.com/go-sql-driver/mysql"

	"github.com/dapr/kit/logger"
)

type mySQLFactory struct {
	logger logger.Logger
}

func newMySQLFactory(logger logger.Logger) *mySQLFactory {
	return &mySQLFactory{
		logger: logger,
	}
}

func (m *mySQLFactory) Open(connectionString string) (*sql.DB, error) {
	return sql.Open("mysql", connectionString)
}

func (m *mySQLFactory) RegisterTLSConfig(pemPath string) error {
	rootCertPool := x509.NewCertPool()
	pem, readErr := ioutil.ReadFile(pemPath)

	if readErr != nil {
		m.logger.Errorf("Error reading PEM file from $s", pemPath)

		return readErr
	}

	ok := rootCertPool.AppendCertsFromPEM(pem)

	if !ok {
		return fmt.Errorf("failed to append PEM")
	}

	mysql.RegisterTLSConfig("custom", &tls.Config{RootCAs: rootCertPool, MinVersion: tls.VersionTLS12})

	return nil
}
