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

package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"os"

	"github.com/go-sql-driver/mysql"

	"github.com/dapr/kit/logger"
)

// This interface is used to help improve testing.
type iMySQLFactory interface {
	Open(connectionString string) (*sql.DB, error)
	RegisterTLSConfig(pemPath string) error
}

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
	pem, readErr := os.ReadFile(pemPath)

	if readErr != nil {
		m.logger.Error("Error reading PEM file from " + pemPath)
		return readErr
	}

	ok := rootCertPool.AppendCertsFromPEM(pem)
	if !ok {
		return errors.New("failed to append PEM")
	}

	mysql.RegisterTLSConfig("custom", &tls.Config{RootCAs: rootCertPool, MinVersion: tls.VersionTLS12})

	return nil
}
