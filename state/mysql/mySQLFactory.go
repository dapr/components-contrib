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
	"encoding/base64"

	"github.com/dapr/dapr/pkg/logger"
	"github.com/go-sql-driver/mysql"
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

func (m *mySQLFactory) RegisterTLSConfigWithFile(pemPath string) error {
	pem, readErr := ioutil.ReadFile(pemPath)

	if readErr != nil {
		m.logger.Errorf("Error reading PEM file from $s", pemPath)

		return readErr
	}

	return m.registerTLSConfig(pem)
}

// Used when running in k8s and reading the pem contents from a secret. This
// is needed because you can't mount a volume to the sidecar
// The string must be saves as a 64 bit encoded value so we have to decode it.
func (m *mySQLFactory) RegisterTLSConfigWithString(pemContents string) error {
	// Base64 Standard Decoding
	decodedContents, err := base64.StdEncoding.DecodeString(pemContents)
	if err != nil {
		fmt.Errorf("Error decoding string: %s ", err.Error())
		return err
	}

	return m.registerTLSConfig([]byte(decodedContents))
}

func (m *mySQLFactory) registerTLSConfig(pemContents []byte) error {
	rootCertPool := x509.NewCertPool()

	ok := rootCertPool.AppendCertsFromPEM(pemContents)

	if !ok {
		return fmt.Errorf("failed to append PEM")
	}

	mysql.RegisterTLSConfig("custom", &tls.Config{RootCAs: rootCertPool, MinVersion: tls.VersionTLS12})

	return nil
}
