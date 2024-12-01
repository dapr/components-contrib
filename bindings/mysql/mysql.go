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
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	// list of operations.
	execOperation  bindings.OperationKind = "exec"
	queryOperation bindings.OperationKind = "query"
	closeOperation bindings.OperationKind = "close"

	// configurations to connect to Mysql, either a data source name represent by URL.
	connectionURLKey = "url"

	// To connect to MySQL running in Azure over SSL you have to download a
	// SSL certificate. If this is provided the driver will connect using
	// SSL. If you have disable SSL you can leave this empty.
	// When the user provides a pem path their connection string must end with
	// &tls=custom
	// The connection string should be in the following format
	// "%s:%s@tcp(%s:3306)/%s?allowNativePasswords=true&tls=custom",'myadmin@mydemoserver', 'yourpassword', 'mydemoserver.mysql.database.azure.com', 'targetdb'.

	// keys from request's metadata.
	commandSQLKey    = "sql"
	commandParamsKey = "params"

	// keys from response's metadata.
	respOpKey           = "operation"
	respSQLKey          = "sql"
	respStartTimeKey    = "start-time"
	respRowsAffectedKey = "rows-affected"
	respEndTimeKey      = "end-time"
	respDurationKey     = "duration"
)

// Mysql represents MySQL output bindings.
type Mysql struct {
	db     *sql.DB
	logger logger.Logger
	closed atomic.Bool
}

type mysqlMetadata struct {
	// URL is the connection string to connect to MySQL.
	URL string `mapstructure:"url"`

	// PemPath is the path to the pem file to connect to MySQL over SSL.
	PemPath string `mapstructure:"pemPath"`

	// PemContents is the contents of the pem file to connect to MySQL over SSL.
	// PemContents supersedes PemPath if both are provided.
	PemContents string `mapstructure:"pemContents"`

	// MaxIdleConns is the maximum number of connections in the idle connection pool.
	MaxIdleConns int `mapstructure:"maxIdleConns"`

	// MaxOpenConns is the maximum number of open connections to the database.
	MaxOpenConns int `mapstructure:"maxOpenConns"`

	// ConnMaxLifetime is the maximum amount of time a connection may be reused.
	ConnMaxLifetime time.Duration `mapstructure:"connMaxLifetime"`

	// ConnMaxIdleTime is the maximum amount of time a connection may be idle.
	ConnMaxIdleTime time.Duration `mapstructure:"connMaxIdleTime"`
}

// NewMysql returns a new MySQL output binding.
func NewMysql(logger logger.Logger) bindings.OutputBinding {
	return &Mysql{
		logger: logger,
	}
}

// Init initializes the MySQL binding.
func (m *Mysql) Init(ctx context.Context, md bindings.Metadata) error {
	if m.closed.Load() {
		return errors.New("cannot initialize a previously-closed component")
	}

	// Parse metadata
	meta := mysqlMetadata{}
	err := kitmd.DecodeMetadata(md.Properties, &meta)
	if err != nil {
		return err
	}

	if meta.URL == "" {
		return errors.New("missing MySql connection string")
	}

	var pemContents []byte

	// meta.PemContents supersedes meta.PemPath if both are provided.
	if meta.PemContents != "" {
		// Reformat the PEM to standard format
		meta.PemContents = reformatPEM(meta.PemContents)
		pemContents = []byte(meta.PemContents)
	} else if meta.PemPath != "" {
		pemContents, err = os.ReadFile(meta.PemPath)
		if err != nil {
			return fmt.Errorf("unable to read PEM file: %w", err)
		}
	}

	// Decode PEM contents and parse certificate to ensure it's valid.
	if len(pemContents) != 0 {
		block, _ := pem.Decode(pemContents)
		if block == nil {
			return errors.New("failed to decode PEM")
		}

		_, err = x509.ParseCertificate(block.Bytes)
		if err != nil {
			return fmt.Errorf("failed to parse PEM contents: %w", err)
		}
	}

	m.db, err = initDB(meta.URL, pemContents)
	if err != nil {
		return err
	}

	if meta.MaxIdleConns > 0 {
		m.db.SetMaxIdleConns(meta.MaxIdleConns)
	}
	if meta.MaxOpenConns > 0 {
		m.db.SetMaxOpenConns(meta.MaxOpenConns)
	}
	if meta.ConnMaxIdleTime > 0 {
		m.db.SetConnMaxIdleTime(meta.ConnMaxIdleTime)
	}
	if meta.ConnMaxLifetime > 0 {
		m.db.SetConnMaxLifetime(meta.ConnMaxLifetime)
	}

	err = m.db.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("unable to ping the DB: %w", err)
	}

	return nil
}

// Invoke handles all invoke operations.
func (m *Mysql) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	if req == nil {
		return nil, errors.New("invoke request required")
	}

	// We let the "close" operation here succeed even if the component has been closed already
	if req.Operation == closeOperation {
		return nil, m.Close()
	}

	if m.closed.Load() {
		return nil, errors.New("component is closed")
	}

	if req.Metadata == nil {
		return nil, errors.New("metadata required")
	}

	s := req.Metadata[commandSQLKey]
	if s == "" {
		return nil, fmt.Errorf("required metadata not set: %s", commandSQLKey)
	}

	// Metadata property "params" contains JSON-encoded parameters, and it's optional
	// If present, it must be unserializable into a []any object
	var (
		params []any
		err    error
	)
	if paramsStr := req.Metadata[commandParamsKey]; paramsStr != "" {
		err = json.Unmarshal([]byte(paramsStr), &params)
		if err != nil {
			return nil, fmt.Errorf("invalid metadata property %s: failed to unserialize into an array: %w", commandParamsKey, err)
		}
	}

	startTime := time.Now().UTC()
	resp := &bindings.InvokeResponse{
		Metadata: map[string]string{
			respOpKey:        string(req.Operation),
			respSQLKey:       s,
			respStartTimeKey: startTime.Format(time.RFC3339Nano),
		},
	}

	switch req.Operation {
	case execOperation:
		r, err := m.exec(ctx, s, params...)
		if err != nil {
			return nil, err
		}
		resp.Metadata[respRowsAffectedKey] = strconv.FormatInt(r, 10)

	case queryOperation:
		d, err := m.query(ctx, s, params...)
		if err != nil {
			return nil, err
		}
		resp.Data = d

	default:
		return nil, fmt.Errorf("invalid operation type: %s. Expected %s, %s, or %s",
			req.Operation, execOperation, queryOperation, closeOperation)
	}

	endTime := time.Now().UTC()
	resp.Metadata[respEndTimeKey] = endTime.Format(time.RFC3339Nano)
	resp.Metadata[respDurationKey] = endTime.Sub(startTime).String()

	return resp, nil
}

// Operations returns list of operations supported by Mysql binding.
func (m *Mysql) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		execOperation,
		queryOperation,
		closeOperation,
	}
}

// Close will close the DB.
func (m *Mysql) Close() error {
	if !m.closed.CompareAndSwap(false, true) {
		// If this failed, the component has already been closed
		// We allow multiple calls to close
		return nil
	}

	if m.db != nil {
		if err := m.db.Close(); err != nil {
			m.logger.Warnf("error closing DB: %v", err)
		}
		m.db = nil
	}

	return nil
}

func (m *Mysql) query(ctx context.Context, sql string, params ...any) ([]byte, error) {
	rows, err := m.db.QueryContext(ctx, sql, params...)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}

	defer func() {
		if err = rows.Close(); err != nil {
			m.logger.Warnf("error closing rows: %v", err)
		}
	}()

	result, err := m.jsonify(rows)
	if err != nil {
		return nil, fmt.Errorf("error marshalling query result for query: %w", err)
	}

	return result, nil
}

func (m *Mysql) exec(ctx context.Context, sql string, params ...any) (int64, error) {
	res, err := m.db.ExecContext(ctx, sql, params...)
	if err != nil {
		return 0, fmt.Errorf("error executing query: %w", err)
	}

	return res.RowsAffected()
}

func initDB(url string, pemContents []byte) (*sql.DB, error) {
	// We need to register the custom TLS config before parsing the DSN if the user
	// has provided a PEM file. DSN parsing will fail if the user has provided a PEM
	// file, but the custom TLS config requested (i.e., "custom") is not registered.
	if len(pemContents) != 0 {
		// Create an empty root cert pool. We will append the PEM contents to this pool.
		rootCertPool := x509.NewCertPool()

		ok := rootCertPool.AppendCertsFromPEM(pemContents)
		if !ok {
			return nil, errors.New("failed to append PEM")
		}

		// Register TLS config with the name "custom". The url must end with &tls=custom
		// to use this custom TLS config.
		err := mysql.RegisterTLSConfig("custom", &tls.Config{
			RootCAs:    rootCertPool,
			MinVersion: tls.VersionTLS12,
		})
		if err != nil {
			return nil, fmt.Errorf("error register TLS config: %w", err)
		}
	}

	// Parse the DSN to get the connection configuration.
	conf, err := mysql.ParseDSN(url)
	if err != nil {
		return nil, fmt.Errorf("illegal Data Source Name (DSN) specified by %s", connectionURLKey)
	}

	// Required to correctly parse time columns
	// See: https://stackoverflow.com/a/45040724
	conf.ParseTime = true

	connector, err := mysql.NewConnector(conf)
	if err != nil {
		return nil, fmt.Errorf("error opening DB connection: %w", err)
	}

	db := sql.OpenDB(connector)
	return db, nil
}

// Helper function to reformat a single-line PEM into standard PEM format
func reformatPEM(pemStr string) string {
	// Ensure headers and footers are on their own lines
	pemStr = strings.ReplaceAll(pemStr, "-----BEGIN CERTIFICATE-----", "\n-----BEGIN CERTIFICATE-----\n")
	pemStr = strings.ReplaceAll(pemStr, "-----END CERTIFICATE-----", "\n-----END CERTIFICATE-----")

	// Split into base64-encoded content and reformat into 64-character lines
	lines := strings.Split(pemStr, "\n")
	if len(lines) >= 3 {
		encodedContent := lines[1]
		lines[1] = strings.Join(chunkString(encodedContent, 64), "\n")
	}
	return strings.Join(lines, "\n")
}

// Helper function to split a string into chunks of a given size
func chunkString(s string, chunkSize int) []string {
	var chunks []string
	for len(s) > chunkSize {
		chunks = append(chunks, s[:chunkSize])
		s = s[chunkSize:]
	}
	chunks = append(chunks, s)
	return chunks
}

func (m *Mysql) jsonify(rows *sql.Rows) ([]byte, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	var ret []any
	for rows.Next() {
		values := prepareValues(columnTypes)
		err := rows.Scan(values...)
		if err != nil {
			return nil, err
		}

		r := m.convert(columnTypes, values)
		ret = append(ret, r)
	}

	return json.Marshal(ret)
}

func prepareValues(columnTypes []*sql.ColumnType) []any {
	types := make([]reflect.Type, len(columnTypes))
	for i, tp := range columnTypes {
		types[i] = tp.ScanType()
	}

	values := make([]any, len(columnTypes))
	for i := range values {
		values[i] = reflect.New(types[i]).Interface()
	}

	return values
}

func (m *Mysql) convert(columnTypes []*sql.ColumnType, values []any) map[string]any {
	r := map[string]any{}

	for i, ct := range columnTypes {
		value := values[i]

		switch v := values[i].(type) {
		case driver.Valuer:
			if vv, err := v.Value(); err == nil {
				value = any(vv)
			} else {
				m.logger.Warnf("error to convert value: %v", err)
			}
		case *sql.RawBytes:
			// special case for sql.RawBytes, see https://github.com/go-sql-driver/mysql/blob/master/fields.go#L178
			switch ct.DatabaseTypeName() {
			case "VARCHAR", "CHAR", "TEXT", "LONGTEXT":
				value = string(*v)
			}
		}

		if value != nil {
			r[ct.Name()] = value
		}
	}

	return r
}

// GetComponentMetadata returns the metadata of the component.
func (m *Mysql) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := mysqlMetadata{}
	if err := metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType); err != nil {
		m.logger.Warnf("error retrieving metadata info: %v", err)
	}

	return
}
