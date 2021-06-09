// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

const (
	// list of operations.
	execOperation  bindings.OperationKind = "exec"
	queryOperation bindings.OperationKind = "query"
	closeOperation bindings.OperationKind = "close"

	// configurations to connect to Mysql, either a data source name represent by URL
	connectionURLKey = "url"

	// To connect to MySQL running in Azure over SSL you have to download a
	// SSL certificate. If this is provided the driver will connect using
	// SSL. If you have disable SSL you can leave this empty.
	// When the user provides a pem path their connection string must end with
	// &tls=custom
	// The connection string should be in the following format
	// "%s:%s@tcp(%s:3306)/%s?allowNativePasswords=true&tls=custom",'myadmin@mydemoserver', 'yourpassword', 'mydemoserver.mysql.database.azure.com', 'targetdb'
	pemPathKey = "pemPath"

	// other general settings for DB connections
	maxIdleConnsKey    = "maxIdleConns"
	maxOpenConnsKey    = "maxOpenConns"
	connMaxLifetimeKey = "connMaxLifetime"
	connMaxIdleTimeKey = "connMaxIdleTime"

	// keys from request's metadata
	commandSQLKey = "sql"

	// keys from response's metadata
	respOpKey           = "operation"
	respSQLKey          = "sql"
	respStartTimeKey    = "start-time"
	respRowsAffectedKey = "rows-affected"
	respEndTimeKey      = "end-time"
	respDurationKey     = "duration"
)

// Mysql represents MySQL output bindings
type Mysql struct {
	db     *sql.DB
	logger logger.Logger
}

var _ = bindings.OutputBinding(&Mysql{})

// NewMysql returns a new MySQL output binding
func NewMysql(logger logger.Logger) *Mysql {
	return &Mysql{logger: logger}
}

// Init initializes the MySQL binding
func (m *Mysql) Init(metadata bindings.Metadata) error {
	m.logger.Debug("Initializing MySql binding")

	p := metadata.Properties
	url, ok := p[connectionURLKey]
	if !ok || url == "" {
		return fmt.Errorf("missing MySql connection string")
	}

	db, err := initDB(url, metadata.Properties[pemPathKey])
	if err != nil {
		return err
	}

	err = propertyToInt(p, maxIdleConnsKey, db.SetMaxIdleConns)
	if err != nil {
		return err
	}

	err = propertyToInt(p, maxOpenConnsKey, db.SetMaxOpenConns)
	if err != nil {
		return err
	}

	err = propertyToDuration(p, connMaxIdleTimeKey, db.SetConnMaxIdleTime)
	if err != nil {
		return err
	}

	err = propertyToDuration(p, connMaxLifetimeKey, db.SetConnMaxLifetime)
	if err != nil {
		return err
	}

	err = db.Ping()
	if err != nil {
		return errors.Wrap(err, "unable to ping the DB")
	}

	m.db = db

	return nil
}

// Invoke handles all invoke operations
func (m *Mysql) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	if req == nil {
		return nil, errors.Errorf("invoke request required")
	}

	if req.Operation == closeOperation {
		return nil, m.db.Close()
	}

	if req.Metadata == nil {
		return nil, errors.Errorf("metadata required")
	}
	m.logger.Debugf("operation: %v", req.Operation)

	s, ok := req.Metadata[commandSQLKey]
	if !ok || s == "" {
		return nil, errors.Errorf("required metadata not set: %s", commandSQLKey)
	}

	startTime := time.Now().UTC()

	resp := &bindings.InvokeResponse{
		Metadata: map[string]string{
			respOpKey:        string(req.Operation),
			respSQLKey:       s,
			respStartTimeKey: startTime.Format(time.RFC3339Nano),
		},
	}

	switch req.Operation { // nolint: exhaustive
	case execOperation:
		r, err := m.exec(s)
		if err != nil {
			return nil, errors.Wrapf(err, "error executing %s with %v", s, err)
		}
		resp.Metadata[respRowsAffectedKey] = strconv.FormatInt(r, 10)

	case queryOperation:
		d, err := m.query(s)
		if err != nil {
			return nil, errors.Wrapf(err, "error executing %s with %v", s, err)
		}
		resp.Data = d

	default:
		return nil, errors.Errorf("invalid operation type: %s. Expected %s, %s, or %s",
			req.Operation, execOperation, queryOperation, closeOperation)
	}

	endTime := time.Now().UTC()
	resp.Metadata[respEndTimeKey] = endTime.Format(time.RFC3339Nano)
	resp.Metadata[respDurationKey] = endTime.Sub(startTime).String()

	return resp, nil
}

// Operations returns list of operations supported by Mysql binding
func (m *Mysql) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		execOperation,
		queryOperation,
		closeOperation,
	}
}

// Close will close the DB
func (m *Mysql) Close() error {
	if m.db != nil {
		return m.db.Close()
	}

	return nil
}

func (m *Mysql) query(s string) ([]byte, error) {
	m.logger.Debugf("query: %s", s)

	rows, err := m.db.Query(s)
	if err != nil {
		return nil, errors.Wrapf(err, "error executing %s", s)
	}

	defer func() {
		_ = rows.Close()
		_ = rows.Err()
	}()

	result, err := jsonify(rows)
	if err != nil {
		return nil, errors.Wrapf(err, "error marshalling query result for %s", s)
	}

	return result, nil
}

func (m *Mysql) exec(sql string) (int64, error) {
	m.logger.Debugf("exec: %s", sql)

	res, err := m.db.Exec(sql)
	if err != nil {
		return 0, errors.Wrapf(err, "error executing %s", sql)
	}

	return res.RowsAffected()
}

func propertyToInt(props map[string]string, key string, setter func(int)) error {
	if v, ok := props[key]; ok {
		if i, err := strconv.Atoi(v); err == nil {
			setter(i)
		} else {
			return errors.Wrapf(err, "error converitng %s:%s to int", key, v)
		}
	}

	return nil
}

func propertyToDuration(props map[string]string, key string, setter func(time.Duration)) error {
	if v, ok := props[key]; ok {
		if d, err := time.ParseDuration(v); err == nil {
			setter(d)
		} else {
			return errors.Wrapf(err, "error converitng %s:%s to time duration", key, v)
		}
	}

	return nil
}

func initDB(url, pemPath string) (*sql.DB, error) {
	if _, err := mysql.ParseDSN(url); err != nil {
		return nil, errors.Wrapf(err, "illegal Data Source Name (DNS) specified by %s", connectionURLKey)
	}

	if pemPath != "" {
		rootCertPool := x509.NewCertPool()
		pem, err := ioutil.ReadFile(pemPath)
		if err != nil {
			return nil, errors.Wrapf(err, "Error reading PEM file from %s", pemPath)
		}

		ok := rootCertPool.AppendCertsFromPEM(pem)
		if !ok {
			return nil, fmt.Errorf("failed to append PEM")
		}

		err = mysql.RegisterTLSConfig("custom", &tls.Config{RootCAs: rootCertPool, MinVersion: tls.VersionTLS12})
		if err != nil {
			return nil, errors.Wrap(err, "Error register TLS config")
		}
	}

	db, err := sql.Open("mysql", url)
	if err != nil {
		return nil, errors.Wrap(err, "error opening DB connection")
	}

	return db, nil
}

func jsonify(rows *sql.Rows) ([]byte, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	var ret []interface{}
	for rows.Next() {
		scanArgs := prepareScanArgs(columnTypes)
		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		r := convertScanArgs(columnTypes, scanArgs)
		ret = append(ret, r)
	}

	return json.Marshal(ret)
}

func convertScanArgs(columnTypes []*sql.ColumnType, scanArgs []interface{}) map[string]interface{} {
	r := map[string]interface{}{}

	for i, v := range columnTypes {
		if s, ok := (scanArgs[i]).(*sql.NullString); ok {
			r[v.Name()] = s.String

			continue
		}

		if s, ok := (scanArgs[i]).(*sql.NullBool); ok {
			r[v.Name()] = s.Bool

			continue
		}

		if s, ok := (scanArgs[i]).(*sql.NullInt32); ok {
			r[v.Name()] = s.Int32

			continue
		}

		if s, ok := (scanArgs[i]).(*sql.NullInt64); ok {
			r[v.Name()] = s.Int64

			continue
		}

		if s, ok := (scanArgs[i]).(*sql.NullFloat64); ok {
			r[v.Name()] = s.Float64

			continue
		}

		if s, ok := (scanArgs[i]).(*sql.NullTime); ok {
			r[v.Name()] = s.Time

			continue
		}

		// this won't happen since the default switch is sql.NullString
		r[v.Name()] = scanArgs[i]
	}

	return r
}

func prepareScanArgs(columnTypes []*sql.ColumnType) []interface{} {
	scanArgs := make([]interface{}, len(columnTypes))
	for i, v := range columnTypes {
		switch v.DatabaseTypeName() {
		case "BOOL":
			scanArgs[i] = new(sql.NullBool)
		case "INT", "MEDIUMINT", "SMALLINT", "CHAR", "TINYINT":
			scanArgs[i] = new(sql.NullInt32)
		case "BIGINT":
			scanArgs[i] = new(sql.NullInt64)
		case "DOUBLE", "FLOAT", "DECIMAL":
			scanArgs[i] = new(sql.NullFloat64)
		case "DATE", "TIME", "YEAR":
			scanArgs[i] = new(sql.NullTime)
		default:
			scanArgs[i] = new(sql.NullString)
		}
	}

	return scanArgs
}
