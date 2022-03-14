package clickhouse

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
	"strconv"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/pkg/errors"
)

const (
	// list of operations.
	execOperation  bindings.OperationKind = "exec"
	queryOperation bindings.OperationKind = "query"
	closeOperation bindings.OperationKind = "close"

	connectionURLKey = "url"

	// To connect to Clickhouse running in Azure over SSL you have to download a
	// SSL certificate. If this is provided the driver will connect using
	// SSL. If you have disable SSL you can leave this empty.
	// When the user provides a pem path their connection string must end with
	// &tls=custom
	// The connection string should be like in the following
	// tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000
	pemPathKey = "pemPath"

	// other general settings for DB connections.
	maxIdleConnsKey    = "maxIdleConns"
	maxOpenConnsKey    = "maxOpenConns"
	connMaxLifetimeKey = "connMaxLifetime"
	connMaxIdleTimeKey = "connMaxIdleTime"

	commandSQLKey = "sql"

	// keys from response's metadata.
	respOpKey        = "operation"
	respSQLKey       = "sql"
	respStartTimeKey = "start-time"
	respEndTimeKey   = "end-time"
	respDurationKey  = "duration"
)

var (
	ErrMetadataURLKeyMissing = errors.New("missing Click  connection DSN url")
	ErrRequestRequired       = errors.New("invoke request required")
	ErrMetadataRequired      = errors.New("metadata required")
	ErrInvalidOperation      = errors.Errorf("invalid operation type Expected %s, %s, or %s", execOperation, queryOperation, closeOperation)
	ErrCommandSQLKeyRequired = errors.Errorf("required metadata not set: %s", commandSQLKey)
)

var _ bindings.OutputBinding = &Clickhouse{}

type Clickhouse struct {
	db     *sql.DB
	logger logger.Logger
}

func NewClickhouse(logger logger.Logger) *Clickhouse {
	return &Clickhouse{logger: logger}
}

func (c *Clickhouse) Init(metadata bindings.Metadata) error {
	c.logger.Debugf("Initializing Clickhouse Binding")

	p := metadata.Properties
	url, ok := p[connectionURLKey]
	if !ok || url == "" {
		return ErrMetadataURLKeyMissing
	}

	db, err := initDB(url, metadata.Properties[pemPathKey])
	if err != nil {
		return err
	}

	if err = propertyToInt(p, maxIdleConnsKey, db.SetMaxIdleConns); err != nil {
		return err
	}

	if err = propertyToInt(p, maxOpenConnsKey, db.SetMaxOpenConns); err != nil {
		return err
	}

	if err = propertyToDuration(p, connMaxIdleTimeKey, db.SetConnMaxIdleTime); err != nil {
		return err
	}

	if err = propertyToDuration(p, connMaxLifetimeKey, db.SetConnMaxLifetime); err != nil {
		return err
	}

	c.db = db
	return nil
}

func (c *Clickhouse) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	if req == nil {
		return nil, ErrRequestRequired
	}

	if req.Operation == closeOperation {
		return nil, c.db.Close()
	}

	if req.Metadata == nil {
		return nil, ErrMetadataRequired
	}
	c.logger.Debugf("operation: %v", req.Operation)

	s, ok := req.Metadata[commandSQLKey]
	if !ok || s == "" {
		return nil, ErrCommandSQLKeyRequired
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
		err := c.exec(s)
		if err != nil {
			return nil, err
		}

	case queryOperation:
		d, err := c.query(s)
		if err != nil {
			return nil, err
		}
		resp.Data = d

	default:
		return nil, ErrInvalidOperation
	}

	endTime := time.Now().UTC()
	resp.Metadata[respEndTimeKey] = endTime.Format(time.RFC3339Nano)
	resp.Metadata[respDurationKey] = endTime.Sub(startTime).String()

	return resp, nil

}

func (c Clickhouse) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{execOperation, queryOperation, closeOperation}
}

func (c *Clickhouse) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

func (c *Clickhouse) query(sql string) ([]byte, error) {
	c.logger.Debugf("query: %s", sql)

	rows, err := c.db.Query(sql)
	if err != nil {
		return nil, errors.Wrapf(err, "error executing %s", sql)
	}

	defer func() {
		_ = rows.Close()
		_ = rows.Err()
	}()

	result, err := c.jsonify(rows)
	if err != nil {
		return nil, errors.Wrapf(err, "error marshalling query result for %s", sql)
	}

	return result, nil
}

func (c *Clickhouse) exec(sql string) error {
	c.logger.Debugf("exec: %s", sql)

	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Close()
	if _, err := stmt.Exec(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (c *Clickhouse) jsonify(rows *sql.Rows) ([]byte, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	var ret []interface{}
	for rows.Next() {
		values := prepareValues(columnTypes)
		err := rows.Scan(values...)
		if err != nil {
			return nil, err
		}

		r := c.convert(columnTypes, values)
		ret = append(ret, r)
	}

	return json.Marshal(ret)
}

func (c *Clickhouse) convert(columnTypes []*sql.ColumnType, values []interface{}) map[string]interface{} {
	r := map[string]interface{}{}

	for i, ct := range columnTypes {
		value := values[i]

		switch v := values[i].(type) {
		case driver.Valuer:
			if vv, err := v.Value(); err == nil {
				value = interface{}(vv)
			} else {
				c.logger.Warnf("error to convert value: %v", err)
			}
		case *sql.RawBytes:
			switch ct.DatabaseTypeName() {
			case "VARCHAR", "CHAR":
				value = string(*v)
			}
		}

		if value != nil {
			r[ct.Name()] = value
		}
	}

	return r
}

func initDB(url, pemPath string) (*sql.DB, error) {
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

		err = clickhouse.RegisterTLSConfig("custom", &tls.Config{RootCAs: rootCertPool, MinVersion: tls.VersionTLS12})
		if err != nil {
			return nil, errors.Wrap(err, "Error register TLS config")
		}
	}
	connect, err := sql.Open("clickhouse", url)
	if err != nil {
		return nil, errors.Wrap(err, "open clickhouse error")
	}
	if err := connect.Ping(); err != nil {
		return nil, errors.Wrap(err, "ping connection error")
	}
	return connect, nil
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

func prepareValues(columnTypes []*sql.ColumnType) []interface{} {
	types := make([]reflect.Type, len(columnTypes))
	for i, tp := range columnTypes {
		types[i] = tp.ScanType()
	}

	values := make([]interface{}, len(columnTypes))
	for i := range values {
		values[i] = reflect.New(types[i]).Interface()
	}

	return values
}
