// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mysql

import (
	"database/sql"
	"encoding/json"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"strconv"
	"time"
)

const (
	// list of operations.
	execOperation  bindings.OperationKind = "exec"
	queryOperation bindings.OperationKind = "query"
	closeOperation bindings.OperationKind = "close"

	// configurations to connect to Mysql, either a data source name represent by URL
	connectionURLKey = "url"
	// , or separated keys:
	userKey     = "user"
	passwordKey = "password"
	networkKey  = "network"
	serverKey   = "server"
	portKey     = "port"
	databaseKey = "database"

	// other general settings for DB connections
	maxIdleConnsKey    = "maxIdleConns"
	maxOpenConnsKey    = "maxOpenConns"
	connMaxLifetimeKey = "connMaxLifetime"
	connMaxIdleTimeKey = "connMaxIdleTime"

	// keys from request's metadata
	commandSQLKey = "sql"

	// keys from response's metadata
	respOpKey           = "operation"
	respSqlKey          = "sql"
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
	p := metadata.Properties
	// either init from URL or from separated keys
	if url, ok := p[connectionURLKey]; ok && url != "" {
		db, err := initDBFromURL(url)
		if err != nil {
			return err
		}
		m.db = db
	} else {
		db, err := initDBFromConfig(p[userKey], p[passwordKey], p[networkKey], p[serverKey], p[portKey], p[databaseKey])
		if err != nil {
			return err
		}
		m.db = db
	}

	if err := propertyToInt(p, maxIdleConnsKey, m.db.SetMaxIdleConns); err != nil {
		return err
	}

	if err := propertyToInt(p, maxOpenConnsKey, m.db.SetMaxOpenConns); err != nil {
		return err
	}

	if err := propertyToDuration(p, connMaxIdleTimeKey, m.db.SetConnMaxIdleTime); err != nil {
		return err
	}

	if err := propertyToDuration(p, connMaxLifetimeKey, m.db.SetConnMaxLifetime); err != nil {
		return err
	}

	if err := m.db.Ping(); err != nil {
		return errors.Wrap(err, "unable to pint the DB")
	}

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
			respSqlKey:       s,
			respStartTimeKey: startTime.Format(time.RFC3339Nano),
		},
	}

	switch req.Operation {
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

func (m *Mysql) query(s string) ([]byte, error) {
	m.logger.Debugf("query: %s", s)

	rows, err := m.db.Query(s)
	if err != nil {
		return nil, errors.Wrapf(err, "error executing %s", s)
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrapf(err, "error finding column names for %s", s)
	}

	var values [][]sql.RawBytes
	for rows.Next() {
		row := make([]sql.RawBytes, len(columns))
		scanArgs := make([]interface{}, len(row))
		for i := range row {
			scanArgs[i] = &row[i]
		}

		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, errors.Wrapf(err, "error scanning query result for %s", s)
		}

		values = append(values, row)
	}

	result, err := json.Marshal(values)
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
		if i, err := strconv.Atoi(v); err == nil {
			setter(time.Duration(i) * time.Second)
		} else {
			return errors.Wrapf(err, "error converitng %s:%s to int", key, v)
		}
	}
	return nil
}

func initDBFromURL(url string) (*sql.DB, error) {
	if _, err := mysql.ParseDSN(url); err != nil {
		return nil, errors.Wrapf(err, "illegal Data Source Name (DNS) specified by %s", connectionURLKey)
	}

	db, err := sql.Open("mysql", url)
	if err != nil {
		return nil, errors.Wrap(err, "error opening DB connection")
	}

	return db, nil
}

func initDBFromConfig(user, passwd, net, server, port, db string) (*sql.DB, error) {
	config := mysql.NewConfig()
	config.User = user
	config.Passwd = passwd
	config.Net = net
	if server != "" && port != "" {
		config.Addr = server + ":" + port
	}
	config.DBName = db
	dns := config.FormatDSN()

	ret, err := sql.Open("mysql", dns)
	if err != nil {
		return nil, errors.Wrap(err, "error opening DB connection")
	}
	return ret, nil
}
