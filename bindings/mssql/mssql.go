package mssql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/pkg/errors"
)

const (
	// list of operations.
	execOperation  bindings.OperationKind = "exec"
	queryOperation bindings.OperationKind = "query"
	closeOperation bindings.OperationKind = "close"

	// configurations to connect to MSSQL, either a data source name represent by URL.
	connectionURLKey = "url"

	// keys from request's metadata.
	commandSQLKey = "sql"
)

type MSSQL struct {
	db     *sql.DB
	logger logger.Logger
}

var _ = bindings.OutputBinding(&MSSQL{})


// NewMSSQL returns a new MSSQL output binding.
func NewMSSQL(logger logger.Logger) *MSSQL {
	return &MSSQL{logger: logger}
}

func (m *MSSQL) Init(metadata bindings.Metadata) error {
	m.logger.Debug("Initializing MSSQL binding")

	p := metadata.Properties
	url, ok := p[connectionURLKey]
	if !ok || url == "" {
		return fmt.Errorf("missing MSSQL connection string")
	}

	db, err := sql.Open("sqlserver", url)
	if err != nil {
		return fmt.Errorf("%q", err)
	}
	m.db = db
	return nil
}

func (m *MSSQL) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
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
	switch req.Operation {
	case queryOperation:
		d, err := m.query(s)
		if err != nil {
			return nil, err
		}
		resp.Data = d
	default:
		return nil, errors.Errorf("invalid operation type: %s. Expected %s, %s, or %s", req.Operation, execOperation, queryOperation, closeOperation)
	}
	endTime := time.Now().UTC()
	resp.Metadata[respEndTimeKey] = endTime.Format(time.RFC3339Nano)
	resp.Metadata[respDurationKey] = endTime.Sub(startTime).String()
	return resp, nil
}

func (m *MSSQL) query(sql string) ([]byte, error) {
	m.logger.Debugf("query: %s", sql)
	rows, err := m.db.Query(sql)
	if err != nil {
		return nil, errors.Wrapf(err, "error executing %s", sql)
	}
	defer func() {
		_ = rows.Close()
		_ = rows.Err()
	}()
	result, err := m.jsonify(rows)
	if err != nil {
		return nil, errors.Wrapf(err, "error marshalling query result for %s", sql)
	}

	return result, nil
}

func (m *MSSQL) jsonify(rows *sql.Rows) ([]byte, error) {
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

		r := m.convert(columnTypes, values)
		ret = append(ret, r)
	}

	return json.Marshal(ret)
}

// Operations returns list of operations supported by MSSQL binding.
func (m *MSSQL) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		execOperation,
		queryOperation,
		closeOperation,
	}
}

// Close will close the DB.
func (m *MSSQL) Close() error {
	if m.db != nil {
		return m.db.Close()
	}

	return nil
}
