// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package postgres

import (
	"database/sql"
	"encoding/json"
	"strconv"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/pkg/errors"

	_ "github.com/lib/pq" // postgres golang driver
)

// List of operations.
const (
	ExecOperation  bindings.OperationKind = "exec"
	QueryOperation bindings.OperationKind = "query"
	CloseOperation bindings.OperationKind = "close"

	ConnectionURLKey = "url"
	CommandSQLKey    = "sql"
)

// Binding represents PostgreSQL output binding
type Binding struct {
	logger logger.Logger
	db     *sql.DB
}

var _ = bindings.OutputBinding(&Binding{})

// NewBinding returns a new PostgreSQL output binding
func NewBinding(logger logger.Logger) *Binding {
	return &Binding{logger: logger}
}

// Init initializes the Twitter binding
func (b *Binding) Init(metadata bindings.Metadata) error {
	url, ok := metadata.Properties[ConnectionURLKey]
	if !ok || url == "" {
		return errors.Errorf("required metadata not set: %s", ConnectionURLKey)
	}

	var err error
	if b.db, err = sql.Open("postgres", url); err != nil {
		errors.Wrap(err, "error opening DB connection")
	}

	if err := b.db.Ping(); err != nil {
		errors.Wrap(err, "unable to ping the DB")
	}

	return nil
}

// Operations returns list of operations supported by twitter binding
func (b *Binding) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		ExecOperation,
		QueryOperation,
		CloseOperation,
	}
}

// Invoke handles all invoke operations
func (b *Binding) Invoke(req *bindings.InvokeRequest) (resp *bindings.InvokeResponse, err error) {
	b.logger.Debugf("operation: %v", req.Operation)

	if req.Operation == CloseOperation {
		b.db.Close()
		return nil, nil
	}

	if req.Metadata == nil {
		return nil, errors.Errorf("metadata required")
	}

	sql, ok := req.Metadata[CommandSQLKey]
	if !ok || sql == "" {
		return nil, errors.Errorf("required metadata not set: %s", CommandSQLKey)
	}

	startTime := time.Now().UTC()
	resp = &bindings.InvokeResponse{
		Metadata: map[string]string{
			"sql":        sql,
			"start-time": startTime.Format(time.RFC3339Nano),
		},
	}

	switch req.Operation {
	case ExecOperation:
		r, err := b.exec(sql)
		if err == nil {
			resp.Metadata["error"] = err.Error()
		}
		resp.Metadata["rows-affected"] = strconv.FormatInt(r, 10) // 0 if error

	case QueryOperation:
		d, err := b.query(sql)
		if err == nil {
			resp.Metadata["error"] = err.Error()
		}
		resp.Data = d

	default:
		return nil, errors.Errorf(
			"invalid operation type: %s. Expected %s, %s, or %s",
			ExecOperation, QueryOperation, CloseOperation, req.Operation,
		)
	}

	endTime := time.Now().UTC()
	resp.Metadata["end-time"] = endTime.Format(time.RFC3339Nano)
	resp.Metadata["duration"] = endTime.Sub(startTime).String()

	return
}

func (b *Binding) query(sql string) (result []byte, err error) {
	b.logger.Debugf("select: %s", sql)

	rows, err := b.db.Query(sql)
	if err != nil {
		return nil, errors.Wrapf(err, "error executing query: %s", sql)
	}

	rs := make([]map[string]interface{}, 0)

	for rows.Next() {
		var row map[string]interface{}
		rows.Scan(row)
		rs = append(rs, row)
	}

	if result, err = json.Marshal(rs); err != nil {
		return nil, errors.Wrap(err, "error serializing results")
	}
	return
}

func (b *Binding) exec(sql string) (result int64, err error) {
	b.logger.Debugf("exec: %s", sql)

	res, err := b.db.Exec(sql)
	if err != nil {
		return 0, errors.Wrapf(err, "error executing query: %s", sql)
	}

	if result, err = res.RowsAffected(); err != nil {
		return 0, errors.Wrap(err, "error parsing result")
	}

	return
}
