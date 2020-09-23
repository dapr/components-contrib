// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package postgres

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/pkg/errors"

	"github.com/jackc/pgx/v4/pgxpool"
)

// List of operations.
const (
	ExecOperation  bindings.OperationKind = "exec"
	QueryOperation bindings.OperationKind = "query"
	CloseOperation bindings.OperationKind = "close"

	ConnectionURLKey = "url"
	CommandSQLKey    = "sql"

	CommandTimeoutDefault = 5
)

// Binding represents PostgreSQL output binding
type Binding struct {
	logger logger.Logger
	db     *pgxpool.Pool
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

	poolConfig, err := pgxpool.ParseConfig(url)
	if err != nil {
		errors.Wrap(err, "error opening DB connection")
	}

	b.db, err = pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
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
			"operation":  string(req.Operation),
			"sql":        sql,
			"start-time": startTime.Format(time.RFC3339Nano),
		},
	}

	switch req.Operation {
	case ExecOperation:
		r, err := b.exec(sql)
		if err != nil {
			resp.Metadata["error"] = err.Error()
		}
		resp.Metadata["rows-affected"] = strconv.FormatInt(r, 10) // 0 if error

	case QueryOperation:
		d, err := b.query(sql)
		if err != nil {
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

	return resp, nil
}

// QueryResults is
type QueryResults struct {
	Columns []interface{}
	Rows    [][]interface{}
}

func (b *Binding) query(sql string) (result []byte, err error) {
	b.logger.Debugf("select: %s", sql)

	ctx, cancel := context.WithTimeout(context.Background(), CommandTimeoutDefault*time.Second)
	defer cancel()

	rows, err := b.db.Query(ctx, sql)
	if err != nil {
		return nil, errors.Wrapf(err, "error executing query: %s", sql)
	}

	rs := make([]interface{}, 0)
	for rows.Next() {
		val, rowErr := rows.Values()
		if rowErr != nil {
			return nil, errors.Wrapf(rowErr, "error parsing result: %v", rows.Err())
		}
		rs = append(rs, val)
	}

	if result, err = json.Marshal(rs); err != nil {
		err = errors.Wrap(err, "error serializing results")
	}
	return
}

func (b *Binding) exec(sql string) (result int64, err error) {
	b.logger.Debugf("exec: %s", sql)

	ctx, cancel := context.WithTimeout(context.Background(), CommandTimeoutDefault*time.Second)
	defer cancel()

	res, err := b.db.Exec(ctx, sql)
	if err != nil {
		return 0, errors.Wrapf(err, "error executing query: %s", sql)
	}

	result = res.RowsAffected()
	return
}
