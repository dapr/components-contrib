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

package postgres

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

// List of operations.
const (
	execOperation  bindings.OperationKind = "exec"
	queryOperation bindings.OperationKind = "query"
	closeOperation bindings.OperationKind = "close"

	connectionURLKey = "url"
	commandSQLKey    = "sql"
)

// Postgres represents PostgreSQL output binding.
type Postgres struct {
	logger logger.Logger
	db     *pgxpool.Pool
}

// NewPostgres returns a new PostgreSQL output binding.
func NewPostgres(logger logger.Logger) bindings.OutputBinding {
	return &Postgres{logger: logger}
}

// Init initializes the PostgreSql binding.
func (p *Postgres) Init(metadata bindings.Metadata) error {
	url, ok := metadata.Properties[connectionURLKey]
	if !ok || url == "" {
		return errors.Errorf("required metadata not set: %s", connectionURLKey)
	}

	poolConfig, err := pgxpool.ParseConfig(url)
	if err != nil {
		return errors.Wrap(err, "error opening DB connection")
	}

	p.db, err = pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return errors.Wrap(err, "unable to ping the DB")
	}

	return nil
}

// Operations returns list of operations supported by PostgreSql binding.
func (p *Postgres) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		execOperation,
		queryOperation,
		closeOperation,
	}
}

// Invoke handles all invoke operations.
func (p *Postgres) Invoke(ctx context.Context, req *bindings.InvokeRequest) (resp *bindings.InvokeResponse, err error) {
	if req == nil {
		return nil, errors.Errorf("invoke request required")
	}

	if req.Operation == closeOperation {
		p.db.Close()

		return nil, nil
	}

	if req.Metadata == nil {
		return nil, errors.Errorf("metadata required")
	}
	p.logger.Debugf("operation: %v", req.Operation)

	sql, ok := req.Metadata[commandSQLKey]
	if !ok || sql == "" {
		return nil, errors.Errorf("required metadata not set: %s", commandSQLKey)
	}

	startTime := time.Now().UTC()
	resp = &bindings.InvokeResponse{
		Metadata: map[string]string{
			"operation":  string(req.Operation),
			"sql":        sql,
			"start-time": startTime.Format(time.RFC3339Nano),
		},
	}

	switch req.Operation { //nolint:exhaustive
	case execOperation:
		r, err := p.exec(ctx, sql)
		if err != nil {
			return nil, errors.Wrapf(err, "error executing %s with %v", sql, err)
		}
		resp.Metadata["rows-affected"] = strconv.FormatInt(r, 10) // 0 if error

	case queryOperation:
		d, err := p.query(ctx, sql)
		if err != nil {
			return nil, errors.Wrapf(err, "error executing %s with %v", sql, err)
		}
		resp.Data = d

	default:
		return nil, errors.Errorf(
			"invalid operation type: %s. Expected %s, %s, or %s",
			req.Operation, execOperation, queryOperation, closeOperation,
		)
	}

	endTime := time.Now().UTC()
	resp.Metadata["end-time"] = endTime.Format(time.RFC3339Nano)
	resp.Metadata["duration"] = endTime.Sub(startTime).String()

	return resp, nil
}

// Close close PostgreSql instance.
func (p *Postgres) Close() error {
	if p.db == nil {
		return nil
	}
	p.db.Close()

	return nil
}

func (p *Postgres) query(ctx context.Context, sql string) (result []byte, err error) {
	p.logger.Debugf("query: %s", sql)

	rows, err := p.db.Query(ctx, sql)
	if err != nil {
		return nil, errors.Wrapf(err, "error executing %s", sql)
	}

	rs := make([]any, 0)
	for rows.Next() {
		val, rowErr := rows.Values()
		if rowErr != nil {
			return nil, errors.Wrapf(rowErr, "error parsing result: %v", rows.Err())
		}
		rs = append(rs, val) //nolint:asasalint
	}

	if result, err = json.Marshal(rs); err != nil {
		err = errors.Wrap(err, "error serializing results")
	}

	return
}

func (p *Postgres) exec(ctx context.Context, sql string) (result int64, err error) {
	p.logger.Debugf("exec: %s", sql)

	res, err := p.db.Exec(ctx, sql)
	if err != nil {
		return 0, errors.Wrapf(err, "error executing %s", sql)
	}

	result = res.RowsAffected()

	return
}
