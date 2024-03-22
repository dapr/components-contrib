/*
Copyright 2023 The Dapr Authors
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
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// List of operations.
const (
	execOperation  bindings.OperationKind = "exec"
	queryOperation bindings.OperationKind = "query"
	closeOperation bindings.OperationKind = "close"

	commandSQLKey  = "sql"
	commandArgsKey = "params"
)

// Postgres represents PostgreSQL output binding.
type Postgres struct {
	logger logger.Logger
	db     *pgxpool.Pool
	closed atomic.Bool
}

// NewPostgres returns a new PostgreSQL output binding.
func NewPostgres(logger logger.Logger) bindings.OutputBinding {
	return &Postgres{
		logger: logger,
	}
}

// Init initializes the PostgreSql binding.
func (p *Postgres) Init(ctx context.Context, meta bindings.Metadata) error {
	if p.closed.Load() {
		return errors.New("cannot initialize a previously-closed component")
	}

	m := psqlMetadata{}
	err := m.InitWithMetadata(meta.Properties)
	if err != nil {
		return err
	}

	poolConfig, err := m.GetPgxPoolConfig()
	if err != nil {
		return err
	}

	// This context doesn't control the lifetime of the connection pool, and is
	// only scoped to postgres creating resources at init.
	p.db, err = pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("unable to connect to the DB: %w", err)
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
		return nil, errors.New("invoke request required")
	}

	// We let the "close" operation here succeed even if the component has been closed already
	if req.Operation == closeOperation {
		err = p.Close()
		return nil, err
	}

	if p.closed.Load() {
		return nil, errors.New("component is closed")
	}

	if req.Metadata == nil {
		return nil, errors.New("metadata required")
	}

	// Metadata property "sql" contains the query to execute
	sql := req.Metadata[commandSQLKey]
	if sql == "" {
		return nil, fmt.Errorf("required metadata not set: %s", commandSQLKey)
	}

	// Metadata property "params" contains JSON-encoded parameters, and it's optional
	// If present, it must be unserializable into a []any object
	var args []any
	if argsStr := req.Metadata[commandArgsKey]; argsStr != "" {
		err = json.Unmarshal([]byte(argsStr), &args)
		if err != nil {
			return nil, fmt.Errorf("invalid metadata property %s: failed to unserialize into an array: %w", commandArgsKey, err)
		}
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
		r, err := p.exec(ctx, sql, args...)
		if err != nil {
			return nil, err
		}
		resp.Metadata["rows-affected"] = strconv.FormatInt(r, 10) // 0 if error

	case queryOperation:
		d, err := p.query(ctx, sql, args...)
		if err != nil {
			return nil, err
		}
		resp.Data = d

	default:
		return nil, fmt.Errorf(
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
	if !p.closed.CompareAndSwap(false, true) {
		// If this failed, the component has already been closed
		// We allow multiple calls to close
		return nil
	}

	if p.db != nil {
		p.db.Close()
	}
	p.db = nil

	return nil
}

func (p *Postgres) query(ctx context.Context, sql string, args ...any) (result []byte, err error) {
	rows, err := p.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}

	rs := make([]any, 0)
	for rows.Next() {
		val, rowErr := rows.Values()
		if rowErr != nil {
			return nil, fmt.Errorf("error reading result '%v': %w", rows.Err(), rowErr)
		}
		rs = append(rs, val) //nolint:asasalint
	}

	result, err = json.Marshal(rs)
	if err != nil {
		return nil, fmt.Errorf("error serializing results: %w", err)
	}

	return result, nil
}

func (p *Postgres) exec(ctx context.Context, sql string, args ...any) (result int64, err error) {
	res, err := p.db.Exec(ctx, sql, args...)
	if err != nil {
		return 0, fmt.Errorf("error executing query: %w", err)
	}

	return res.RowsAffected(), nil
}

// GetComponentMetadata returns the metadata of the component.
func (p *Postgres) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := psqlMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}
