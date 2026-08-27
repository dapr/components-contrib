/*
Copyright 2026 The Dapr Authors
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

package sqlserver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	// list of operations.
	execOperation  bindings.OperationKind = "exec"
	queryOperation bindings.OperationKind = "query"
	closeOperation bindings.OperationKind = "close"

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

type SQLServer struct {
	db     *sql.DB
	logger logger.Logger
	closed atomic.Bool
}

// NewSQLServer returns a new SQL Server output binding.
func NewSQLServer(logger logger.Logger) bindings.OutputBinding {
	return &SQLServer{
		logger: logger,
	}
}

// Init initializes the SQL Server binding.
func (s *SQLServer) Init(ctx context.Context, md bindings.Metadata) error {
	if s.closed.Load() {
		return errors.New("cannot initialize a previously-closed component")
	}

	meta := sqlServerMetadata{}
	err := meta.Parse(md.Properties)
	if err != nil {
		return err
	}

	// The caller's connection string is used as-is (including Azure AD auth if configured).
	connector, _, err := meta.GetConnector(false)
	if err != nil {
		return err
	}

	s.db = sql.OpenDB(connector)

	if meta.MaxIdleConns > 0 {
		s.db.SetMaxIdleConns(meta.MaxIdleConns)
	}
	if meta.MaxOpenConns > 0 {
		s.db.SetMaxOpenConns(meta.MaxOpenConns)
	}
	if meta.ConnMaxIdleTime > 0 {
		s.db.SetConnMaxIdleTime(meta.ConnMaxIdleTime)
	}
	if meta.ConnMaxLifetime > 0 {
		s.db.SetConnMaxLifetime(meta.ConnMaxLifetime)
	}

err = s.db.PingContext(ctx)
	if err != nil {
		_ = s.db.Close()
		s.db = nil
		return fmt.Errorf("unable to ping the DB: %w", err)
	}

	return nil
}

// Invoke handles all invoke operations.
func (s *SQLServer) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	if req == nil {
		return nil, errors.New("invoke request required")
	}

	// We let the "close" operation here succeed even if the component has been closed already
	if req.Operation == closeOperation {
		return nil, s.Close()
	}

	if s.closed.Load() {
		return nil, errors.New("component is closed")
	}

	if req.Metadata == nil {
		return nil, errors.New("metadata required")
	}

	q := req.Metadata[commandSQLKey]
	if q == "" {
		return nil, fmt.Errorf("required metadata not set: %s", commandSQLKey)
	}

	// Metadata property "params" contains JSON-encoded parameters, and it's optional
	// If present, it must be unserializable into a []any object
	var (
		params []any
		err    error
	)
	if paramsStr := req.Metadata[commandParamsKey]; paramsStr != "" {
		// Decode with UseNumber so JSON numbers aren't collapsed into float64,
		// which would silently lose precision for large integer parameters.
		decoder := json.NewDecoder(strings.NewReader(paramsStr))
		decoder.UseNumber()
		err = decoder.Decode(&params)
		if err != nil {
			return nil, fmt.Errorf("invalid metadata property %s: failed to unserialize into an array: %w", commandParamsKey, err)
		}

		err = normalizeNumberParams(params)
		if err != nil {
			return nil, fmt.Errorf("invalid metadata property %s: %w", commandParamsKey, err)
		}
	}

	startTime := time.Now().UTC()
	resp := &bindings.InvokeResponse{
		Metadata: map[string]string{
			respOpKey:        string(req.Operation),
			respSQLKey:       q,
			respStartTimeKey: startTime.Format(time.RFC3339Nano),
		},
	}

	switch req.Operation {
	case execOperation:
		r, err := s.exec(ctx, q, params...)
		if err != nil {
			return nil, err
		}
		resp.Metadata[respRowsAffectedKey] = strconv.FormatInt(r, 10)

	case queryOperation:
		d, err := s.query(ctx, q, params...)
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

// Operations returns list of operations supported by the SQL Server binding.
func (s *SQLServer) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		execOperation,
		queryOperation,
		closeOperation,
	}
}

// Close will close the DB.
func (s *SQLServer) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		// If this failed, the component has already been closed
		// We allow multiple calls to close
		return nil
	}

	if s.db != nil {
		_ = s.db.Close()
		s.db = nil
	}

	return nil
}

func (s *SQLServer) query(ctx context.Context, sqlQuery string, params ...any) ([]byte, error) {
	rows, err := s.db.QueryContext(ctx, sqlQuery, params...)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()

	result, err := s.jsonify(rows)
	if err != nil {
		return nil, fmt.Errorf("error marshalling query result for query: %w", err)
	}

	return result, nil
}

// normalizeNumberParams replaces each json.Number (produced by decoding with
// UseNumber) with an int64 when it represents a whole number, or a float64
// otherwise. Without this, query parameters that are whole numbers would be
// sent to the driver as float64, which loses precision for large integers
// (e.g. BIGINT values beyond 2^53).
func normalizeNumberParams(params []any) error {
	for i, p := range params {
		n, ok := p.(json.Number)
		if !ok {
			continue
		}

		if i64, err := n.Int64(); err == nil {
			params[i] = i64
			continue
		}

		f64, err := n.Float64()
		if err != nil {
			return fmt.Errorf("invalid number %q at index %d: %w", n.String(), i, err)
		}
		params[i] = f64
	}

	return nil
}

func (s *SQLServer) exec(ctx context.Context, sqlQuery string, params ...any) (int64, error) {
	res, err := s.db.ExecContext(ctx, sqlQuery, params...)
	if err != nil {
		return 0, fmt.Errorf("error executing query: %w", err)
	}

	return res.RowsAffected()
}

func (s *SQLServer) jsonify(rows *sql.Rows) ([]byte, error) {
      columnTypes, err := rows.ColumnTypes()
      if err != nil {
              return nil, err
      }

      var ret []any
      for rows.Next() {
              values := make([]any, len(columnTypes))
              for i := range values {
                      values[i] = new(any)
              }
              err = rows.Scan(values...)
              if err != nil {
                      return nil, err
              }

              r := s.convert(columnTypes, values)
              ret = append(ret, r)
      }
      if err := rows.Err(); err != nil {
              return nil, err
      }

      return json.Marshal(ret)
}

func (s *SQLServer) convert(columnTypes []*sql.ColumnType, values []any) map[string]any {
      r := map[string]any{}

      for i, ct := range columnTypes {
              value := *(values[i].(*any))
              if value == nil {
                      continue
              }

              // go-mssqldb returns DECIMAL, NUMERIC, MONEY and SMALLMONEY values as a
              // []byte holding the number's string form; convert to string so JSON
              // marshalling does not base64-encode it.
              if b, ok := value.([]byte); ok {
                      switch ct.DatabaseTypeName() {
                      case "DECIMAL", "MONEY", "SMALLMONEY":
                              value = string(b)
                      }
              }

              r[ct.Name()] = value
      }

      return r
}

// GetComponentMetadata returns the metadata of the component.
func (s *SQLServer) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := sqlServerMetadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}
