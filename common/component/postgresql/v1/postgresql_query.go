/*
Copyright 2022 The Dapr Authors
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
package postgresql

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	pginterfaces "github.com/dapr/components-contrib/common/component/postgresql/interfaces"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

// PostgreSQLQuery extends PostgreSQL to add querying capabilities
type PostgreSQLQuery struct {
	PostgreSQL
}

// NewPostgreSQLQueryStateStore creates a new instance of PostgreSQL state store.
func NewPostgreSQLQueryStateStore(logger logger.Logger, opts Options) state.Store {
	s := &PostgreSQLQuery{
		PostgreSQL: PostgreSQL{
			logger:        logger,
			migrateFn:     opts.MigrateFn,
			setQueryFn:    opts.SetQueryFn,
			etagColumn:    opts.ETagColumn,
			enableAzureAD: opts.EnableAzureAD,
		},
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

// Features returns the features available in this component.
func (p *PostgreSQLQuery) Features() []state.Feature {
	return []state.Feature{
		state.FeatureETag,
		state.FeatureTransactional,
		state.FeatureQueryAPI,
		state.FeatureTTL,
	}
}

// Query executes a query against store.
func (p *PostgreSQLQuery) Query(parentCtx context.Context, req *state.QueryRequest) (*state.QueryResponse, error) {
	q := &Query{
		query:      "",
		params:     []any{},
		tableName:  p.metadata.TableName,
		etagColumn: p.etagColumn,
	}
	qbuilder := query.NewQueryBuilder(q)
	if err := qbuilder.BuildQuery(&req.Query); err != nil {
		return &state.QueryResponse{}, err
	}
	data, token, err := q.execute(parentCtx, p.db)
	if err != nil {
		return &state.QueryResponse{}, err
	}

	return &state.QueryResponse{
		Results: data,
		Token:   token,
	}, nil
}

type Query struct {
	query      string
	params     []interface{}
	limit      int
	skip       *int64
	tableName  string
	etagColumn string
}

func (q *Query) VisitEQ(f *query.EQ) (string, error) {
	return q.whereFieldEqual(f.Key, f.Val), nil
}

func (q *Query) VisitNEQ(f *query.NEQ) (string, error) {
	return q.whereFieldNotEqual(f.Key, f.Val), nil
}

func (q *Query) VisitGT(f *query.GT) (string, error) {
	switch v := f.Val.(type) {
	case string:
		return "", fmt.Errorf("unsupported type of value %s; string type not permitted", v)
	default:
		return q.whereFieldGreaterThan(f.Key, v), nil
	}
}

func (q *Query) VisitGTE(f *query.GTE) (string, error) {
	switch v := f.Val.(type) {
	case string:
		return "", fmt.Errorf("unsupported type of value %s; string type not permitted", v)
	default:
		return q.whereFieldGreaterThanEqual(f.Key, v), nil
	}
}

func (q *Query) VisitLT(f *query.LT) (string, error) {
	switch v := f.Val.(type) {
	case string:
		return "", fmt.Errorf("unsupported type of value %s; string type not permitted", v)
	default:
		return q.whereFieldLessThan(f.Key, v), nil
	}
}

func (q *Query) VisitLTE(f *query.LTE) (string, error) {
	switch v := f.Val.(type) {
	case string:
		return "", fmt.Errorf("unsupported type of value %s; string type not permitted", v)
	default:
		return q.whereFieldLessThanEqual(f.Key, v), nil
	}
}

func (q *Query) VisitIN(f *query.IN) (string, error) {
	if len(f.Vals) == 0 {
		return "", fmt.Errorf("empty IN operator for key %q", f.Key)
	}

	str := "("
	str += q.whereFieldEqual(f.Key, f.Vals[0])

	for _, v := range f.Vals[1:] {
		str += " OR "
		str += q.whereFieldEqual(f.Key, v)
	}
	str += ")"
	return str, nil
}

func (q *Query) visitFilters(op string, filters []query.Filter) (string, error) {
	var (
		arr []string
		str string
		err error
	)

	for _, fil := range filters {
		switch f := fil.(type) {
		case *query.EQ:
			if str, err = q.VisitEQ(f); err != nil {
				return "", err
			}
			arr = append(arr, str)
		case *query.NEQ:
			if str, err = q.VisitNEQ(f); err != nil {
				return "", err
			}
			arr = append(arr, str)
		case *query.GT:
			if str, err = q.VisitGT(f); err != nil {
				return "", err
			}
			arr = append(arr, str)
		case *query.GTE:
			if str, err = q.VisitGTE(f); err != nil {
				return "", err
			}
			arr = append(arr, str)
		case *query.LT:
			if str, err = q.VisitLT(f); err != nil {
				return "", err
			}
			arr = append(arr, str)
		case *query.LTE:
			if str, err = q.VisitLTE(f); err != nil {
				return "", err
			}
			arr = append(arr, str)
		case *query.IN:
			if str, err = q.VisitIN(f); err != nil {
				return "", err
			}
			arr = append(arr, str)
		case *query.OR:
			if str, err = q.VisitOR(f); err != nil {
				return "", err
			}
			arr = append(arr, str)
		case *query.AND:
			if str, err = q.VisitAND(f); err != nil {
				return "", err
			}
			arr = append(arr, str)
		default:
			return "", fmt.Errorf("unsupported filter type %#v", f)
		}
	}

	sep := " " + op + " "

	return "(" + strings.Join(arr, sep) + ")", nil
}

func (q *Query) VisitAND(f *query.AND) (string, error) {
	return q.visitFilters("AND", f.Filters)
}

func (q *Query) VisitOR(f *query.OR) (string, error) {
	return q.visitFilters("OR", f.Filters)
}

func (q *Query) Finalize(filters string, qq *query.Query) error {
	q.query = fmt.Sprintf("SELECT key, value, %s as etag FROM "+q.tableName, q.etagColumn)

	if filters != "" {
		q.query += " WHERE " + filters
	}

	if len(qq.Sort) > 0 {
		q.query += " ORDER BY "

		for sortIndex, sortItem := range qq.Sort {
			if sortIndex > 0 {
				q.query += ", "
			}
			q.query += translateFieldToFilter(sortItem.Key)
			if sortItem.Order != "" {
				q.query += " " + sortItem.Order
			}
		}
	}

	if qq.Page.Limit > 0 {
		q.query += " LIMIT " + strconv.Itoa(qq.Page.Limit)
		q.limit = qq.Page.Limit
	}

	if len(qq.Page.Token) != 0 {
		skip, err := strconv.ParseInt(qq.Page.Token, 10, 64)
		if err != nil {
			return err
		}
		q.query += " OFFSET " + strconv.FormatInt(skip, 10)
		q.skip = &skip
	}

	return nil
}

func (q *Query) execute(ctx context.Context, db pginterfaces.DBQuerier) ([]state.QueryItem, string, error) {
	rows, err := db.Query(ctx, q.query, q.params...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	ret := []state.QueryItem{}
	for rows.Next() {
		var (
			key  string
			data []byte
			etag uint32
		)
		if err = rows.Scan(&key, &data, &etag); err != nil {
			return nil, "", err
		}
		result := state.QueryItem{
			Key:  key,
			Data: data,
			ETag: ptr.Of(strconv.FormatUint(uint64(etag), 10)),
		}
		ret = append(ret, result)
	}

	if err = rows.Err(); err != nil {
		return nil, "", err
	}

	var token string
	if q.limit != 0 {
		var skip int64
		if q.skip != nil {
			skip = *q.skip
		}
		token = strconv.FormatInt(skip+int64(len(ret)), 10)
	}

	return ret, token, nil
}

func (q *Query) addParamValueAndReturnPosition(value interface{}) int {
	q.params = append(q.params, fmt.Sprintf("%v", value))
	return len(q.params)
}

func translateFieldToFilter(key string) string {
	// add preceding "value"
	key = "value." + key

	fieldParts := strings.Split(key, ".")
	filterField := fieldParts[0]
	fieldParts = fieldParts[1:]

	for fieldIndex, fieldPart := range fieldParts {
		filterField += "->"

		if fieldIndex+1 == len(fieldParts) {
			filterField += ">"
		}

		filterField += "'" + fieldPart + "'"
	}

	return filterField
}

func (q *Query) whereFieldEqual(key string, value interface{}) string {
	position := q.addParamValueAndReturnPosition(value)
	filterField := translateFieldToFilter(key)
	query := filterField + "=$" + strconv.Itoa(position)
	return query
}

func (q *Query) whereFieldNotEqual(key string, value interface{}) string {
	position := q.addParamValueAndReturnPosition(value)
	filterField := translateFieldToFilter(key)
	query := filterField + "!=$" + strconv.Itoa(position)
	return query
}

func (q *Query) whereFieldGreaterThan(key string, value interface{}) string {
	position := q.addParamValueAndReturnPosition(value)
	filterField := translateFieldToFilter(key)
	query := filterField + ">$" + strconv.Itoa(position)
	return query
}

func (q *Query) whereFieldGreaterThanEqual(key string, value interface{}) string {
	position := q.addParamValueAndReturnPosition(value)
	filterField := translateFieldToFilter(key)
	query := filterField + ">=$" + strconv.Itoa(position)
	return query
}

func (q *Query) whereFieldLessThan(key string, value interface{}) string {
	position := q.addParamValueAndReturnPosition(value)
	filterField := translateFieldToFilter(key)
	query := filterField + "<$" + strconv.Itoa(position)
	return query
}

func (q *Query) whereFieldLessThanEqual(key string, value interface{}) string {
	position := q.addParamValueAndReturnPosition(value)
	filterField := translateFieldToFilter(key)
	query := filterField + "<=$" + strconv.Itoa(position)
	return query
}
