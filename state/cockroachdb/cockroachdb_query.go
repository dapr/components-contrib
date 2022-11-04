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

package cockroachdb

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

type Query struct {
	query  string
	params []interface{}
	limit  int
	skip   *int64
}

func (q *Query) VisitEQ(filter *query.EQ) (string, error) {
	return q.whereFieldEqual(filter.Key, filter.Val), nil
}

func (q *Query) VisitIN(filter *query.IN) (string, error) {
	if len(filter.Vals) == 0 {
		return "", fmt.Errorf("empty IN operator for key %q", filter.Key)
	}

	str := "("
	str += q.whereFieldEqual(filter.Key, filter.Vals[0])

	for _, v := range filter.Vals[1:] {
		str += " OR "
		str += q.whereFieldEqual(filter.Key, v)
	}
	str += ")"
	return str, nil
}

func (q *Query) visitFilters(operation string, filters []query.Filter) (string, error) {
	var (
		str string
		err error
	)

	arr := make([]string, len(filters))

	for filterIndex, filter := range filters {
		switch filterType := filter.(type) {
		case *query.EQ:
			str, err = q.VisitEQ(filterType)
		case *query.IN:
			str, err = q.VisitIN(filterType)
		case *query.OR:
			str, err = q.VisitOR(filterType)
		case *query.AND:
			str, err = q.VisitAND(filterType)
		default:
			return "", fmt.Errorf("unsupported filter type %#v", filterType)
		}

		if err != nil {
			return "", err
		}

		arr[filterIndex] = str
	}

	sep := fmt.Sprintf(" %s ", operation)

	return fmt.Sprintf("(%s)", strings.Join(arr, sep)), nil
}

func (q *Query) VisitAND(filter *query.AND) (string, error) {
	return q.visitFilters("AND", filter.Filters)
}

func (q *Query) VisitOR(filter *query.OR) (string, error) {
	return q.visitFilters("OR", filter.Filters)
}

func (q *Query) Finalize(filters string, storeQuery *query.Query) error {
	q.query = fmt.Sprintf("SELECT key, value, etag FROM %s", tableName)

	if filters != "" {
		q.query += fmt.Sprintf(" WHERE %s", filters)
	}

	if len(storeQuery.Sort) > 0 {
		q.query += " ORDER BY "

		for sortIndex, sortItem := range storeQuery.Sort {
			if sortIndex > 0 {
				q.query += ", "
			}
			q.query += translateFieldToFilter(sortItem.Key)
			if sortItem.Order != "" {
				q.query += fmt.Sprintf(" %s", sortItem.Order)
			}
		}
	}

	if storeQuery.Page.Limit > 0 {
		q.query += fmt.Sprintf(" LIMIT %d", storeQuery.Page.Limit)
		q.limit = storeQuery.Page.Limit
	}

	if len(storeQuery.Page.Token) != 0 {
		skip, err := strconv.ParseInt(storeQuery.Page.Token, 10, 64)
		if err != nil {
			return err
		}
		q.query += fmt.Sprintf(" OFFSET %d", skip)
		q.skip = &skip
	}

	return nil
}

func (q *Query) execute(logger logger.Logger, db *sql.DB) ([]state.QueryItem, string, error) {
	rows, err := db.Query(q.query, q.params...)
	if err != nil {
		return nil, "", fmt.Errorf("query executes '%s' failed: %w", q.query, err)
	}
	defer rows.Close()

	ret := []state.QueryItem{}
	for rows.Next() {
		var (
			key  string
			data []byte
			etag int
		)
		if err = rows.Scan(&key, &data, &etag); err != nil {
			return nil, "", fmt.Errorf("read fields from query '%s' failed: %w", q.query, err)
		}
		result := state.QueryItem{
			Key:         key,
			Data:        data,
			ETag:        ptr.Of(strconv.Itoa(etag)),
			Error:       "",
			ContentType: nil,
		}
		ret = append(ret, result)
	}

	if err = rows.Err(); err != nil {
		return nil, "", fmt.Errorf("interation rows from query '%s' failed: %w", q.query, err)
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
	// add preceding "value".
	key = "value." + key

	fieldParts := strings.Split(key, ".")
	filterField := fieldParts[0]
	fieldParts = fieldParts[1:]

	for fieldIndex, fieldPart := range fieldParts {
		filterField += "->"

		if fieldIndex+1 == len(fieldParts) {
			filterField += ">"
		}

		filterField += fmt.Sprintf("'%s'", fieldPart)
	}

	return filterField
}

func (q *Query) whereFieldEqual(key string, value interface{}) string {
	position := q.addParamValueAndReturnPosition(value)
	filterField := translateFieldToFilter(key)
	query := fmt.Sprintf("%s=$%v", filterField, position)
	return query
}
