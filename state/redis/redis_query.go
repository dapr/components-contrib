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

package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	rediscomponent "github.com/dapr/components-contrib/common/component/redis"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
)

var ErrMultipleSortBy error = errors.New("multiple SORTBY steps are not allowed. Sort multiple fields in a single step")

type Query struct {
	schemaName string
	aliases    map[string]string
	query      []interface{}
	limit      int
	offset     int64
}

func NewQuery(schemaName string, aliases map[string]string) *Query {
	return &Query{
		schemaName: schemaName,
		aliases:    aliases,
	}
}

func (q *Query) getAlias(jsonPath string) (string, error) {
	alias, ok := q.aliases[jsonPath]
	if !ok {
		return "", fmt.Errorf("JSON path %q is not indexed", jsonPath)
	}
	return alias, nil
}

func (q *Query) VisitEQ(f *query.EQ) (string, error) {
	// string:  @<key>:(<val>)
	// numeric: @<key>:[<val> <val>]
	alias, err := q.getAlias(f.Key)
	if err != nil {
		return "", err
	}

	switch v := f.Val.(type) {
	case string:
		return fmt.Sprintf("@%s:(%s)", alias, v), nil
	case float64, float32:
		return fmt.Sprintf("@%s:[%f %f]", alias, v, v), nil
	default:
		return fmt.Sprintf("@%s:[%d %d]", alias, v, v), nil
	}
}

func (q *Query) VisitNEQ(f *query.NEQ) (string, error) {
	// string:  @<key>:(<val>)
	// numeric: @<key>:[<val> <val>]
	alias, err := q.getAlias(f.Key)
	if err != nil {
		return "", err
	}

	switch v := f.Val.(type) {
	case string:
		return fmt.Sprintf("@%s:(%s)", alias, v), nil
	case float64, float32:
		return fmt.Sprintf("@%s:[%f %f]", alias, v, v), nil
	default:
		return fmt.Sprintf("@%s:[%d %d]", alias, v, v), nil
	}
}

func (q *Query) VisitGT(f *query.GT) (string, error) {
	// numeric: @<key>:[(<val> +inf]
	alias, err := q.getAlias(f.Key)
	if err != nil {
		return "", err
	}

	switch v := f.Val.(type) {
	case string:
		return "", fmt.Errorf("unsupported type of value %s; string type not permitted", f.Val)
	case float64, float32:
		return fmt.Sprintf("@%s:[(%f +inf]", alias, v), nil
	default:
		return fmt.Sprintf("@%s:[(%d +inf]", alias, v), nil
	}
}

func (q *Query) VisitGTE(f *query.GTE) (string, error) {
	// numeric: @<key>:[<val> +inf]
	alias, err := q.getAlias(f.Key)
	if err != nil {
		return "", err
	}

	switch v := f.Val.(type) {
	case string:
		return "", fmt.Errorf("unsupported type of value %s; string type not permitted", f.Val)
	case float64, float32:
		return fmt.Sprintf("@%s:[%f +inf]", alias, v), nil
	default:
		return fmt.Sprintf("@%s:[%d +inf]", alias, v), nil
	}
}

func (q *Query) VisitLT(f *query.LT) (string, error) {
	// numeric: @<key>:[-inf <val>)]
	alias, err := q.getAlias(f.Key)
	if err != nil {
		return "", err
	}

	switch v := f.Val.(type) {
	case string:
		return "", fmt.Errorf("unsupported type of value %s; string type not permitted", f.Val)
	case float64, float32:
		return fmt.Sprintf("@%s:[-inf (%f]", alias, v), nil
	default:
		return fmt.Sprintf("@%s:[-inf (%d]", alias, v), nil
	}
}

func (q *Query) VisitLTE(f *query.LTE) (string, error) {
	// numeric: @<key>:[-inf <val>]
	alias, err := q.getAlias(f.Key)
	if err != nil {
		return "", err
	}
	switch v := f.Val.(type) {
	case string:
		return "", fmt.Errorf("unsupported type of value %s; string type not permitted", f.Val)
	case float64, float32:
		return fmt.Sprintf("@%s:[-inf %f]", alias, v), nil
	default:
		return fmt.Sprintf("@%s:[-inf %d]", alias, v), nil
	}
}

func (q *Query) VisitIN(f *query.IN) (string, error) {
	// string:  @<key>:(<val1>|<val2>...)
	// numeric: replace with OR
	n := len(f.Vals)
	if n < 2 {
		return "", fmt.Errorf("too few values in IN operator for key %q", f.Key)
	}

	switch f.Vals[0].(type) {
	case string:
		alias, err := q.getAlias(f.Key)
		if err != nil {
			return "", err
		}
		vals := make([]string, n)
		for i := range n {
			vals[i] = f.Vals[i].(string)
		}
		str := fmt.Sprintf("@%s:(%s)", alias, strings.Join(vals, "|"))

		return str, nil

	default:
		or := &query.OR{
			Filters: make([]query.Filter, n),
		}
		for i := range n {
			or.Filters[i] = &query.EQ{
				Key: f.Key,
				Val: f.Vals[i],
			}
		}

		return q.VisitOR(or)
	}
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
			arr = append(arr, fmt.Sprintf("(%s)", str))
		case *query.NEQ:
			if str, err = q.VisitNEQ(f); err != nil {
				return "", err
			}
			arr = append(arr, fmt.Sprintf("-(%s)", str))
		case *query.GT:
			if str, err = q.VisitGT(f); err != nil {
				return "", err
			}
			arr = append(arr, fmt.Sprintf("(%s)", str))
		case *query.GTE:
			if str, err = q.VisitGTE(f); err != nil {
				return "", err
			}
			arr = append(arr, fmt.Sprintf("(%s)", str))
		case *query.LT:
			if str, err = q.VisitLT(f); err != nil {
				return "", err
			}
			arr = append(arr, fmt.Sprintf("(%s)", str))
		case *query.LTE:
			if str, err = q.VisitLTE(f); err != nil {
				return "", err
			}
			arr = append(arr, fmt.Sprintf("(%s)", str))
		case *query.IN:
			if str, err = q.VisitIN(f); err != nil {
				return "", err
			}
			arr = append(arr, fmt.Sprintf("(%s)", str))
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

	return fmt.Sprintf("(%s)", strings.Join(arr, op)), nil
}

func (q *Query) VisitAND(f *query.AND) (string, error) {
	// ( <expression1> <expression2> ... <expressionN> )
	return q.visitFilters(" ", f.Filters)
}

func (q *Query) VisitOR(f *query.OR) (string, error) {
	// ( <expression1> | <expression2> | ... | <expressionN> )
	return q.visitFilters("|", f.Filters)
}

func (q *Query) Finalize(filters string, qq *query.Query) error {
	if len(filters) == 0 {
		filters = "*"
	}
	q.query = []interface{}{filters}

	// sorting
	if len(qq.Sort) > 0 {
		if len(qq.Sort) != 1 {
			return ErrMultipleSortBy
		}
		alias, err := q.getAlias(qq.Sort[0].Key)
		if err != nil {
			return err
		}
		q.query = append(q.query, "SORTBY", alias)
		if qq.Sort[0].Order == query.DESC {
			q.query = append(q.query, "DESC")
		}
	}
	// pagination
	if qq.Page.Limit > 0 {
		q.limit = qq.Page.Limit
		q.offset = 0
		if len(qq.Page.Token) != 0 {
			var err error
			q.offset, err = strconv.ParseInt(qq.Page.Token, 10, 64)
			if err != nil {
				return err
			}
			q.query = append(q.query, "LIMIT", qq.Page.Token, strconv.Itoa(q.limit))
		} else {
			q.offset = 0
			q.query = append(q.query, "LIMIT", "0", strconv.Itoa(q.limit))
		}
	}

	return nil
}

func (q *Query) execute(ctx context.Context, client rediscomponent.RedisClient) ([]state.QueryItem, string, error) {
	query := append(append([]interface{}{"FT.SEARCH", q.schemaName}, q.query...), "RETURN", "2", "$.data", "$.version")
	ret, err := client.DoRead(ctx, query...)
	if err != nil {
		return nil, "", err
	}

	res, ok, err := parseQueryResponsePost28(ret)
	if err != nil {
		return nil, "", err
	}
	if !ok {
		res, err = parseQueryResponsePre28(ret)
		if err != nil {
			return nil, "", err
		}
	}

	// set next query token only if limit is specified
	var token string
	if q.limit > 0 && len(res) > 0 {
		token = strconv.FormatInt(q.offset+int64(len(res)), 10)
	}

	return res, token, err
}

// parseQueryResponsePost28 parses the query Do response from redisearch 2.8+.
func parseQueryResponsePost28(ret any) ([]state.QueryItem, bool, error) {
	aarr, ok := ret.(map[any]any)
	if !ok {
		return nil, false, nil
	}

	var res []state.QueryItem //nolint:prealloc
	arr := aarr["results"].([]any)
	if len(arr) == 0 {
		return nil, false, errors.New("invalid output")
	}
	for i := range arr {
		inner, ok := arr[i].(map[any]any)
		if !ok {
			return nil, false, errors.New("invalid output")
		}
		exattr, ok := inner["extra_attributes"].(map[any]any)
		if !ok {
			return nil, false, errors.New("invalid output")
		}
		item := state.QueryItem{
			Key: inner["id"].(string),
		}
		if data, ok := exattr["$.data"].(string); ok {
			item.Data = []byte(data)
		} else {
			item.Error = fmt.Sprintf("%#v is not string", exattr["$.data"])
		}
		if etag, ok := exattr["$.version"].(string); ok {
			item.ETag = &etag
		}
		res = append(res, item)
	}

	return res, true, nil
}

// parseQueryResponsePre28 parses the query Do response from redisearch 2.8-.
func parseQueryResponsePre28(ret any) ([]state.QueryItem, error) {
	arr, ok := ret.([]any)
	if !ok {
		return nil, errors.New("invalid output")
	}

	// arr[0] = number of matching elements in DB (ignoring pagination
	// arr[2n] = key
	// arr[2n+1][0] = "$.data"
	// arr[2n+1][1] = value
	// arr[2n+1][2] = "$.version"
	// arr[2n+1][3] = etag
	if len(arr)%2 != 1 {
		return nil, errors.New("invalid output")
	}

	var res []state.QueryItem
	for i := 1; i < len(arr); i += 2 {
		item := state.QueryItem{
			Key: arr[i].(string),
		}
		if data, ok := arr[i+1].([]interface{}); ok && len(data) == 4 && data[0] == "$.data" && data[2] == "$.version" {
			item.Data = []byte(data[1].(string))
			etag := data[3].(string)
			item.ETag = &etag
		} else {
			item.Error = fmt.Sprintf("%#v is not []interface{}", arr[i+1])
		}
		res = append(res, item)
	}

	return res, nil
}
