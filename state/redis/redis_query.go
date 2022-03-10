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

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"

	"github.com/go-redis/redis/v8"
)

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
	// @<key>:(<val>)
	alias, err := q.getAlias(f.Key)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("@%s:(%s)", alias, f.Val), nil
}

func (q *Query) VisitIN(f *query.IN) (string, error) {
	// @<key>:(<val1>|<val2>...)
	if len(f.Vals) == 0 {
		return "", fmt.Errorf("empty IN operator for key %q", f.Key)
	}
	alias, err := q.getAlias(f.Key)
	if err != nil {
		return "", err
	}
	vals := make([]string, len(f.Vals))
	for i := range f.Vals {
		vals[i] = f.Vals[i].(string)
	}
	str := fmt.Sprintf("@%s:(%s)", alias, strings.Join(vals, "|"))

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
			return errors.New("multiple SORTBY steps are not allowed. Sort multiple fields in a single step")
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
			q.query = append(q.query, "LIMIT", qq.Page.Token, fmt.Sprintf("%d", q.limit))
		} else {
			q.offset = 0
			q.query = append(q.query, "LIMIT", "0", fmt.Sprintf("%d", q.limit))
		}
	}

	return nil
}

func (q *Query) execute(ctx context.Context, client redis.UniversalClient) ([]state.QueryItem, string, error) {
	query := append(append([]interface{}{"FT.SEARCH", q.schemaName}, q.query...), "RETURN", "2", "$.data", "$.version")
	ret, err := client.Do(ctx, query...).Result()
	if err != nil {
		return nil, "", err
	}
	arr, ok := ret.([]interface{})
	if !ok {
		return nil, "", fmt.Errorf("invalid output")
	}
	// arr[0] = number of matching elements in DB (ignoring pagination)
	// arr[2n] = key
	// arr[2n+1][0] = "$.data"
	// arr[2n+1][1] = value
	// arr[2n+1][2] = "$.version"
	// arr[2n+1][3] = etag
	if len(arr)%2 != 1 {
		return nil, "", fmt.Errorf("invalid output")
	}
	res := []state.QueryItem{}
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
	// set next query token only if limit is specified
	var token string
	if q.limit > 0 && len(res) > 0 {
		token = strconv.FormatInt(q.offset+int64(len(res)), 10)
	}

	return res, token, err
}
