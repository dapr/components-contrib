// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cosmosdb

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/a8m/documentdb"
	"github.com/agrea/ptr"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
)

type Query struct {
	query documentdb.Query
	limit int
	token string
}

func (q *Query) VisitEQ(f *query.EQ) (string, error) {
	// <key> = <val>
	val, ok := f.Val.(string)
	if !ok {
		return "", fmt.Errorf("unsupported type of value %#v; expected string", f.Val)
	}
	name := q.setNextParameter(val)

	return fmt.Sprintf("%s = %s", replaceKeywords("c."+f.Key), name), nil
}

func (q *Query) VisitIN(f *query.IN) (string, error) {
	// <key> IN ( <val1>, <val2>, ... , <valN> )
	if len(f.Vals) == 0 {
		return "", fmt.Errorf("empty IN operator for key %q", f.Key)
	}
	names := make([]string, len(f.Vals))
	for i, v := range f.Vals {
		val, ok := v.(string)
		if !ok {
			return "", fmt.Errorf("unsupported type of value %#v; expected string", v)
		}
		names[i] = q.setNextParameter(val)
	}

	return fmt.Sprintf("%s IN (%s)", replaceKeywords("c."+f.Key), strings.Join(names, ", ")), nil
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
		case *query.IN:
			if str, err = q.VisitIN(f); err != nil {
				return "", err
			}
			arr = append(arr, str)
		case *query.OR:
			if str, err = q.VisitOR(f); err != nil {
				return "", err
			}
			arr = append(arr, "("+str+")")
		case *query.AND:
			if str, err = q.VisitAND(f); err != nil {
				return "", err
			}
			arr = append(arr, "("+str+")")
		default:
			return "", fmt.Errorf("unsupported filter type %#v", f)
		}
	}

	return strings.Join(arr, " "+op+" "), nil
}

func (q *Query) VisitAND(f *query.AND) (string, error) {
	// <expression1> AND <expression2> AND ... AND <expressionN>
	return q.visitFilters("AND", f.Filters)
}

func (q *Query) VisitOR(f *query.OR) (string, error) {
	// <expression1> OR <expression2> OR ... OR <expressionN>
	return q.visitFilters("OR", f.Filters)
}

func (q *Query) Finalize(filters string, qq *query.Query) error {
	var filter, orderBy string
	if len(filters) != 0 {
		filter = fmt.Sprintf(" WHERE %s", filters)
	}
	if sz := len(qq.Sort); sz != 0 {
		order := make([]string, sz)
		for i, item := range qq.Sort {
			if item.Order == query.DESC {
				order[i] = fmt.Sprintf("%s DESC", replaceKeywords("c."+item.Key))
			} else {
				order[i] = fmt.Sprintf("%s ASC", replaceKeywords("c."+item.Key))
			}
		}
		orderBy = fmt.Sprintf(" ORDER BY %s", strings.Join(order, ", "))
	}
	q.query.Query = fmt.Sprintf("SELECT * FROM c%s%s", filter, orderBy)
	q.limit = qq.Page.Limit
	q.token = qq.Page.Token

	return nil
}

func (q *Query) setNextParameter(val string) string {
	pname := fmt.Sprintf("@__param__%d__", len(q.query.Parameters))
	q.query.Parameters = append(q.query.Parameters, documentdb.Parameter{Name: pname, Value: val})

	return pname
}

func (q *Query) execute(client *documentdb.DocumentDB, collection *documentdb.Collection) ([]state.QueryItem, string, error) {
	opts := []documentdb.CallOption{documentdb.CrossPartition()}
	if q.limit != 0 {
		opts = append(opts, documentdb.Limit(q.limit))
	}
	if len(q.token) != 0 {
		opts = append(opts, documentdb.Continuation(q.token))
	}
	items := []CosmosItem{}
	resp, err := client.QueryDocuments(collection.Self, &q.query, &items, opts...)
	if err != nil {
		return nil, "", err
	}
	token := resp.Header.Get(documentdb.HeaderContinuation)

	ret := make([]state.QueryItem, len(items))
	for i := range items {
		ret[i].Key = items[i].ID
		ret[i].ETag = ptr.String(items[i].Etag)

		if items[i].IsBinary {
			ret[i].Data, _ = base64.StdEncoding.DecodeString(items[i].Value.(string))

			continue
		}
		ret[i].Data, err = jsoniter.ConfigFastest.Marshal(&items[i].Value)
		if err != nil {
			ret[i].Error = err.Error()

			continue
		}
	}

	return ret, token, nil
}

func replaceKeywords(key string) string {
	reserved := []string{"value"}

	for _, keyword := range reserved {
		key = replaceKeyword(key, keyword)
	}

	return key
}

func replaceKeyword(key, keyword string) string {
	indx := strings.Index(strings.ToUpper(key), "."+strings.ToUpper(keyword))
	if indx == -1 {
		return key
	}
	nextIndx := indx + len(keyword) + 1
	if nextIndx == len(key) || !isLetter(key[nextIndx]) {
		return fmt.Sprintf("%s['%s']%s", key[:indx], key[indx+1:nextIndx], replaceKeyword(key[nextIndx:], keyword))
	}

	return key
}

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}
