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

package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
)

type Query struct {
	query  string
	filter interface{}
	opts   *options.FindOptions
}

func (q *Query) VisitEQ(f *query.EQ) (string, error) {
	// { <key>: <val> }
	switch v := f.Val.(type) {
	case string:
		return fmt.Sprintf(`{ "value.%s": %q }`, f.Key, v), nil
	default:
		return fmt.Sprintf(`{ "value.%s": %v }`, f.Key, v), nil
	}
}

func (q *Query) VisitNEQ(f *query.NEQ) (string, error) {
	// { <key>: <val> }
	switch v := f.Val.(type) {
	case string:
		return fmt.Sprintf(`{ "value.%s": {"$ne": %q} }`, f.Key, v), nil
	default:
		return fmt.Sprintf(`{ "value.%s": {"$ne": %v} }`, f.Key, v), nil
	}
}

func (q *Query) VisitGT(f *query.GT) (string, error) {
	// { <key>: <val> }
	switch v := f.Val.(type) {
	case string:
		return "", fmt.Errorf("unsupported type of value %s; string type not permitted", f.Val)
	default:
		return fmt.Sprintf(`{ "value.%s": {"$gt": %v} }`, f.Key, v), nil
	}
}

func (q *Query) VisitGTE(f *query.GTE) (string, error) {
	// { <key>: <val> }
	switch v := f.Val.(type) {
	case string:
		return "", fmt.Errorf("unsupported type of value %s; string type not permitted", f.Val)
	default:
		return fmt.Sprintf(`{ "value.%s": {"$gte": %v} }`, f.Key, v), nil
	}
}

func (q *Query) VisitLT(f *query.LT) (string, error) {
	// { <key>: <val> }
	switch v := f.Val.(type) {
	case string:
		return "", fmt.Errorf("unsupported type of value %s; string type not permitted", f.Val)
	default:
		return fmt.Sprintf(`{ "value.%s": {"$lt": %v} }`, f.Key, v), nil
	}
}

func (q *Query) VisitLTE(f *query.LTE) (string, error) {
	// { <key>: <val> }
	switch v := f.Val.(type) {
	case string:
		return "", fmt.Errorf("unsupported type of value %s; string type not permitted", f.Val)
	default:
		return fmt.Sprintf(`{ "value.%s": {"$lte": %v} }`, f.Key, v), nil
	}
}

func (q *Query) VisitIN(f *query.IN) (string, error) {
	// { $in: [ <val1>, <val2>, ... , <valN> ] }
	if len(f.Vals) == 0 {
		return "", fmt.Errorf("empty IN operator for key %q", f.Key)
	}
	str := fmt.Sprintf(`{ "value.%s": { "$in": [ `, f.Key)

	for i := range len(f.Vals) {
		if i > 0 {
			str += ", "
		}
		switch v := f.Vals[i].(type) {
		case string:
			str += fmt.Sprintf("%q", v)
		default:
			str += fmt.Sprintf("%v", v)
		}
	}
	str += " ] } }"

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

	return fmt.Sprintf(`{ "%s": [ %s ] }`, op, strings.Join(arr, ", ")), nil
}

func (q *Query) VisitAND(f *query.AND) (string, error) {
	// { $and: [ { <expression1> }, { <expression2> } , ... , { <expressionN> } ] }
	return q.visitFilters("$and", f.Filters)
}

func (q *Query) VisitOR(f *query.OR) (string, error) {
	// { $or: [ { <expression1> }, { <expression2> } , ... , { <expressionN> } ] }
	return q.visitFilters("$or", f.Filters)
}

func (q *Query) Finalize(filters string, qq *query.Query) error {
	q.query = filters
	if len(filters) == 0 {
		q.filter = bson.D{}
	} else if err := bson.UnmarshalExtJSON([]byte(filters), false, &q.filter); err != nil {
		return err
	}
	q.opts = options.Find()

	// sorting
	if len(qq.Sort) > 0 {
		sort := bson.D{}
		for _, s := range qq.Sort {
			order := 1 // ascending
			if s.Order == query.DESC {
				order = -1
			}
			sort = append(sort, bson.E{Key: "value." + s.Key, Value: order})
		}
		q.opts.SetSort(sort)
	}
	// pagination
	if qq.Page.Limit > 0 {
		q.opts.SetLimit(int64(qq.Page.Limit))
	}
	if len(qq.Page.Token) != 0 {
		skip, err := strconv.ParseInt(qq.Page.Token, 10, 64)
		if err != nil {
			return err
		}
		q.opts.SetSkip(skip)
	}

	return nil
}

func (q *Query) execute(ctx context.Context, collection *mongo.Collection) ([]state.QueryItem, string, error) {
	cur, err := collection.Find(ctx, q.filter, []*options.FindOptions{q.opts}...)
	if err != nil {
		return nil, "", err
	}
	defer cur.Close(ctx)
	ret := []state.QueryItem{}
	for cur.Next(ctx) {
		var item Item
		if err = cur.Decode(&item); err != nil {
			return nil, "", err
		}
		result := state.QueryItem{
			Key:  item.Key,
			ETag: &item.Etag,
		}

		switch obj := item.Value.(type) {
		case string:
			result.Data = []byte(obj)
		case primitive.D:
			// Setting canonical to `false`.
			// See https://docs.mongodb.com/manual/reference/mongodb-extended-json/#bson-data-types-and-associated-representations
			// Having bson marshalled into Relaxed JSON instead of canonical JSON, this way type preservation is lost but
			// interoperability is preserved
			// See https://mongodb.github.io/swift-bson/docs/current/SwiftBSON/json-interop.html
			// A decimal value stored as BSON will be returned as {"d": 5.5} if canonical is set to false instead of
			// {"d": {"$numberDouble": 5.5}} when canonical JSON is returned.
			if result.Data, err = bson.MarshalExtJSON(obj, false, true); err != nil {
				result.Error = err.Error()
			}
		default:
			if result.Data, err = json.Marshal(item.Value); err != nil {
				result.Error = err.Error()
			}
		}
		ret = append(ret, result)
	}
	if err = cur.Err(); err != nil {
		return nil, "", err
	}
	// set next query token only if limit is specified
	var token string
	if q.opts.Limit != nil && *q.opts.Limit != 0 {
		var skip int64
		if q.opts.Skip != nil {
			skip = *q.opts.Skip
		}
		token = strconv.FormatInt(skip+int64(len(ret)), 10)
	}

	return ret, token, nil
}
