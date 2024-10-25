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

package cosmosdb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
)

// Internal query object is created here since azcosmos has no notion of a query object
type InternalQuery struct {
	query      string
	parameters []azcosmos.QueryParameter
}

type Query struct {
	query        InternalQuery
	limit        int
	token        string
	partitionKey string
}

func (q *Query) VisitEQ(f *query.EQ) (string, error) {
	// <key> = <val>
	val, ok := f.Val.(string)
	if !ok {
		return "", fmt.Errorf("unsupported type of value %#v; expected string", f.Val)
	}
	name := q.setNextParameter(val)

	return replaceKeywords("c.value."+f.Key) + " = " + name, nil
}

func (q *Query) VisitNEQ(f *query.NEQ) (string, error) {
	// <key> != <val>
	val, ok := f.Val.(string)
	if !ok {
		return "", fmt.Errorf("unsupported type of value %#v; expected string", f.Val)
	}
	name := q.setNextParameter(val)

	return replaceKeywords("c.value."+f.Key) + " != " + name, nil
}

func (q *Query) VisitGT(f *query.GT) (string, error) {
	// <key> > <val>
	var name string
	switch value := f.Val.(type) {
	case int:
		name = q.setNextParameterInt(value)
	case float64:
		name = q.setNextParameterFloat(value)
	default:
		return "", fmt.Errorf("unsupported type of value %#v; expected number", f.Val)
	}
	return replaceKeywords("c.value."+f.Key) + " > " + name, nil
}

func (q *Query) VisitGTE(f *query.GTE) (string, error) {
	// <key> >= <val>
	var name string
	switch value := f.Val.(type) {
	case int:
		name = q.setNextParameterInt(value)
	case float64:
		name = q.setNextParameterFloat(value)
	default:
		return "", fmt.Errorf("unsupported type of value %#v; expected number", f.Val)
	}

	return replaceKeywords("c.value."+f.Key) + " >= " + name, nil
}

func (q *Query) VisitLT(f *query.LT) (string, error) {
	// <key> < <val>
	var name string
	switch value := f.Val.(type) {
	case int:
		name = q.setNextParameterInt(value)
	case float64:
		name = q.setNextParameterFloat(value)
	default:
		return "", fmt.Errorf("unsupported type of value %#v; expected number", f.Val)
	}

	return replaceKeywords("c.value."+f.Key) + " < " + name, nil
}

func (q *Query) VisitLTE(f *query.LTE) (string, error) {
	// <key> <= <val>
	var name string
	switch value := f.Val.(type) {
	case int:
		name = q.setNextParameterInt(value)
	case float64:
		name = q.setNextParameterFloat(value)
	default:
		return "", fmt.Errorf("unsupported type of value %#v; expected number", f.Val)
	}

	return replaceKeywords("c.value."+f.Key) + " <= " + name, nil
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

	return fmt.Sprintf("%s IN (%s)", replaceKeywords("c.value."+f.Key), strings.Join(names, ", ")), nil
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
		filter = " WHERE " + filters
	}
	if sz := len(qq.Sort); sz != 0 {
		order := make([]string, sz)
		for i, item := range qq.Sort {
			if item.Order == query.DESC {
				order[i] = replaceKeywords("c.value."+item.Key) + " DESC"
			} else {
				order[i] = replaceKeywords("c.value."+item.Key) + " ASC"
			}
		}
		orderBy = " ORDER BY " + strings.Join(order, ", ")
	}

	q.query.query = "SELECT * FROM c" + filter + orderBy
	q.limit = qq.Page.Limit
	q.token = qq.Page.Token

	return nil
}

func (q *Query) setNextParameter(val string) string {
	pname := fmt.Sprintf("@__param__%d__", len(q.query.parameters))
	q.query.parameters = append(q.query.parameters, azcosmos.QueryParameter{Name: pname, Value: val})

	return pname
}

func (q *Query) setNextParameterInt(val int) string {
	pname := fmt.Sprintf("@__param__%d__", len(q.query.parameters))
	q.query.parameters = append(q.query.parameters, azcosmos.QueryParameter{Name: pname, Value: val})

	return pname
}

func (q *Query) setNextParameterFloat(val float64) string {
	pname := fmt.Sprintf("@__param__%d__", len(q.query.parameters))
	q.query.parameters = append(q.query.parameters, azcosmos.QueryParameter{Name: pname, Value: val})

	return pname
}

func (q *Query) execute(ctx context.Context, client *azcosmos.ContainerClient) ([]state.QueryItem, string, error) {
	opts := &azcosmos.QueryOptions{}

	resultLimit := q.limit
	opts.QueryParameters = append(opts.QueryParameters, q.query.parameters...)

	if len(q.token) != 0 {
		opts.ContinuationToken = &q.token
	}

	items := []CosmosItem{}

	var pk azcosmos.PartitionKey
	if q.partitionKey != "" {
		pk = azcosmos.NewPartitionKeyString(q.partitionKey)
	} else {
		pk = azcosmos.NewPartitionKeyBool(true)
	}

	queryPager := client.NewQueryItemsPager(q.query.query, pk, opts)

	token := ""
	for queryPager.More() {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, defaultTimeout)
		queryResponse, innerErr := queryPager.NextPage(ctxWithTimeout)
		cancel()
		if innerErr != nil {
			return nil, "", innerErr
		}

		if queryResponse.ContinuationToken == nil {
			token = ""
		} else {
			token = *queryResponse.ContinuationToken
		}
		for _, item := range queryResponse.Items {
			tempItem := CosmosItem{}
			err := json.Unmarshal(item, &tempItem)
			if err != nil {
				return nil, "", err
			}
			if (resultLimit != 0) && (len(items) >= resultLimit) {
				break
			}
			items = append(items, tempItem)
		}
		if (resultLimit != 0) && (len(items) >= resultLimit) {
			break
		}
	}

	ret := make([]state.QueryItem, len(items))
	var err error
	for i := range items {
		ret[i].Key = items[i].ID
		ret[i].ETag = &items[i].Etag

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

// Replaces reserved keywords. If a replacement of a reserved keyword is made, all other words will be changed from .word to ['word']
func replaceKeyword(key, keyword string) string {
	indx := strings.Index(strings.ToUpper(key), "."+strings.ToUpper(keyword))
	if indx == -1 {
		return key
	}
	// Grab the next index to check and ensure that it doesn't over-index
	nextIndx := indx + len(keyword) + 1
	if nextIndx == len(key) || !isLetter(key[nextIndx]) {
		// Get the new keyword to replace
		newKeyword := keyword
		if nextIndx < len(key)-1 {
			// Get the index of the next period (Note that it grabs the index relative to the beginning of the initial string)
			idxOfPeriod := strings.Index(key[nextIndx+1:], ".")
			if idxOfPeriod != -1 {
				newKeyword = key[nextIndx+1 : nextIndx+idxOfPeriod+1]
			} else {
				newKeyword = key[nextIndx+1:]
			}
		}
		return fmt.Sprintf("%s['%s']%s", key[:indx], key[indx+1:nextIndx], replaceKeyword(key[nextIndx:], newKeyword))
	}
	return key
}

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}
