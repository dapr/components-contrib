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

package opensearch

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTranslateFilter(t *testing.T) {
	for _, tc := range []struct {
		name   string
		filter map[string]any
		want   string
	}{
		{"empty", nil, `{"match_all":{}}`},
		{"string", map[string]any{"make": "dapr"}, `{"bool":{"filter":[{"term":{"metadata.make.keyword":"dapr"}}]}}`},
		{"numeric", map[string]any{"price": map[string]any{"$gte": 12}}, `{"bool":{"filter":[{"range":{"metadata.price":{"gte":12}}}]}}`},
		{"boolean", map[string]any{"active": true}, `{"bool":{"filter":[{"term":{"metadata.active":true}}]}}`},
		{"missing", map[string]any{"nested.field": map[string]any{"$exists": false}}, `{"bool":{"filter":[{"bool":{"must_not":[{"exists":{"field":"metadata.nested.field"}}]}}]}}`},
		{"null", map[string]any{"missing": nil}, `{"bool":{"filter":[{"bool":{"must_not":[{"exists":{"field":"metadata.missing"}}]}}]}}`},
		{"in", map[string]any{"make": map[string]any{"$in": []string{"a", "b"}}}, `{"bool":{"filter":[{"bool":{"should":[{"term":{"metadata.make.keyword":"a"}},{"term":{"metadata.make.keyword":"b"}}],"minimum_should_match":1}}]}}`},
		{"empty in", map[string]any{"make": map[string]any{"$in": []any{}}}, `{"bool":{"filter":[{"match_none":{}}]}}`},
		{"empty nin", map[string]any{"make": map[string]any{"$nin": []any{}}}, `{"bool":{"filter":[{"bool":{"must_not":[{"match_none":{}}]}}]}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := TranslateFilter(tc.filter, "metadata.")
			require.NoError(t, err)
			data, err := json.Marshal(got)
			require.NoError(t, err)
			assert.JSONEq(t, tc.want, string(data))
		})
	}
}

func TestFilterLogicalAndInvalid(t *testing.T) {
	clause, err := TranslateFilter(map[string]any{"$or": []any{
		map[string]any{"make": "a"},
		map[string]any{"$not": map[string]any{"make": map[string]any{"$ne": "b"}}},
	}}, "metadata.")
	require.NoError(t, err)
	data, err := json.Marshal(clause)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"minimum_should_match":1`)
	assert.Contains(t, string(data), `"must_not"`)
	for _, filter := range []map[string]any{
		{"$bad": true}, {"$or": "bad"}, {"$and": []any{}}, {"$not": []any{}},
		{"field": map[string]any{"$regex": ".*"}}, {"field": map[string]any{"$exists": "true"}},
		{"field": map[string]any{"$in": 1}}, {"field": []any{1}}, {"bad field": 1},
		{"field": map[string]any{}}, {"$and": []any{1}},
	} {
		_, err = TranslateFilter(filter, "")
		assert.Equal(t, codes.InvalidArgument, status.Code(err), filter)
	}
}
