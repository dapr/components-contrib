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
	"reflect"
	"regexp"
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var fieldName = regexp.MustCompile(`^[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)*$`)

// TranslateFilter maps portable filters to OpenSearch's query DSL. String
// equality uses keyword subfields; numbers and booleans retain their types.
func TranslateFilter(filter map[string]any, prefix string) (map[string]any, error) {
	parts := make([]any, 0, len(filter))
	keys := make([]string, 0, len(filter))
	for key := range filter {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := filter[key]
		switch key {
		case "$and", "$or":
			items, ok := value.([]any)
			if !ok || len(items) == 0 {
				return nil, status.Errorf(codes.InvalidArgument, "%s requires a non-empty array", key)
			}
			clauses := make([]any, 0, len(items))
			for _, item := range items {
				expr, ok := item.(map[string]any)
				if !ok {
					return nil, status.Error(codes.InvalidArgument, "logical filter operands must be objects")
				}
				clause, err := TranslateFilter(expr, prefix)
				if err != nil {
					return nil, err
				}
				clauses = append(clauses, clause)
			}
			if key == "$or" {
				parts = append(parts, map[string]any{"bool": map[string]any{"should": clauses, "minimum_should_match": 1}})
			} else {
				parts = append(parts, conjunction(clauses))
			}
		case "$not":
			expr, ok := value.(map[string]any)
			if !ok {
				return nil, status.Error(codes.InvalidArgument, "$not requires an object")
			}
			clause, err := TranslateFilter(expr, prefix)
			if err != nil {
				return nil, err
			}
			parts = append(parts, negate(clause))
		default:
			if !fieldName.MatchString(key) {
				return nil, status.Errorf(codes.InvalidArgument, "invalid filter field or operator %q", key)
			}
			ops, ok := value.(map[string]any)
			if !ok {
				ops = map[string]any{"$eq": value}
			}
			if len(ops) == 0 {
				return nil, status.Error(codes.InvalidArgument, "field filter requires an operator")
			}
			operators := make([]string, 0, len(ops))
			for op := range ops {
				operators = append(operators, op)
			}
			sort.Strings(operators)
			for _, op := range operators {
				part, err := fieldFilter(prefix+key, op, ops[op])
				if err != nil {
					return nil, err
				}
				parts = append(parts, part)
			}
		}
	}
	return conjunction(parts), nil
}

func conjunction(parts []any) map[string]any {
	if len(parts) == 0 {
		return map[string]any{"match_all": map[string]any{}}
	}
	return map[string]any{"bool": map[string]any{"filter": parts}}
}

func negate(clause map[string]any) map[string]any {
	return map[string]any{"bool": map[string]any{"must_not": []any{clause}}}
}

func scalarField(field string, value any) (string, error) {
	switch value.(type) {
	case string:
		return field + ".keyword", nil
	case nil, bool, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, json.Number:
		return field, nil
	default:
		return "", status.Errorf(codes.InvalidArgument, "filter operand must be a scalar, got %T", value)
	}
}

func fieldFilter(field, op string, operand any) (map[string]any, error) {
	exists := map[string]any{"exists": map[string]any{"field": field}}
	switch op {
	case "$exists":
		b, ok := operand.(bool)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "$exists requires a boolean")
		}
		if !b {
			return negate(exists), nil
		}
		return exists, nil
	case "$in", "$nin":
		items := reflect.ValueOf(operand)
		if !items.IsValid() || (items.Kind() != reflect.Slice && items.Kind() != reflect.Array) {
			return nil, status.Errorf(codes.InvalidArgument, "%s requires an array", op)
		}
		clauses := make([]any, 0, items.Len())
		for i := range items.Len() {
			clause, err := fieldFilter(field, "$eq", items.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			clauses = append(clauses, clause)
		}
		clause := map[string]any{"match_none": map[string]any{}}
		if len(clauses) > 0 {
			clause = map[string]any{"bool": map[string]any{"should": clauses, "minimum_should_match": 1}}
		}
		if op == "$nin" {
			clause = negate(clause)
		}
		return clause, nil
	case "$eq", "$ne", "$gt", "$gte", "$lt", "$lte":
		name, err := scalarField(field, operand)
		if err != nil {
			return nil, err
		}
		var clause map[string]any
		switch op {
		case "$eq", "$ne":
			if operand == nil {
				clause = negate(exists)
			} else {
				clause = map[string]any{"term": map[string]any{name: operand}}
			}
			if op == "$ne" {
				clause = negate(clause)
			}
		default:
			if operand == nil {
				return nil, status.Error(codes.InvalidArgument, "range filter requires a non-null operand")
			}
			clause = map[string]any{"range": map[string]any{name: map[string]any{strings.TrimPrefix(op, "$"): operand}}}
		}
		return clause, nil
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported filter operator %q", op)
	}
}
