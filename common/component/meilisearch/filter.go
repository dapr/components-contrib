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

package meilisearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// TranslateFilter converts a Dapr filter expression into a Meilisearch filter string.
func TranslateFilter(filter map[string]any) (string, error) {
	return translateExpression(filter)
}

func translateExpression(expr any) (string, error) {
	switch v := expr.(type) {
	case map[string]any:
		return translateMap(v)
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			part, err := translateExpression(item)
			if err != nil {
				return "", err
			}
			parts = append(parts, "("+part+")")
		}
		return strings.Join(parts, " AND "), nil
	default:
		return "", fmt.Errorf("filter expression must be an object, got %T", expr)
	}
}

func translateMap(m map[string]any) (string, error) {
	parts := make([]string, 0, len(m))
	for key, value := range m {
		switch key {
		case "$and", "$or":
			items, ok := value.([]any)
			if !ok {
				return "", fmt.Errorf("%s requires an array", key)
			}
			op := " AND "
			if key == "$or" {
				op = " OR "
			}
			compound := make([]string, 0, len(items))
			for _, item := range items {
				part, err := translateExpression(item)
				if err != nil {
					return "", err
				}
				compound = append(compound, "("+part+")")
			}
			parts = append(parts, strings.Join(compound, op))
		case "$not":
			part, err := translateExpression(value)
			if err != nil {
				return "", err
			}
			parts = append(parts, "NOT ("+part+")")
		default:
			part, err := translateField(key, value)
			if err != nil {
				return "", err
			}
			parts = append(parts, part)
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, " AND "), nil
}

func translateField(field string, value any) (string, error) {
	if strings.HasPrefix(field, "$") {
		return "", fmt.Errorf("unsupported filter operator %q", field)
	}
	ops, ok := value.(map[string]any)
	if !ok {
		return fmt.Sprintf("%s = %s", field, FormatFilterValue(value)), nil
	}
	parts := make([]string, 0, len(ops))
	for op, operand := range ops {
		switch op {
		case "$eq":
			parts = append(parts, fmt.Sprintf("%s = %s", field, FormatFilterValue(operand)))
		case "$ne":
			parts = append(parts, fmt.Sprintf("%s != %s", field, FormatFilterValue(operand)))
		case "$gt":
			parts = append(parts, fmt.Sprintf("%s > %s", field, FormatFilterValue(operand)))
		case "$gte":
			parts = append(parts, fmt.Sprintf("%s >= %s", field, FormatFilterValue(operand)))
		case "$lt":
			parts = append(parts, fmt.Sprintf("%s < %s", field, FormatFilterValue(operand)))
		case "$lte":
			parts = append(parts, fmt.Sprintf("%s <= %s", field, FormatFilterValue(operand)))
		case "$in", "$nin":
			items, err := arrayValues(operand)
			if err != nil {
				return "", fmt.Errorf("%s requires an array: %w", op, err)
			}
			vals := make([]string, 0, len(items))
			for _, item := range items {
				vals = append(vals, FormatFilterValue(item))
			}
			part := fmt.Sprintf("%s IN [%s]", field, strings.Join(vals, ", "))
			if op == "$nin" {
				part = "NOT (" + part + ")"
			}
			parts = append(parts, part)
		case "$exists":
			exists, ok := operand.(bool)
			if !ok {
				return "", errors.New("$exists requires a boolean")
			}
			part := field + " EXISTS"
			if !exists {
				part = "NOT (" + part + ")"
			}
			parts = append(parts, part)
		case "$regex":
			pattern, ok := operand.(string)
			if !ok {
				return "", errors.New("$regex requires a string")
			}
			return "", fmt.Errorf("unsupported filter operator $regex for field %q: Meilisearch filters do not support regular expressions (pattern %q)", field, pattern)
		default:
			return "", fmt.Errorf("unsupported filter operator %q for field %q", op, field)
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, " AND "), nil
}

func arrayValues(value any) ([]any, error) {
	switch v := value.(type) {
	case []any:
		return v, nil
	case []string:
		out := make([]any, len(v))
		for i := range v {
			out[i] = v[i]
		}
		return out, nil
	case []int:
		out := make([]any, len(v))
		for i := range v {
			out[i] = v[i]
		}
		return out, nil
	default:
		return nil, fmt.Errorf("got %T", value)
	}
}

// FormatFilterValue formats a value for use in a Meilisearch filter string.
func FormatFilterValue(value any) string {
	switch v := value.(type) {
	case string:
		b, _ := json.Marshal(v)
		return string(b)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case nil:
		return "NULL"
	default:
		return fmt.Sprint(v)
	}
}
