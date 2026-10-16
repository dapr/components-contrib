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
	"strconv"
	"strings"
)

// TranslateFilter converts a Dapr filter expression into a Meilisearch filter
// string. Field paths address document attributes directly, as search filters
// address the keys of a document's content.
func TranslateFilter(filter map[string]any) (string, error) {
	return TranslateFilterWithPrefix(filter, "")
}

// TranslateFilterWithPrefix converts a Dapr filter expression into a
// Meilisearch filter string, prefixing every field path with prefix. Vector
// filters address record metadata, which is stored under MetadataField, so
// they pass MetadataField + "." and a portable path `a.b` becomes
// `daprMetadata.a.b`.
func TranslateFilterWithPrefix(filter map[string]any, prefix string) (string, error) {
	// An empty filter is no filter. Every nested expression must constrain
	// something: an empty one would translate to nothing, and a caller that
	// omits an empty filter would match every document where the expression
	// was meant to narrow the query.
	if len(filter) == 0 {
		return "", nil
	}
	return translateExpression(filter, prefix)
}

func translateExpression(expr any, prefix string) (string, error) {
	switch v := expr.(type) {
	case map[string]any:
		if len(v) == 0 {
			return "", errors.New("a filter expression must not be empty")
		}
		return translateMap(v, prefix)
	case []any:
		if len(v) == 0 {
			return "", errors.New("a filter expression must not be empty")
		}
		parts := make([]string, 0, len(v))
		for _, item := range v {
			part, err := translateExpression(item, prefix)
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

func translateMap(m map[string]any, prefix string) (string, error) {
	parts := make([]string, 0, len(m))
	for key, value := range m {
		switch key {
		case "$and", "$or":
			items, ok := value.([]any)
			if !ok {
				return "", fmt.Errorf("%s requires an array", key)
			}
			if len(items) == 0 {
				return "", fmt.Errorf("%s requires at least one expression", key)
			}
			op := " AND "
			if key == "$or" {
				op = " OR "
			}
			compound := make([]string, 0, len(items))
			for _, item := range items {
				part, err := translateExpression(item, prefix)
				if err != nil {
					return "", err
				}
				compound = append(compound, "("+part+")")
			}
			part := strings.Join(compound, op)
			// AND binds tighter than OR in a Meilisearch filter, so a compound
			// that shares its object with other conditions is parenthesized
			// before it is ANDed with them.
			if len(m) > 1 {
				part = "(" + part + ")"
			}
			parts = append(parts, part)
		case "$not":
			part, err := translateExpression(value, prefix)
			if err != nil {
				return "", err
			}
			parts = append(parts, "NOT ("+part+")")
		default:
			if strings.HasPrefix(key, "$") {
				return "", fmt.Errorf("unsupported filter operator %q", key)
			}
			part, err := translateField(prefix+key, value)
			if err != nil {
				return "", err
			}
			parts = append(parts, part)
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, " AND "), nil
}

// translateField translates the conditions on one field. path is caller data
// (a filter key, optionally prefixed), so it is quoted: unquoted, a key
// containing filter syntax would rewrite the boolean structure of the filter.
func translateField(path string, value any) (string, error) {
	field := quoteString(path)
	ops, ok := value.(map[string]any)
	if !ok {
		return fmt.Sprintf("%s = %s", field, FormatFilterValue(value)), nil
	}
	if len(ops) == 0 {
		return "", fmt.Errorf("the condition on field %q has no operator", path)
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
			return "", fmt.Errorf("unsupported filter operator $regex for field %q: Meilisearch filters do not support regular expressions (pattern %q)", path, pattern)
		default:
			return "", fmt.Errorf("unsupported filter operator %q for field %q", op, path)
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
		return quoteString(v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case nil:
		return "NULL"
	case float64:
		// Numbers decoded from JSON arrive as float64. Meilisearch does not
		// parse the exponent form Go prints for large values (1e+06), so
		// floats are always written in decimal notation.
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return quoteString(v.String())
		}
		return strconv.FormatFloat(f, 'f', -1, 64)
	default:
		return fmt.Sprint(v)
	}
}

// quoteString renders s as a double-quoted filter token, which Meilisearch
// reads as a single attribute name or string value whatever it contains.
func quoteString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
