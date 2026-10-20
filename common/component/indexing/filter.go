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

// Package indexing holds the in-process pieces shared by the in-memory search
// and vector components: the portable filter evaluator and the write
// acknowledgement model.
package indexing

import (
	"cmp"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Predicate reports whether a document's fields satisfy a compiled filter.
type Predicate func(fields map[string]any) bool

var errEmptyExpression = errors.New("a filter expression must not be empty")

// CompileFilter compiles a portable filter expression. The whole expression is
// validated up front, so an invalid filter is rejected whatever the data.
// Errors are INVALID_ARGUMENT status errors.
func CompileFilter(filter map[string]any) (Predicate, error) {
	if len(filter) == 0 {
		return func(map[string]any) bool { return true }, nil
	}
	p, err := compileExpression(filter)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return p, nil
}

func compileExpression(expr any) (Predicate, error) {
	switch v := expr.(type) {
	case map[string]any:
		if len(v) == 0 {
			return nil, errEmptyExpression
		}
		return compileMap(v)
	case []any:
		if len(v) == 0 {
			return nil, errEmptyExpression
		}
		ps, err := compileList(v)
		if err != nil {
			return nil, err
		}
		return all(ps), nil
	default:
		return nil, fmt.Errorf("filter expression must be an object, got %T", expr)
	}
}

func compileList(items []any) ([]Predicate, error) {
	ps := make([]Predicate, 0, len(items))
	for _, item := range items {
		p, err := compileExpression(item)
		if err != nil {
			return nil, err
		}
		ps = append(ps, p)
	}
	return ps, nil
}

func all(ps []Predicate) Predicate {
	return func(fields map[string]any) bool {
		for _, p := range ps {
			if !p(fields) {
				return false
			}
		}
		return true
	}
}

func compileMap(m map[string]any) (Predicate, error) {
	ps := make([]Predicate, 0, len(m))
	for key, value := range m {
		switch key {
		case "$and", "$or":
			items, ok := value.([]any)
			if !ok {
				return nil, fmt.Errorf("%s requires an array", key)
			}
			if len(items) == 0 {
				return nil, fmt.Errorf("%s requires at least one expression", key)
			}
			sub, err := compileList(items)
			if err != nil {
				return nil, err
			}
			if key == "$and" {
				ps = append(ps, all(sub))
				continue
			}
			ps = append(ps, func(fields map[string]any) bool {
				for _, p := range sub {
					if p(fields) {
						return true
					}
				}
				return false
			})
		case "$not":
			p, err := compileExpression(value)
			if err != nil {
				return nil, err
			}
			ps = append(ps, func(fields map[string]any) bool { return !p(fields) })
		default:
			if strings.HasPrefix(key, "$") {
				return nil, fmt.Errorf("unsupported filter operator %q", key)
			}
			p, err := compileField(key, value)
			if err != nil {
				return nil, err
			}
			ps = append(ps, p)
		}
	}
	return all(ps), nil
}

// compileField compiles the conditions on one field. A bare value is $eq
// shorthand. A missing field only satisfies $ne, $nin and $exists: false.
func compileField(path string, value any) (Predicate, error) {
	ops, ok := value.(map[string]any)
	if !ok {
		ops = map[string]any{"$eq": value}
	}
	if len(ops) == 0 {
		return nil, fmt.Errorf("the condition on field %q has no operator", path)
	}

	tests := make([]func(v any, found bool) bool, 0, len(ops))
	for op, operand := range ops {
		var test func(v any, found bool) bool
		switch op {
		case "$eq":
			test = func(v any, found bool) bool { return found && equal(v, operand) }
		case "$ne":
			test = func(v any, found bool) bool { return !found || !equal(v, operand) }
		case "$gt", "$gte", "$lt", "$lte":
			test = ordered(op, operand)
		case "$in", "$nin":
			items, ok := operand.([]any)
			if !ok {
				return nil, fmt.Errorf("%s requires an array, got %T", op, operand)
			}
			in := func(v any) bool {
				for _, item := range items {
					if equal(v, item) {
						return true
					}
				}
				return false
			}
			if op == "$in" {
				test = func(v any, found bool) bool { return found && in(v) }
			} else {
				test = func(v any, found bool) bool { return !found || !in(v) }
			}
		case "$exists":
			exists, ok := operand.(bool)
			if !ok {
				return nil, errors.New("$exists requires a boolean")
			}
			test = func(_ any, found bool) bool { return found == exists }
		default:
			return nil, fmt.Errorf("unsupported filter operator %q for field %q", op, path)
		}
		tests = append(tests, anyElement(test, op == "$ne" || op == "$nin"))
	}

	return func(fields map[string]any) bool {
		v, found := Lookup(fields, path)
		for _, test := range tests {
			if !test(v, found) {
				return false
			}
		}
		return true
	}, nil
}

// anyElement lifts a scalar test over array-valued fields: an array matches
// when any of its elements does, so a negated test matches when none does.
func anyElement(test func(v any, found bool) bool, negated bool) func(v any, found bool) bool {
	return func(v any, found bool) bool {
		items, ok := v.([]any)
		if !ok {
			return test(v, found)
		}
		for _, item := range items {
			if test(item, true) != negated {
				return !negated
			}
		}
		return negated
	}
}

func ordered(op string, operand any) func(v any, found bool) bool {
	return func(v any, found bool) bool {
		if !found {
			return false
		}
		c, ok := Compare(v, operand)
		if !ok {
			return false
		}
		switch op {
		case "$gt":
			return c > 0
		case "$gte":
			return c >= 0
		case "$lt":
			return c < 0
		default:
			return c <= 0
		}
	}
}

func equal(a, b any) bool {
	c, ok := Compare(a, b)
	return ok && c == 0
}

// Lookup resolves a dotted field path through nested objects.
func Lookup(fields map[string]any, path string) (any, bool) {
	var cur any = fields
	for part := range strings.SplitSeq(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = m[part]; !ok {
			return nil, false
		}
	}
	return cur, true
}

// Compare orders two values of the same JSON type: numbers numerically,
// strings lexically and booleans false before true. An explicit null equals
// only null. ok is false when the values are of different types or not
// comparable.
func Compare(a, b any) (c int, ok bool) {
	if a == nil || b == nil {
		return 0, a == nil && b == nil
	}
	if x, isNum := toFloat(a); isNum {
		y, isNum := toFloat(b)
		return cmp.Compare(x, y), isNum
	}
	switch x := a.(type) {
	case string:
		y, isStr := b.(string)
		return strings.Compare(x, y), isStr
	case bool:
		y, isBool := b.(bool)
		return cmp.Compare(boolInt(x), boolInt(y)), isBool
	default:
		return 0, false
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	default:
		return 0, false
	}
}
