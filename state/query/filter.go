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

package query

import (
	"fmt"
)

type Filter interface {
	Parse(interface{}) error
}

// ParseFilter parses a filter struct using the visitor pattern returning a built Filter interface.
func ParseFilter(obj interface{}) (Filter, error) {
	m, ok := obj.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("filter unit must be a map")
	}
	if len(m) != 1 {
		return nil, fmt.Errorf("filter unit must have a single element")
	}
	for k, v := range m {
		switch k {
		case "EQ":
			f := &EQ{}
			err := f.Parse(v)

			return f, err
		case "IN":
			f := &IN{}
			err := f.Parse(v)

			return f, err
		case "AND":
			f := &AND{}
			err := f.Parse(v)

			return f, err
		case "OR":
			f := &OR{}
			err := f.Parse(v)

			return f, err
		default:
			return nil, fmt.Errorf("unsupported filter %q", k)
		}
	}

	return nil, nil
}

type EQ struct {
	Key string
	Val interface{}
}

func (f *EQ) Parse(obj interface{}) error {
	m, ok := obj.(map[string]interface{})
	if !ok {
		return fmt.Errorf("EQ filter must be a map")
	}
	if len(m) != 1 {
		return fmt.Errorf("EQ filter must contain a single key/value pair")
	}
	for k, v := range m {
		f.Key = k
		f.Val = v
	}

	return nil
}

type IN struct {
	Key  string
	Vals []interface{}
}

func (f *IN) Parse(obj interface{}) error {
	m, ok := obj.(map[string]interface{})
	if !ok {
		return fmt.Errorf("IN filter must be a map")
	}
	if len(m) != 1 {
		return fmt.Errorf("IN filter must contain a single key/value pair")
	}
	for k, v := range m {
		f.Key = k
		if f.Vals, ok = v.([]interface{}); !ok {
			return fmt.Errorf("IN filter value must be an array")
		}
	}

	return nil
}

type AND struct {
	Filters []Filter
}

func (f *AND) Parse(obj interface{}) (err error) {
	f.Filters, err = parseFilters("AND", obj)

	return
}

type OR struct {
	Filters []Filter
}

func (f *OR) Parse(obj interface{}) (err error) {
	f.Filters, err = parseFilters("OR", obj)

	return
}

func parseFilters(t string, obj interface{}) ([]Filter, error) {
	arr, ok := obj.([]interface{})
	if !ok {
		return nil, fmt.Errorf("%s filter must be an array", t)
	}
	if len(arr) < 2 {
		return nil, fmt.Errorf("%s filter must contain at least two entries", t)
	}
	filters := make([]Filter, len(arr))
	for i, entry := range arr {
		var err error
		if filters[i], err = ParseFilter(entry); err != nil {
			return nil, err
		}
	}

	return filters, nil
}
