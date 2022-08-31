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
	"encoding/json"
	"fmt"
)

const (
	FILTER = "filter"
	SORT   = "sort"
	PAGE   = "page"
	ASC    = "ASC"
	DESC   = "DESC"
)

type Sorting struct {
	Key   string `json:"key"`
	Order string `json:"order,omitempty"`
}

type Pagination struct {
	Limit int    `json:"limit"`
	Token string `json:"token,omitempty"`
}

// used only for intermediate query value.
type QueryFields struct {
	Filters map[string]interface{} `json:"filter"`
	Sort    []Sorting              `json:"sort"`
	Page    Pagination             `json:"page"`
}

type Query struct {
	QueryFields `json:",inline"`

	// derived from Filters
	Filter Filter
}

type Visitor interface {
	// returns "equal" expression
	VisitEQ(*EQ) (string, error)
	// returns "in" expression
	VisitIN(*IN) (string, error)
	// returns "and" expression
	VisitAND(*AND) (string, error)
	// returns "or" expression
	VisitOR(*OR) (string, error)
	// receives concatenated filters and finalizes the native query
	Finalize(string, *Query) error
}

type Builder struct {
	visitor Visitor
}

func NewQueryBuilder(visitor Visitor) *Builder {
	return &Builder{
		visitor: visitor,
	}
}

func (h *Builder) BuildQuery(q *Query) error {
	filters, err := h.buildFilter(q.Filter)
	if err != nil {
		return err
	}

	return h.visitor.Finalize(filters, q)
}

func (h *Builder) buildFilter(filter Filter) (string, error) {
	if filter == nil {
		return "", nil
	}
	switch f := filter.(type) {
	case *EQ:
		return h.visitor.VisitEQ(f)
	case *IN:
		return h.visitor.VisitIN(f)
	case *OR:
		return h.visitor.VisitOR(f)
	case *AND:
		return h.visitor.VisitAND(f)
	default:
		return "", fmt.Errorf("unsupported filter type %#v", filter)
	}
}

func (q *Query) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &q.QueryFields)
	if err != nil {
		return err
	}
	if len(q.QueryFields.Filters) == 0 {
		return nil
	}

	filter, err := parseFilter(q.QueryFields.Filters)
	if err != nil {
		return err
	}

	q.Filter = filter
	return nil
}
