// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package query

import (
	"encoding/json"
	"fmt"
)

const (
	FILTER = "filter"
	SORT   = "sort"
	PAGE   = "pagination"
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

type Query struct {
	Filters map[string]interface{} `json:"filter"`
	Sort    []Sorting              `json:"sort"`
	Page    Pagination             `json:"page"`

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
	var m map[string]interface{}
	err := json.Unmarshal(data, &m)
	if err != nil {
		return err
	}
	if elem, ok := m[FILTER]; ok {
		q.Filter, err = parseFilter(elem)
		if err != nil {
			return err
		}
	}
	// setting sorting
	if elem, ok := m[SORT]; ok {
		arr, ok := elem.([]interface{})
		if !ok {
			return fmt.Errorf("%q must be an array", SORT)
		}
		jdata, err := json.Marshal(arr)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(jdata, &q.Sort); err != nil {
			return err
		}
	}
	// setting pagination
	if elem, ok := m[PAGE]; ok {
		page, ok := elem.(map[string]interface{})
		if !ok {
			return fmt.Errorf("%q must be a map", PAGE)
		}
		jdata, err := json.Marshal(page)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(jdata, &q.Page); err != nil {
			return err
		}
	}

	return nil
}
