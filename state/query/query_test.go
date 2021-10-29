// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package query

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	tests := []struct {
		input string
		query Query
	}{
		{
			input: "../../tests/state/query/q1.json",
			query: Query{
				Filters: nil,
				Sort:    nil,
				Page:    Pagination{Limit: 2, Token: ""},
				Filter:  nil,
			},
		},
		{
			input: "../../tests/state/query/q2.json",
			query: Query{
				Filters: nil,
				Sort:    nil,
				Page:    Pagination{Limit: 2, Token: ""},
				Filter:  &EQ{Key: "state", Val: "CA"},
			},
		},
		{
			input: "../../tests/state/query/q3.json",
			query: Query{
				Filters: nil,
				Sort: []Sorting{
					{Key: "state", Order: "DESC"},
					{Key: "person.name", Order: ""},
				},
				Page: Pagination{Limit: 0, Token: ""},
				Filter: &AND{
					Filters: []Filter{
						&EQ{Key: "person.org", Val: "A"},
						&IN{Key: "state", Vals: []interface{}{"CA", "WA"}},
					},
				},
			},
		},
		{
			input: "../../tests/state/query/q4.json",
			query: Query{
				Filters: nil,
				Sort: []Sorting{
					{Key: "state", Order: "DESC"},
					{Key: "person.name", Order: ""},
				},
				Page: Pagination{Limit: 2, Token: ""},
				Filter: &OR{
					Filters: []Filter{
						&EQ{Key: "person.org", Val: "A"},
						&AND{
							Filters: []Filter{
								&EQ{Key: "person.org", Val: "B"},
								&IN{Key: "state", Vals: []interface{}{"CA", "WA"}},
							},
						},
					},
				},
			},
		},
	}
	for _, test := range tests {
		data, err := os.ReadFile(test.input)
		assert.NoError(t, err)
		var q Query
		err = json.Unmarshal(data, &q)
		assert.NoError(t, err)
		assert.Equal(t, test.query, q)
	}
}
