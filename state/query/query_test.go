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
				QueryFields: QueryFields{
					Filters: nil,
					Sort:    nil,
					Page:    Pagination{Limit: 2, Token: ""},
				},
				Filter: nil,
			},
		},
		{
			input: "../../tests/state/query/q2.json",
			query: Query{
				QueryFields: QueryFields{
					Filters: map[string]any{
						"EQ": map[string]any{
							"state": "CA",
						},
					},
					Sort: nil,
					Page: Pagination{Limit: 2, Token: ""},
				},
				Filter: &EQ{Key: "state", Val: "CA"},
			},
		},
		{
			input: "../../tests/state/query/q3.json",
			query: Query{
				QueryFields: QueryFields{
					Filters: map[string]any{
						"AND": []any{
							map[string]any{
								"EQ": map[string]any{
									"person.org": "A",
								},
							},
							map[string]any{
								"IN": map[string]any{
									"state": []any{"CA", "WA"},
								},
							},
						},
					},
					Sort: []Sorting{
						{Key: "state", Order: "DESC"},
						{Key: "person.name", Order: ""},
					},
					Page: Pagination{Limit: 0, Token: ""},
				},
				Filter: &AND{
					Filters: []Filter{
						&EQ{Key: "person.org", Val: "A"},
						&IN{Key: "state", Vals: []any{"CA", "WA"}},
					},
				},
			},
		},
		{
			input: "../../tests/state/query/q4.json",
			query: Query{
				QueryFields: QueryFields{
					Filters: map[string]any{
						"OR": []any{
							map[string]any{
								"EQ": map[string]any{
									"person.org": "A",
								},
							},
							map[string]any{
								"AND": []any{
									map[string]any{
										"EQ": map[string]any{
											"person.org": "B",
										},
									},
									map[string]any{
										"IN": map[string]any{
											"state": []any{"CA", "WA"},
										},
									},
								},
							},
						},
					},
					Sort: []Sorting{
						{Key: "state", Order: "DESC"},
						{Key: "person.name", Order: ""},
					},
					Page: Pagination{Limit: 2, Token: ""},
				},
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
