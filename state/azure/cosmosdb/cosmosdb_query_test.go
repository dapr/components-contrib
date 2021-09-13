// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cosmosdb

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/a8m/documentdb"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state/query"
)

func TestCosmosDbKeyReplace(t *testing.T) {
	tests := []struct{ input, expected string }{
		{
			input:    "c.a",
			expected: "c.a",
		},
		{
			input:    "c.a.b",
			expected: "c.a.b",
		},
		{
			input:    "c.value",
			expected: "c['value']",
		},
		{
			input:    "c.value.a",
			expected: "c['value'].a",
		},
		{
			input:    "c.value.value",
			expected: "c['value']['value']",
		},
		{
			input:    "c.value.a.value",
			expected: "c['value'].a['value']",
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, replaceKeywords(test.input))
	}
}

func TestCosmosDbQuery(t *testing.T) {
	tests := []struct {
		input string
		query documentdb.Query
	}{
		{
			input: "../../../tests/state/query/q1.json",
			query: documentdb.Query{
				Query:      "SELECT * FROM c",
				Parameters: nil,
			},
		},
		{
			input: "../../../tests/state/query/q2.json",
			query: documentdb.Query{
				Query: "SELECT * FROM c WHERE c.state = @__param__0__",
				Parameters: []documentdb.Parameter{
					{
						Name:  "@__param__0__",
						Value: "CA",
					},
				},
			},
		},
		{
			input: "../../../tests/state/query/q3.json",
			query: documentdb.Query{
				Query: "SELECT * FROM c WHERE c.person.org = @__param__0__ AND c.state IN (@__param__1__, @__param__2__) ORDER BY c.state DESC, c.person.name ASC",
				Parameters: []documentdb.Parameter{
					{
						Name:  "@__param__0__",
						Value: "A",
					},
					{
						Name:  "@__param__1__",
						Value: "CA",
					},
					{
						Name:  "@__param__2__",
						Value: "WA",
					},
				},
			},
		},
		{
			input: "../../../tests/state/query/q4.json",
			query: documentdb.Query{
				Query: "SELECT * FROM c WHERE c.person.org = @__param__0__ OR (c.person.org = @__param__1__ AND c.state IN (@__param__2__, @__param__3__)) ORDER BY c.state DESC, c.person.name ASC",
				Parameters: []documentdb.Parameter{
					{
						Name:  "@__param__0__",
						Value: "A",
					},
					{
						Name:  "@__param__1__",
						Value: "B",
					},
					{
						Name:  "@__param__2__",
						Value: "CA",
					},
					{
						Name:  "@__param__3__",
						Value: "WA",
					},
				},
			},
		},
	}
	for _, test := range tests {
		data, err := os.ReadFile(test.input)
		assert.NoError(t, err)
		var qq query.Query
		err = json.Unmarshal(data, &qq)
		assert.NoError(t, err)

		q := &Query{}
		qbuilder := query.NewQueryBuilder(q)
		err = qbuilder.BuildQuery(&qq)
		assert.NoError(t, err)
		assert.Equal(t, test.query, q.query)
	}
}
