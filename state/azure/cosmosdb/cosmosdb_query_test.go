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

package cosmosdb

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
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
			expected: "c['value']['a']",
		},
		{
			input:    "c.value.value",
			expected: "c['value']['value']",
		},
		{
			input:    "c.value.a.value",
			expected: "c['value']['a']['value']",
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, replaceKeywords(test.input))
	}
}

func TestCosmosDbQuery(t *testing.T) {
	tests := []struct {
		input string
		query InternalQuery
	}{
		{
			input: "../../../tests/state/query/q1.json",
			query: InternalQuery{
				query:      "SELECT * FROM c",
				parameters: nil,
			},
		},
		{
			input: "../../../tests/state/query/q2.json",
			query: InternalQuery{
				query: "SELECT * FROM c WHERE c['value']['state'] = @__param__0__",
				parameters: []azcosmos.QueryParameter{
					{
						Name:  "@__param__0__",
						Value: "CA",
					},
				},
			},
		},
		{
			input: "../../../tests/state/query/q3.json",
			query: InternalQuery{
				query: "SELECT * FROM c WHERE c['value']['person']['org'] = @__param__0__ AND c['value']['state'] IN (@__param__1__, @__param__2__) ORDER BY c['value']['state'] DESC, c['value']['person']['name'] ASC",
				parameters: []azcosmos.QueryParameter{
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
			query: InternalQuery{
				query: "SELECT * FROM c WHERE c['value']['person']['org'] = @__param__0__ OR (c['value']['person']['org'] = @__param__1__ AND c['value']['state'] IN (@__param__2__, @__param__3__)) ORDER BY c['value']['state'] DESC, c['value']['person']['name'] ASC",
				parameters: []azcosmos.QueryParameter{
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
