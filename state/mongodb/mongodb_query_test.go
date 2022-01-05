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

package mongodb

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state/query"
)

func TestMongoQuery(t *testing.T) {
	tests := []struct {
		input string
		query string
	}{
		{
			input: "../../tests/state/query/q1.json",
			query: ``,
		},
		{
			input: "../../tests/state/query/q2.json",
			query: `{ "state": "CA" }`,
		},
		{
			input: "../../tests/state/query/q3.json",
			query: `{ "$and": [ { "person.org": "A" }, { "state": { "$in": [ "CA", "WA" ] } } ] }`,
		},
		{
			input: "../../tests/state/query/q4.json",
			query: `{ "$or": [ { "person.org": "A" }, { "$and": [ { "person.org": "B" }, { "state": { "$in": [ "CA", "WA" ] } } ] } ] }`,
		},
	}
	for _, test := range tests {
		data, err := ioutil.ReadFile(test.input)
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
