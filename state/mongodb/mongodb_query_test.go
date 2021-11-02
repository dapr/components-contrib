// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

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
