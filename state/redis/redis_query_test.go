/*
Copyright 2022 The Dapr Authors
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

package redis

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state/query"
)

func TestMongoQuery(t *testing.T) {
	tests := []struct {
		input string
		query []interface{}
		err   error
	}{
		{
			input: "../../tests/state/query/q1.json",
			query: []interface{}{"*", "LIMIT", "0", "2"},
		},
		{
			input: "../../tests/state/query/q2.json",
			query: []interface{}{"@state:(CA)", "LIMIT", "0", "2"},
		},
		{
			input: "../../tests/state/query/q3.json",
			err:   ErrMultipleSortBy,
		},
		{
			input: "../../tests/state/query/q6.json",
			query: []interface{}{"((@id:[123 123])|((@org:(B)) (((@id:[567 567])|(@id:[890 890])))))", "SORTBY", "id", "LIMIT", "0", "2"},
		},
	}
	for _, test := range tests {
		data, err := os.ReadFile(test.input)
		assert.NoError(t, err)
		var qq query.Query
		err = json.Unmarshal(data, &qq)
		assert.NoError(t, err)

		q := &Query{
			aliases: map[string]string{"person.org": "org", "person.id": "id", "state": "state"},
		}
		qbuilder := query.NewQueryBuilder(q)
		if err = qbuilder.BuildQuery(&qq); err != nil {
			assert.EqualError(t, err, test.err.Error())
		} else {
			assert.Equal(t, test.query, q.query)
		}
	}
}
