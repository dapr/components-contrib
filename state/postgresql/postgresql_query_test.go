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

package postgresql

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state/query"
)

func TestPostgresqlQueryBuildQuery(t *testing.T) {
	tests := []struct {
		input string
		query string
	}{
		{
			input: "../../tests/state/query/q1.json",
			query: "SELECT key, value, xmin as etag FROM state LIMIT 2",
		},
		{
			input: "../../tests/state/query/q2.json",
			query: "SELECT key, value, xmin as etag FROM state WHERE value->>'state'=$1 LIMIT 2",
		},
		{
			input: "../../tests/state/query/q2-token.json",
			query: "SELECT key, value, xmin as etag FROM state WHERE value->>'state'=$1 LIMIT 2 OFFSET 2",
		},
		{
			input: "../../tests/state/query/q3.json",
			query: "SELECT key, value, xmin as etag FROM state WHERE (value->'person'->>'org'=$1 AND (value->>'state'=$2 OR value->>'state'=$3)) ORDER BY value->>'state' DESC, value->'person'->>'name'",
		},
		{
			input: "../../tests/state/query/q4.json",
			query: "SELECT key, value, xmin as etag FROM state WHERE (value->'person'->>'org'=$1 OR (value->'person'->>'org'=$2 AND (value->>'state'=$3 OR value->>'state'=$4))) ORDER BY value->>'state' DESC, value->'person'->>'name' LIMIT 2",
		},
		{
			input: "../../tests/state/query/q5.json",
			query: "SELECT key, value, xmin as etag FROM state WHERE (value->'person'->>'org'=$1 AND (value->'person'->>'name'=$2 OR (value->>'state'=$3 OR value->>'state'=$4))) ORDER BY value->>'state' DESC, value->'person'->>'name' LIMIT 2",
		},
	}
	for _, test := range tests {
		data, err := os.ReadFile(test.input)
		assert.NoError(t, err)
		var qq query.Query
		err = json.Unmarshal(data, &qq)
		assert.NoError(t, err)

		q := &Query{
			tableName: defaultTableName,
		}
		qbuilder := query.NewQueryBuilder(q)
		err = qbuilder.BuildQuery(&qq)
		assert.NoError(t, err)
		assert.Equal(t, test.query, q.query)
	}
}
