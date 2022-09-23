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

package postgres

import (
	"context"
	"regexp"
	"testing"

	"github.com/pashagolub/pgxmock/v2"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/configuration"
)

func TestSelectAllQuery(t *testing.T) {
	g := &configuration.GetRequest{}
	expected := "SELECT * FROM cfgtbl"
	query, _, err := buildQuery(g, "cfgtbl")
	if err != nil {
		t.Errorf("Error building query: %v ", err)
	}
	assert.Equal(t, expected, query, "did not get expected result. Got: '%v' , Expected: '%v'", query, expected)

	g = &configuration.GetRequest{
		Keys: []string{},
		Metadata: map[string]string{
			"Version": "1.0",
		},
	}
	query, _, err = buildQuery(g, "cfgtbl")
	if err != nil {
		t.Errorf("Error building query: %v ", err)
	}
	assert.Nil(t, err, "Error building query: %v ", err)
	assert.Equal(t, expected, query, "did not get expected result. Got: '%v' , Expected: '%v'", query, expected)
}

func TestPostgresbuildQuery(t *testing.T) {
	g := &configuration.GetRequest{
		Keys: []string{"someKey"},
		Metadata: map[string]string{
			"Version": "1.0",
		},
	}

	query, params, err := buildQuery(g, "cfgtbl")
	_ = params
	assert.Nil(t, err, "Error building query: %v ", err)
	expected := "SELECT * FROM cfgtbl WHERE KEY IN ($1) AND $2 = $3"
	assert.Equal(t, expected, query, "did not get expected result. Got: '%v' , Expected: '%v'", query, expected)
	i := 0
	for _, v := range params {
		got := v.(string)
		switch i {
		case 0:
			expected := "someKey"
			assert.Equal(t, expected, got, "Did not get expected result. Got: '%v' , Expected: '%v'", got, expected)
		case 1:
			expected := "Version"
			assert.Equal(t, expected, got, "Did not get expected result. Got: '%v' , Expected: '%v'", got, expected)
		case 2:
			expected := "1.0"
			assert.Equal(t, expected, got, "Did not get expected result. Got: '%v' , Expected: '%v'", got, expected)
		}
		i++
	}
}

func TestConnectAndQuery(t *testing.T) {
	m := metadata{
		connectionString: "mockConnectionString",
		configTable:      "mockConfigTable",
	}

	mock, err := pgxmock.NewPool()
	assert.Nil(t, err)
	defer mock.Close()

	query := "SELECT EXISTS (SELECT FROM pg_tables where tablename = '" + m.configTable + "'"
	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WillReturnRows(pgxmock.NewRows(
			[]string{"exists"}).
			AddRow(string("t")),
		)
	rows := mock.QueryRow(context.Background(), query)
	var id string
	err = rows.Scan(&id)
	assert.Nil(t, err, "error in scan")
	err = mock.ExpectationsWereMet()
	assert.Nil(t, err, "pgxmock error in expectations were met")
}

func TestValidateInput(t *testing.T) {
	keys := []string{"testKey1", "testKey2"}
	assert.Nil(t, validateInput(keys), "incorrect input provided: %v", keys)

	var keys2 []string
	assert.Nil(t, validateInput(keys), "incorrect input provided: %v", keys2)

	keys3 := []string{"Name 1=1"}
	assert.Error(t, validateInput(keys3), "invalid key : 'Name 1=1'")
}
