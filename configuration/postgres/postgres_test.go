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

package postgres

import (
	"context"
	"regexp"
	"testing"

	"github.com/pashagolub/pgxmock"

	"github.com/dapr/components-contrib/configuration"
)

func TestPostgresbuildQuery(t *testing.T) {
	g := &configuration.GetRequest{
		Keys: []string{"someKey"},
		Metadata: map[string]string{
			"Version": "1.0",
		},
	}

	query, params, err := buildQuery(g, "cfgtbl")
	_ = params
	if err != nil {
		t.Errorf("Error building query: %v ", err)
	}
	expected := "SELECT * FROM cfgtbl WHERE KEY IN ($1) AND $2 = $3"
	if query != expected {
		t.Errorf("Did not get expected result. Got: '%v' , Expected: '%v'", query, expected)
	}
	i := 0
	for _, v := range params {
		got := v.(string)
		switch i {
		case 0:
			expected := "someKey"
			if expected != got {
				t.Errorf("Did not get expected result. Got: '%v' , Expected: '%v'", got, expected)
			}
		case 1:
			expected := "Version"
			if expected != got {
				t.Errorf("Did not get expected result. Got: '%v' , Expected: '%v'", got, expected)
			}
		case 2:
			expected := "1.0"
			if expected != got {
				t.Errorf("Did not get expected result. Got: '%v' , Expected: '%v'", got, expected)
			}
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
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
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
	if err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
