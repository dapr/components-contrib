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

package redis

import (
	"encoding/json"
	"errors"
	"fmt"
)

type index struct {
	Key  string `json:"key"`
	Type string `json:"type"`
}

type querySchema struct {
	Name    string  `json:"name"`
	Indexes []index `json:"indexes"`
}

type querySchemaElem struct {
	schema []interface{}
	keys   map[string]string
}

type querySchemas map[string]*querySchemaElem

func parseQuerySchemas(content string) (querySchemas, error) {
	if len(content) == 0 {
		return nil, nil
	}

	var schemas []querySchema
	if err := json.Unmarshal([]byte(content), &schemas); err != nil {
		return nil, err
	}

	ret := querySchemas{}
	for _, schema := range schemas {
		if len(schema.Name) == 0 {
			return nil, errors.New("empty query schema name")
		}
		if _, ok := ret[schema.Name]; ok {
			return nil, fmt.Errorf("duplicate schema name %s", schema.Name)
		}
		elem := &querySchemaElem{
			keys:   make(map[string]string),
			schema: []interface{}{"FT.CREATE", schema.Name, "ON", "JSON", "SCHEMA"},
		}
		for id, indx := range schema.Indexes {
			if err := validateIndex(schema.Name, indx); err != nil {
				return nil, err
			}
			alias := fmt.Sprintf("var%d", id)
			elem.keys[indx.Key] = alias
			elem.schema = append(elem.schema, "$.data."+indx.Key, "AS", alias, indx.Type, "SORTABLE")
		}
		ret[schema.Name] = elem
	}

	return ret, nil
}

func validateIndex(name string, indx index) error {
	if len(indx.Key) == 0 {
		return fmt.Errorf("empty key in query schema %s", name)
	}
	if len(indx.Type) == 0 {
		return fmt.Errorf("empty type in query schema %s", name)
	}

	return nil
}
