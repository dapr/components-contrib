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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"Collection": "a", "Database": "a", "MasterKey": "a", "PartitionKey": "a", "URL": "a"}
	cosmosDB := CosmosDB{logger: logger.NewLogger("test")}
	meta, err := cosmosDB.parseMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "a", meta.Collection)
	assert.Equal(t, "a", meta.Database)
	assert.Equal(t, "a", meta.MasterKey)
	assert.Equal(t, "a", meta.PartitionKey)
	assert.Equal(t, "a", meta.URL)
}

func TestPartitionKeyValue(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"Collection": "a", "Database": "a", "MasterKey": "a", "PartitionKey": "a", "URL": "a"}
	cosmosDB := CosmosDB{logger: logger.NewLogger("test")}
	var obj interface{}
	jsonStr := `{"name": "name", "empty" : "", "address": { "planet" : { "name": "earth" }, "zip" : "zipcode" }}`
	json.Unmarshal([]byte(jsonStr), &obj)

	// Valid single partition key
	val, err := cosmosDB.getPartitionKeyValue("name", obj)
	assert.Nil(t, err)
	assert.Equal(t, "name", val)

	// Not existing key
	_, err = cosmosDB.getPartitionKeyValue("notexists", obj)
	assert.NotNil(t, err)

	// // Empty value for the key
	_, err = cosmosDB.getPartitionKeyValue("empty", obj)
	assert.NotNil(t, err)

	// Valid nested partition key
	val, err = cosmosDB.getPartitionKeyValue("address.zip", obj)
	assert.Nil(t, err)
	assert.Equal(t, "zipcode", val)

	// Valid nested three level partition key
	val, err = cosmosDB.getPartitionKeyValue("address.planet.name", obj)
	assert.Nil(t, err)
	assert.Equal(t, "earth", val)

	// Invalid nested partition key
	_, err = cosmosDB.getPartitionKeyValue("address.notexists", obj)
	assert.NotNil(t, err)

	// Empty key is passed
	_, err = cosmosDB.getPartitionKeyValue("", obj)
	assert.NotNil(t, err)
}
