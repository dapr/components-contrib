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

package tablestorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetTableStorageMetadata(t *testing.T) {
	t.Run("Nothing at all passed", func(t *testing.T) {
		m := make(map[string]string)
		_, err := getTablesMetadata(m)

		assert.NotNil(t, err)
	})

	t.Run("All parameters passed and parsed", func(t *testing.T) {
		m := make(map[string]string)
		m["accountName"] = "acc"
		m["accountKey"] = "key"
		m["tableName"] = "dapr"
		m[cosmosDBModeKey] = "on"
		meta, err := getTablesMetadata(m)

		assert.Nil(t, err)
		assert.Equal(t, "acc", meta.AccountName)
		assert.Equal(t, "key", meta.AccountKey)
		assert.Equal(t, "dapr", meta.TableName)
		assert.Equal(t, true, meta.CosmosDBMode)
	})

	t.Run("All parameters passed and parsed, using aliases", func(t *testing.T) {
		m := make(map[string]string)
		m["storageAccountName"] = "acc"
		m["accessKey"] = "key"
		m["table"] = "dapr"
		meta, err := getTablesMetadata(m)

		assert.Nil(t, err)
		assert.Equal(t, "acc", meta.AccountName)
		assert.Equal(t, "key", meta.AccountKey)
		assert.Equal(t, "dapr", meta.TableName)
	})
}

func TestPartitionAndRowKey(t *testing.T) {
	t.Run("Valid composite key", func(t *testing.T) {
		pk, rk := getPartitionAndRowKey("pk||rk", false)
		assert.Equal(t, "pk", pk)
		assert.Equal(t, "rk", rk)
	})

	t.Run("No delimiter present", func(t *testing.T) {
		pk, rk := getPartitionAndRowKey("pk_rk", false)
		assert.Equal(t, "pk_rk", pk)
		assert.Equal(t, "", rk)
	})
}
