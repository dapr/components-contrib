// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

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
		meta, err := getTablesMetadata(m)

		assert.Nil(t, err)
		assert.Equal(t, "acc", meta.accountName)
		assert.Equal(t, "key", meta.accountKey)
		assert.Equal(t, "dapr", meta.tableName)
	})
}

func TestPartitionAndRowKey(t *testing.T) {
	t.Run("Valid composite key", func(t *testing.T) {
		pk, rk := getPartitionAndRowKey("pk||rk")
		assert.Equal(t, "pk", pk)
		assert.Equal(t, "rk", rk)
	})

	t.Run("No delimiter present", func(t *testing.T) {
		pk, rk := getPartitionAndRowKey("pk_rk")
		assert.Equal(t, "pk_rk", pk)
		assert.Equal(t, "", rk)
	})
}
