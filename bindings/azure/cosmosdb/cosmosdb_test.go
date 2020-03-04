// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cosmosdb

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
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
