// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cosmosdbgremlinapi

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"Url": "a", "masterKey": "a", "username": "a"}
	cosmosdbgremlinapi := CosmosDBGremlinAPI{logger: logger.NewLogger("test")}
	im, err := cosmosdbgremlinapi.parseMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "a", im.URL)
	assert.Equal(t, "a", im.MasterKey)
	assert.Equal(t, "a", im.Username)
}
