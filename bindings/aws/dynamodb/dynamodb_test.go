// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package dynamodb

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"AccessKey": "a", "Region": "a", "SecretKey": "a", "Table": "a", "Endpoint": "a", "SessionToken": "t",
	}
	dy := DynamoDB{}
	meta, err := dy.getDynamoDBMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "a", meta.AccessKey)
	assert.Equal(t, "a", meta.Region)
	assert.Equal(t, "a", meta.SecretKey)
	assert.Equal(t, "a", meta.Table)
	assert.Equal(t, "a", meta.Endpoint)
	assert.Equal(t, "t", meta.SessionToken)
}
