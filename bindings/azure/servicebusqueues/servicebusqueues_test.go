// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package servicebusqueues

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"connectionString": "connString", "queueName": "queue1"}
	a := NewAzureServiceBusQueues()
	meta, err := a.parseMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "connString", meta.ConnectionString)
	assert.Equal(t, "queue1", meta.QueueName)
}
