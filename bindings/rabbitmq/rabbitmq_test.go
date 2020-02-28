// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rabbitmq

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"QueueName": "a", "Host": "a", "DeleteWhenUnused": "true", "Durable": "true"}
	r := RabbitMQ{logger: logger.NewLogger("test")}
	rm, err := r.getRabbitMQMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "a", rm.QueueName)
	assert.Equal(t, "a", rm.Host)
	assert.Equal(t, true, rm.DeleteWhenUnused)
	assert.Equal(t, true, rm.Durable)
}
