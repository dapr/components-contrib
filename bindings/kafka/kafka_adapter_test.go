// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kafka_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings/kafka"
	"github.com/dapr/kit/logger"
)

func TestKafkaAdapter(t *testing.T) {
	log := logger.NewLogger("test")
	b := kafka.NewKafkaAdapter(log)
	assert.NotNil(t, b)
}
