// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mqtt

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"URL": "a", "Topic": "a"}
	mq := MQTT{logger: logger.NewLogger("test")}
	mm, err := mq.getMQTTMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "a", mm.URL)
	assert.Equal(t, "a", mm.Topic)
}
