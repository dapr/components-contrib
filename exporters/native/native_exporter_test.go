// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package native

import (
	"testing"

	"github.com/dapr/components-contrib/exporters"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	m := exporters.Metadata{}
	m.Properties = map[string]string{"agentEndpoint": "c"}
	exporter := NewNativeExporter(logger.NewLogger("test"))
	metadata, err := exporter.getNativeMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "c", metadata.AgentEndpoint)
}
