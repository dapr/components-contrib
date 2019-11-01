// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package zipkin

import (
	"testing"

	"github.com/dapr/components-contrib/exporters"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	m := exporters.Metadata{}
	m.Properties = map[string]string{"exporterAddress": "c"}
	exporter := NewZipkinExporter()
	metadata, err := exporter.getZipkinMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "c", metadata.ExporterAddress)
}
