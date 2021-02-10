// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package influx

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"Url": "a", "Token": "a", "Org": "a", "Bucket": "a"}
	influx := Influx{logger: logger.NewLogger("test")}
	im, err := influx.getInfluxMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "a", im.URL)
	assert.Equal(t, "a", im.Token)
	assert.Equal(t, "a", im.Org)
	assert.Equal(t, "a", im.Bucket)
}
