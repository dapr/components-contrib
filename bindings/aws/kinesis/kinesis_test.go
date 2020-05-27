// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kinesis

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"AccessKey":    "key",
		"Region":       "region",
		"SecretKey":    "secret",
		"ConsumerName": "test",
		"StreamName":   "stream",
		"Mode":         "extended",
		"Endpoint":     "endpoint",
	}
	kinesis := AWSKinesis{}
	meta, err := kinesis.parseMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "key", meta.AccessKey)
	assert.Equal(t, "region", meta.Region)
	assert.Equal(t, "secret", meta.SecretKey)
	assert.Equal(t, "test", meta.ConsumerName)
	assert.Equal(t, "stream", meta.StreamName)
	assert.Equal(t, "endpoint", meta.Endpoint)
	assert.Equal(t, kinesisConsumerMode("extended"), meta.KinesisConsumerMode)
}
