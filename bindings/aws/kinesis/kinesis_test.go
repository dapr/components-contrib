// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kinesis

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
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
		"SessionToken": "token",
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
	assert.Equal(t, "token", meta.SessionToken)
	assert.Equal(t, kinesisConsumerMode("extended"), meta.KinesisConsumerMode)
}
