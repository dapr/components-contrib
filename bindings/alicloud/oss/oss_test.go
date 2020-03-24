// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package oss

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"AccessKey": "key", "Endpoint": "endpoint", "AccessKeyID": "accessKeyID", "Bucket": "test"}
	aliCloudOSS := AliCloudOSS{}
	meta, err := aliCloudOSS.parseMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "key", meta.AccessKey)
	assert.Equal(t, "endpoint", meta.Endpoint)
	assert.Equal(t, "accessKeyID", meta.AccessKeyID)
	assert.Equal(t, "test", meta.Bucket)
}
