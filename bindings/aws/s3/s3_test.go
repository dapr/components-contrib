// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package s3

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"AccessKey": "key", "Region": "region", "SecretKey": "secret", "Bucket": "test", "Endpoint": "endpoint", "SessionToken": "token",
	}
	s3 := AWSS3{}
	meta, err := s3.parseMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "key", meta.AccessKey)
	assert.Equal(t, "region", meta.Region)
	assert.Equal(t, "secret", meta.SecretKey)
	assert.Equal(t, "test", meta.Bucket)
	assert.Equal(t, "endpoint", meta.Endpoint)
	assert.Equal(t, "token", meta.SessionToken)
}
