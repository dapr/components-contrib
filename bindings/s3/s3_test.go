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
	m.Properties = map[string]string{"AccessKey": "key", "Region": "region", "SecretKey": "secret", "Bucket": "test"}
	s3 := AWSS3{}
	meta, err := s3.parseMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "key", meta.AccessKey)
	assert.Equal(t, "region", meta.Region)
	secretKey, err := meta.SecretKey.Open()
	assert.NoError(t, err)
	defer secretKey.Destroy()
	assert.Equal(t, "secret", secretKey.String())
	assert.Equal(t, "test", meta.Bucket)
}
