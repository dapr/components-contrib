// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package s3

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	t.Run("Has correct metadata", func(t *testing.T) {

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
	})
}
func TestMergeWithRequestMetadata(t *testing.T) {
	t.Run("Has merged metadata", func(t *testing.T) {

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

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"decodeBase64": "true",
		}

		mergedMeta, err := meta.mergeWithRequestMetadata(&request)

		assert.Nil(t, err)

		assert.Nil(t, err)
		assert.Equal(t, "key", mergedMeta.AccessKey)
		assert.Equal(t, "region", mergedMeta.Region)
		assert.Equal(t, "secret", mergedMeta.SecretKey)
		assert.Equal(t, "test", mergedMeta.Bucket)
		assert.Equal(t, "endpoint", mergedMeta.Endpoint)
		assert.Equal(t, "token", mergedMeta.SessionToken)
		assert.Equal(t, true, mergedMeta.DecodeBase64)

	})

	t.Run("Has invalid merged metadata", func(t *testing.T) {

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

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"decodeBase64": "hello",
		}

		mergedMeta, err := meta.mergeWithRequestMetadata(&request)

		assert.NotNil(t, err)
		assert.NotNil(t, mergedMeta)

	})
}

func TestGetOption(t *testing.T) {
	s3 := NewAWSS3(logger.NewLogger("s3"))

	t.Run("return error if key is missing", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		_, err := s3.get(&r)
		assert.Error(t, err)
	})
}
