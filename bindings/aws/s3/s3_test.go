// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package s3

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	t.Run("Has correct metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"AccessKey": "key", "Region": "region", "SecretKey": "secret", "Bucket": "test", "Endpoint": "endpoint", "SessionToken": "token", "ForcePathStyle": "true",
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
		assert.Equal(t, true, meta.ForcePathStyle)
	})
}

func TestMergeWithRequestMetadata(t *testing.T) {
	t.Run("Has merged metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"AccessKey": "key", "Region": "region", "SecretKey": "secret", "Bucket": "test", "Endpoint": "endpoint", "SessionToken": "token", "ForcePathStyle": "true",
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
		assert.Equal(t, true, meta.ForcePathStyle)

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"decodeBase64": "true",
			"encodeBase64": "false",
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
		assert.Equal(t, true, meta.ForcePathStyle)
		assert.Equal(t, true, mergedMeta.DecodeBase64)
		assert.Equal(t, false, mergedMeta.EncodeBase64)
	})

	t.Run("Has invalid merged metadata decodeBase64", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"AccessKey": "key", "Region": "region", "SecretKey": "secret", "Bucket": "test", "Endpoint": "endpoint", "SessionToken": "token", "ForcePathStyle": "true",
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
		assert.Equal(t, true, meta.ForcePathStyle)

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"decodeBase64": "hello",
		}

		mergedMeta, err := meta.mergeWithRequestMetadata(&request)

		assert.NotNil(t, err)
		assert.NotNil(t, mergedMeta)
	})

	t.Run("Has invalid merged metadata encodeBase64", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"AccessKey": "key", "Region": "region", "SecretKey": "secret", "Bucket": "test", "Endpoint": "endpoint", "SessionToken": "token", "ForcePathStyle": "true",
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
		assert.Equal(t, true, meta.ForcePathStyle)

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"encodeBase64": "bye",
		}

		mergedMeta, err := meta.mergeWithRequestMetadata(&request)

		assert.NotNil(t, err)
		assert.NotNil(t, mergedMeta)
	})
}

func TestGetOption(t *testing.T) {
	s3 := NewAWSS3(logger.NewLogger("s3"))
	s3.metadata = &s3Metadata{}

	t.Run("return error if key is missing", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		_, err := s3.get(&r)
		assert.Error(t, err)
	})
}

func TestDeleteOption(t *testing.T) {
	s3 := NewAWSS3(logger.NewLogger("s3"))
	s3.metadata = &s3Metadata{}

	t.Run("return error if key is missing", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		_, err := s3.delete(&r)
		assert.Error(t, err)
	})
}
