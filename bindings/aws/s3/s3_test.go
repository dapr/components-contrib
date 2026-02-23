/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package s3

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	t.Run("Has correct metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"AccessKey":      "key",
			"Region":         "region",
			"SecretKey":      "secret",
			"Bucket":         "test",
			"Endpoint":       "endpoint",
			"SessionToken":   "token",
			"ForcePathStyle": "yes",
			"DisableSSL":     "true",
			"InsecureSSL":    "1",
		}
		s3 := AWSS3{}
		meta, err := s3.parseMetadata(m)

		require.NoError(t, err)
		assert.Equal(t, "key", meta.AccessKey)
		assert.Equal(t, "region", meta.Region)
		assert.Equal(t, "secret", meta.SecretKey)
		assert.Equal(t, "test", meta.Bucket)
		assert.Equal(t, "endpoint", meta.Endpoint)
		assert.Equal(t, "token", meta.SessionToken)
		assert.True(t, meta.ForcePathStyle)
		assert.True(t, meta.DisableSSL)
		assert.True(t, meta.InsecureSSL)
	})
}

func TestParseS3Tags(t *testing.T) {
	t.Run("Has parsed s3 tags", func(t *testing.T) {
		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"decodeBase64": "yes",
			"encodeBase64": "false",
			"filePath":     "/usr/vader.darth",
			"storageClass": "STANDARD_IA",
			"tags":         "project=myproject,year=2024",
		}
		s3 := AWSS3{}
		parsedTags, err := s3.parseS3Tags(request.Metadata["tags"])

		require.NoError(t, err)
		assert.Equal(t, "project=myproject&year=2024", *parsedTags)
	})
}

func TestMergeWithRequestMetadata(t *testing.T) {
	t.Run("Has merged metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"AccessKey":      "key",
			"Region":         "region",
			"SecretKey":      "secret",
			"Bucket":         "test",
			"Endpoint":       "endpoint",
			"SessionToken":   "token",
			"ForcePathStyle": "YES",
		}
		s3 := AWSS3{}
		meta, err := s3.parseMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, "key", meta.AccessKey)
		assert.Equal(t, "region", meta.Region)
		assert.Equal(t, "secret", meta.SecretKey)
		assert.Equal(t, "test", meta.Bucket)
		assert.Equal(t, "endpoint", meta.Endpoint)
		assert.Equal(t, "token", meta.SessionToken)
		assert.True(t, meta.ForcePathStyle)

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"decodeBase64": "yes",
			"encodeBase64": "false",
			"filePath":     "/usr/vader.darth",
			"presignTTL":   "15s",
			"storageClass": "STANDARD_IA",
		}

		mergedMeta, err := meta.mergeWithRequestMetadata(&request)

		require.NoError(t, err)
		assert.Equal(t, "key", mergedMeta.AccessKey)
		assert.Equal(t, "region", mergedMeta.Region)
		assert.Equal(t, "secret", mergedMeta.SecretKey)
		assert.Equal(t, "test", mergedMeta.Bucket)
		assert.Equal(t, "endpoint", mergedMeta.Endpoint)
		assert.Equal(t, "token", mergedMeta.SessionToken)
		assert.True(t, meta.ForcePathStyle)
		assert.True(t, mergedMeta.DecodeBase64)
		assert.False(t, mergedMeta.EncodeBase64)
		assert.Equal(t, "/usr/vader.darth", mergedMeta.FilePath)
		assert.Equal(t, "15s", mergedMeta.PresignTTL)
		assert.Equal(t, "STANDARD_IA", mergedMeta.StorageClass)
	})

	t.Run("Has invalid merged metadata decodeBase64", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"AccessKey":      "key",
			"Region":         "region",
			"SecretKey":      "secret",
			"Bucket":         "test",
			"Endpoint":       "endpoint",
			"SessionToken":   "token",
			"ForcePathStyle": "true",
		}
		s3 := AWSS3{}
		meta, err := s3.parseMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, "key", meta.AccessKey)
		assert.Equal(t, "region", meta.Region)
		assert.Equal(t, "secret", meta.SecretKey)
		assert.Equal(t, "test", meta.Bucket)
		assert.Equal(t, "endpoint", meta.Endpoint)
		assert.Equal(t, "token", meta.SessionToken)
		assert.True(t, meta.ForcePathStyle)

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"decodeBase64": "hello",
		}

		mergedMeta, err := meta.mergeWithRequestMetadata(&request)

		require.NoError(t, err)
		assert.False(t, mergedMeta.DecodeBase64)
	})

	t.Run("Has invalid merged metadata encodeBase64", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"AccessKey":      "key",
			"Region":         "region",
			"SecretKey":      "secret",
			"Bucket":         "test",
			"Endpoint":       "endpoint",
			"SessionToken":   "token",
			"ForcePathStyle": "true",
		}
		s3 := AWSS3{}
		meta, err := s3.parseMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, "key", meta.AccessKey)
		assert.Equal(t, "region", meta.Region)
		assert.Equal(t, "secret", meta.SecretKey)
		assert.Equal(t, "test", meta.Bucket)
		assert.Equal(t, "endpoint", meta.Endpoint)
		assert.Equal(t, "token", meta.SessionToken)
		assert.True(t, meta.ForcePathStyle)

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"encodeBase64": "bye",
		}

		mergedMeta, err := meta.mergeWithRequestMetadata(&request)

		require.NoError(t, err)
		assert.False(t, mergedMeta.EncodeBase64)
	})
}

func TestGetOption(t *testing.T) {
	s3 := NewAWSS3(logger.NewLogger("s3")).(*AWSS3)
	s3.metadata = &s3Metadata{}

	t.Run("return error if key is missing", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		_, err := s3.get(t.Context(), &r)
		require.Error(t, err)
	})
}

func TestDeleteOption(t *testing.T) {
	s3 := NewAWSS3(logger.NewLogger("s3")).(*AWSS3)
	s3.metadata = &s3Metadata{}

	t.Run("return error if key is missing", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		_, err := s3.delete(t.Context(), &r)
		require.Error(t, err)
	})
}

func TestInitCreatesV2Clients(t *testing.T) {
	s3 := NewAWSS3(logger.NewLogger("s3")).(*AWSS3)

	md := bindings.Metadata{}
	md.Properties = map[string]string{
		"bucket": "test-bucket",
		"region": "us-west-2",
	}

	reqCtx := t.Context()
	err := s3.Init(reqCtx, md)
	require.NoError(t, err)
	require.NotNil(t, s3.s3Client)
	require.NotNil(t, s3.tmClient)
	require.NotNil(t, s3.presigner)
}

func TestForcePathStylePresignURL(t *testing.T) {
	bucket := "dapr-s3-test"
	key := "filename.txt"
	region := "us-east-1"

	cfg := aws.Config{
		Region:      region,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("AKID", "SECRET", "")),
	}

	t.Run("forcePathStyle=false", func(t *testing.T) {
		s3Client := s3.NewFromConfig(cfg)
		presignClient := s3.NewPresignClient(s3Client)
		presigned, err := presignClient.PresignGetObject(t.Context(), &s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &key,
		})
		require.NoError(t, err)
		require.Contains(t, presigned.URL, ".s3.")
		require.Contains(t, presigned.URL, bucket)
		require.Contains(t, presigned.URL, key)
		require.Equal(t, "https://"+bucket+".s3."+region+".amazonaws.com/"+key, presigned.URL[:len("https://"+bucket+".s3."+region+".amazonaws.com/"+key)])
	})

	t.Run("forcePathStyle=true", func(t *testing.T) {
		s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.UsePathStyle = true
		})
		presignClient := s3.NewPresignClient(s3Client)
		presigned, err := presignClient.PresignGetObject(t.Context(), &s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &key,
		})
		require.NoError(t, err)
		require.Contains(t, presigned.URL, "/"+bucket+"/"+key)
		require.Equal(t, "https://s3."+region+".amazonaws.com/"+bucket+"/"+key, presigned.URL[:len("https://s3."+region+".amazonaws.com/"+bucket+"/"+key)])
	})
}
