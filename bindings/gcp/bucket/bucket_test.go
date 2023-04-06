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

package bucket

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	t.Run("Has correct metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"auth_provider_x509_cert_url": "my_auth_provider_x509",
			"auth_uri":                    "my_auth_uri",
			"Bucket":                      "my_bucket",
			"client_x509_cert_url":        "my_client_x509",
			"client_email":                "my_email@mail.dapr",
			"client_id":                   "my_client_id",
			"private_key":                 "my_private_key",
			"private_key_id":              "my_private_key_id",
			"project_id":                  "my_project_id",
			"token_uri":                   "my_token_uri",
			"type":                        "my_type",
		}
		gs := GCPStorage{logger: logger.NewLogger("test")}
		meta, err := gs.parseMetadata(m)
		assert.Nil(t, err)

		assert.Equal(t, "my_auth_provider_x509", meta.AuthProviderCertURL)
		assert.Equal(t, "my_auth_uri", meta.AuthURI)
		assert.Equal(t, "my_bucket", meta.Bucket)
		assert.Equal(t, "my_client_x509", meta.ClientCertURL)
		assert.Equal(t, "my_email@mail.dapr", meta.ClientEmail)
		assert.Equal(t, "my_client_id", meta.ClientID)
		assert.Equal(t, "my_private_key", meta.PrivateKey)
		assert.Equal(t, "my_private_key_id", meta.PrivateKeyID)
		assert.Equal(t, "my_project_id", meta.ProjectID)
		assert.Equal(t, "my_token_uri", meta.TokenURI)
		assert.Equal(t, "my_type", meta.Type)
	})

	t.Run("check backward compatibility", func(t *testing.T) {
		gs := GCPStorage{logger: logger.NewLogger("test")}

		request := bindings.InvokeRequest{}
		request.Operation = bindings.CreateOperation
		request.Metadata = map[string]string{
			"name": "my_file.txt",
		}
		result := gs.handleBackwardCompatibilityForMetadata(request.Metadata)
		assert.NotEmpty(t, result["key"])
	})
}

func TestMergeWithRequestMetadata(t *testing.T) {
	t.Run("Has merged metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"auth_provider_x509_cert_url": "my_auth_provider_x509",
			"auth_uri":                    "my_auth_uri",
			"Bucket":                      "my_bucket",
			"client_x509_cert_url":        "my_client_x509",
			"client_email":                "my_email@mail.dapr",
			"client_id":                   "my_client_id",
			"private_key":                 "my_private_key",
			"private_key_id":              "my_private_key_id",
			"project_id":                  "my_project_id",
			"token_uri":                   "my_token_uri",
			"type":                        "my_type",
			"decodeBase64":                "false",
		}
		gs := GCPStorage{logger: logger.NewLogger("test")}
		meta, err := gs.parseMetadata(m)
		assert.Nil(t, err)

		assert.Equal(t, "my_auth_provider_x509", meta.AuthProviderCertURL)
		assert.Equal(t, "my_auth_uri", meta.AuthURI)
		assert.Equal(t, "my_bucket", meta.Bucket)
		assert.Equal(t, "my_client_x509", meta.ClientCertURL)
		assert.Equal(t, "my_email@mail.dapr", meta.ClientEmail)
		assert.Equal(t, "my_client_id", meta.ClientID)
		assert.Equal(t, "my_private_key", meta.PrivateKey)
		assert.Equal(t, "my_private_key_id", meta.PrivateKeyID)
		assert.Equal(t, "my_project_id", meta.ProjectID)
		assert.Equal(t, "my_token_uri", meta.TokenURI)
		assert.Equal(t, "my_type", meta.Type)
		assert.Equal(t, false, meta.DecodeBase64)

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"decodeBase64": "true",
		}

		mergedMeta, err := meta.mergeWithRequestMetadata(&request)

		assert.Nil(t, err)

		assert.Equal(t, "my_auth_provider_x509", mergedMeta.AuthProviderCertURL)
		assert.Equal(t, "my_auth_uri", mergedMeta.AuthURI)
		assert.Equal(t, "my_bucket", mergedMeta.Bucket)
		assert.Equal(t, "my_client_x509", mergedMeta.ClientCertURL)
		assert.Equal(t, "my_email@mail.dapr", mergedMeta.ClientEmail)
		assert.Equal(t, "my_client_id", mergedMeta.ClientID)
		assert.Equal(t, "my_private_key", mergedMeta.PrivateKey)
		assert.Equal(t, "my_private_key_id", mergedMeta.PrivateKeyID)
		assert.Equal(t, "my_project_id", mergedMeta.ProjectID)
		assert.Equal(t, "my_token_uri", mergedMeta.TokenURI)
		assert.Equal(t, "my_type", mergedMeta.Type)
		assert.Equal(t, true, mergedMeta.DecodeBase64)
	})

	t.Run("Has invalid merged metadata decodeBase64", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"auth_provider_x509_cert_url": "my_auth_provider_x509",
			"auth_uri":                    "my_auth_uri",
			"Bucket":                      "my_bucket",
			"client_x509_cert_url":        "my_client_x509",
			"client_email":                "my_email@mail.dapr",
			"client_id":                   "my_client_id",
			"private_key":                 "my_private_key",
			"private_key_id":              "my_private_key_id",
			"project_id":                  "my_project_id",
			"token_uri":                   "my_token_uri",
			"type":                        "my_type",
			"decodeBase64":                "false",
		}
		gs := GCPStorage{logger: logger.NewLogger("test")}
		meta, err := gs.parseMetadata(m)
		assert.Nil(t, err)

		assert.Equal(t, "my_auth_provider_x509", meta.AuthProviderCertURL)
		assert.Equal(t, "my_auth_uri", meta.AuthURI)
		assert.Equal(t, "my_bucket", meta.Bucket)
		assert.Equal(t, "my_client_x509", meta.ClientCertURL)
		assert.Equal(t, "my_email@mail.dapr", meta.ClientEmail)
		assert.Equal(t, "my_client_id", meta.ClientID)
		assert.Equal(t, "my_private_key", meta.PrivateKey)
		assert.Equal(t, "my_private_key_id", meta.PrivateKeyID)
		assert.Equal(t, "my_project_id", meta.ProjectID)
		assert.Equal(t, "my_token_uri", meta.TokenURI)
		assert.Equal(t, "my_type", meta.Type)
		assert.Equal(t, false, meta.DecodeBase64)

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"decodeBase64": "hello",
		}

		mergedMeta, err := meta.mergeWithRequestMetadata(&request)

		assert.Nil(t, err)
		assert.NotNil(t, mergedMeta)
		assert.False(t, mergedMeta.DecodeBase64)
	})
	t.Run("Has invalid merged metadata encodeBase64", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"auth_provider_x509_cert_url": "my_auth_provider_x509",
			"auth_uri":                    "my_auth_uri",
			"Bucket":                      "my_bucket",
			"client_x509_cert_url":        "my_client_x509",
			"client_email":                "my_email@mail.dapr",
			"client_id":                   "my_client_id",
			"private_key":                 "my_private_key",
			"private_key_id":              "my_private_key_id",
			"project_id":                  "my_project_id",
			"token_uri":                   "my_token_uri",
			"type":                        "my_type",
			"decodeBase64":                "false",
			"encodeBase64":                "true",
		}
		gs := GCPStorage{logger: logger.NewLogger("test")}
		meta, err := gs.parseMetadata(m)
		assert.Nil(t, err)

		assert.Equal(t, "my_auth_provider_x509", meta.AuthProviderCertURL)
		assert.Equal(t, "my_auth_uri", meta.AuthURI)
		assert.Equal(t, "my_bucket", meta.Bucket)
		assert.Equal(t, "my_client_x509", meta.ClientCertURL)
		assert.Equal(t, "my_email@mail.dapr", meta.ClientEmail)
		assert.Equal(t, "my_client_id", meta.ClientID)
		assert.Equal(t, "my_private_key", meta.PrivateKey)
		assert.Equal(t, "my_private_key_id", meta.PrivateKeyID)
		assert.Equal(t, "my_project_id", meta.ProjectID)
		assert.Equal(t, "my_token_uri", meta.TokenURI)
		assert.Equal(t, "my_type", meta.Type)
		assert.Equal(t, false, meta.DecodeBase64)
		assert.Equal(t, true, meta.EncodeBase64)

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"encodeBase64": "hello",
		}

		mergedMeta, err := meta.mergeWithRequestMetadata(&request)

		assert.Nil(t, err)
		assert.NotNil(t, mergedMeta)
		assert.False(t, mergedMeta.EncodeBase64)
	})
}

func TestGetOption(t *testing.T) {
	gs := GCPStorage{logger: logger.NewLogger("test")}
	gs.metadata = &gcpMetadata{}
	t.Run("return error if key is missing", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		_, err := gs.get(context.TODO(), &r)
		assert.Error(t, err)
	})
}

func TestDeleteOption(t *testing.T) {
	gs := GCPStorage{logger: logger.NewLogger("test")}
	gs.metadata = &gcpMetadata{}

	t.Run("return error if key is missing", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		_, err := gs.delete(context.TODO(), &r)
		assert.Error(t, err)
	})
}
