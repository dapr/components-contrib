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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	t.Run("Has correct metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"authProviderX509CertURL": "my_auth_provider_x509",
			"authURI":                 "my_auth_uri",
			"Bucket":                  "my_bucket",
			"clientX509CertURL":       "my_client_x509",
			"clientEmail":             "my_email@mail.dapr",
			"clientID":                "my_client_id",
			"privateKey":              "my_private_key",
			"privateKeyID":            "my_private_key_id",
			"projectID":               "my_project_id",
			"tokenURI":                "my_token_uri",
			"type":                    "my_type",
			"signTTL":                 "15s",
		}
		gs := GCPStorage{logger: logger.NewLogger("test")}
		meta, err := gs.parseMetadata(m)
		require.NoError(t, err)

		t.Run("Metadata is correctly decoded", func(t *testing.T) {
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
			assert.Equal(t, "15s", meta.SignTTL)
		})

		t.Run("Metadata is correctly marshalled to JSON", func(t *testing.T) {
			json, err := json.Marshal(meta)
			require.NoError(t, err)
			assert.Equal(t,
				"{\"type\":\"my_type\",\"project_id\":\"my_project_id\",\"private_key_id\":\"my_private_key_id\","+
					"\"private_key\":\"my_private_key\",\"client_email\":\"my_email@mail.dapr\",\"client_id\":\"my_client_id\","+
					"\"auth_uri\":\"my_auth_uri\",\"token_uri\":\"my_token_uri\",\"auth_provider_x509_cert_url\":\"my_auth_provider_x509\","+
					"\"client_x509_cert_url\":\"my_client_x509\",\"bucket\":\"my_bucket\",\"decodeBase64\":\"false\","+
					"\"encodeBase64\":\"false\",\"signTTL\":\"15s\"}", string(json))
		})
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
			"authProviderX509CertURL": "my_auth_provider_x509",
			"authURI":                 "my_auth_uri",
			"Bucket":                  "my_bucket",
			"clientX509CertURL":       "my_client_x509",
			"clientEmail":             "my_email@mail.dapr",
			"clientID":                "my_client_id",
			"privateKey":              "my_private_key",
			"privateKeyID":            "my_private_key_id",
			"projectID":               "my_project_id",
			"tokenURI":                "my_token_uri",
			"type":                    "my_type",
			"decodeBase64":            "false",
		}
		gs := GCPStorage{logger: logger.NewLogger("test")}
		meta, err := gs.parseMetadata(m)
		require.NoError(t, err)

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
		assert.False(t, meta.DecodeBase64)

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"decodeBase64": "true",
		}

		mergedMeta, err := meta.mergeWithRequestMetadata(&request)

		require.NoError(t, err)

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
		assert.True(t, mergedMeta.DecodeBase64)
	})

	t.Run("Has invalid merged metadata decodeBase64", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"authProviderX509CertURL": "my_auth_provider_x509",
			"authURI":                 "my_auth_uri",
			"Bucket":                  "my_bucket",
			"clientX509CertURL":       "my_client_x509",
			"clientEmail":             "my_email@mail.dapr",
			"clientID":                "my_client_id",
			"privateKey":              "my_private_key",
			"privateKeyID":            "my_private_key_id",
			"projectID":               "my_project_id",
			"tokenURI":                "my_token_uri",
			"type":                    "my_type",
			"decodeBase64":            "false",
		}
		gs := GCPStorage{logger: logger.NewLogger("test")}
		meta, err := gs.parseMetadata(m)
		require.NoError(t, err)

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
		assert.False(t, meta.DecodeBase64)

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"decodeBase64": "hello",
		}

		mergedMeta, err := meta.mergeWithRequestMetadata(&request)

		require.NoError(t, err)
		assert.NotNil(t, mergedMeta)
		assert.False(t, mergedMeta.DecodeBase64)
	})
	t.Run("Has invalid merged metadata encodeBase64", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"authProviderX509CertURL": "my_auth_provider_x509",
			"authURI":                 "my_auth_uri",
			"Bucket":                  "my_bucket",
			"clientX509CertURL":       "my_client_x509",
			"clientEmail":             "my_email@mail.dapr",
			"clientID":                "my_client_id",
			"privateKey":              "my_private_key",
			"privateKeyID":            "my_private_key_id",
			"projectID":               "my_project_id",
			"tokenURI":                "my_token_uri",
			"type":                    "my_type",
			"decodeBase64":            "false",
			"encodeBase64":            "true",
		}
		gs := GCPStorage{logger: logger.NewLogger("test")}
		meta, err := gs.parseMetadata(m)
		require.NoError(t, err)

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
		assert.False(t, meta.DecodeBase64)
		assert.True(t, meta.EncodeBase64)

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"encodeBase64": "hello",
		}

		mergedMeta, err := meta.mergeWithRequestMetadata(&request)

		require.NoError(t, err)
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
		require.Error(t, err)
	})
}

func TestDeleteOption(t *testing.T) {
	gs := GCPStorage{logger: logger.NewLogger("test")}
	gs.metadata = &gcpMetadata{}

	t.Run("return error if key is missing", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		_, err := gs.delete(context.TODO(), &r)
		require.Error(t, err)
	})
}
