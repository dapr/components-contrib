/*
Copyright 2023 The Dapr Authors
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

package oauth2

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

func TestMetadataValidation(t *testing.T) {
	log := logger.NewLogger("test")

	allMeta := map[string]string{
		"clientID":           "something",
		"clientSecret":       "something",
		"authURL":            "http://example.com",
		"tokenURL":           "http://example.com",
		"redirectURL":        "http://example.com",
		"tokenEncryptionKey": "something",
	}

	t.Run("clientID is required", func(t *testing.T) {
		md := &OAuth2MiddlewareMetadata{}
		meta := maps.Clone(allMeta)
		delete(meta, "clientID")
		err := md.FromMetadata(middleware.Metadata{metadata.Base{Properties: meta}}, log)
		require.ErrorContains(t, err, "clientID")
	})

	t.Run("clientSecret is required", func(t *testing.T) {
		md := &OAuth2MiddlewareMetadata{}
		meta := maps.Clone(allMeta)
		delete(meta, "clientSecret")
		err := md.FromMetadata(middleware.Metadata{metadata.Base{Properties: meta}}, log)
		require.ErrorContains(t, err, "clientSecret")
	})

	t.Run("authURL is required", func(t *testing.T) {
		md := &OAuth2MiddlewareMetadata{}
		meta := maps.Clone(allMeta)
		delete(meta, "authURL")
		err := md.FromMetadata(middleware.Metadata{metadata.Base{Properties: meta}}, log)
		require.ErrorContains(t, err, "authURL")
	})

	t.Run("tokenURL is required", func(t *testing.T) {
		md := &OAuth2MiddlewareMetadata{}
		meta := maps.Clone(allMeta)
		delete(meta, "tokenURL")
		err := md.FromMetadata(middleware.Metadata{metadata.Base{Properties: meta}}, log)
		require.ErrorContains(t, err, "tokenURL")
	})

	t.Run("redirectURL is required", func(t *testing.T) {
		md := &OAuth2MiddlewareMetadata{}
		meta := maps.Clone(allMeta)
		delete(meta, "redirectURL")
		err := md.FromMetadata(middleware.Metadata{metadata.Base{Properties: meta}}, log)
		require.ErrorContains(t, err, "redirectURL")
	})

	// TODO @ItalyPaleAle: uncomment for 1.13 when this becomes required
	/*
		t.Run("tokenEncryptionKey is required", func(t *testing.T) {
			md := &OAuth2MiddlewareMetadata{}
			meta := maps.Clone(allMeta)
			delete(meta, "tokenEncryptionKey")
			err := md.FromMetadata(middleware.Metadata{metadata.Base{Properties: meta}}, log)
			require.ErrorContains(t, err, "tokenEncryptionKey")
		})
	*/

	t.Run("default values", func(t *testing.T) {
		md := &OAuth2MiddlewareMetadata{}
		err := md.FromMetadata(middleware.Metadata{metadata.Base{Properties: allMeta}}, log)
		require.NoError(t, err)

		assert.Equal(t, modeCookie, md.Mode)
		assert.Equal(t, defaultAuthHeaderName, md.AuthHeaderName)
		assert.Equal(t, defaultCookieName, md.CookieName)
	})

	t.Run("modes", func(t *testing.T) {
		t.Run("default is cookie", func(t *testing.T) {
			md := &OAuth2MiddlewareMetadata{}
			meta := maps.Clone(allMeta)
			delete(meta, "mode")

			err := md.FromMetadata(middleware.Metadata{metadata.Base{Properties: meta}}, log)
			require.NoError(t, err)

			assert.Equal(t, modeCookie, md.Mode)
		})

		t.Run("set to cookie", func(t *testing.T) {
			md := &OAuth2MiddlewareMetadata{}
			meta := maps.Clone(allMeta)
			meta["mode"] = "COOKIE" // Should be case-insensitive

			err := md.FromMetadata(middleware.Metadata{metadata.Base{Properties: meta}}, log)
			require.NoError(t, err)

			assert.Equal(t, modeCookie, md.Mode)
		})

		t.Run("set to header", func(t *testing.T) {
			md := &OAuth2MiddlewareMetadata{}
			meta := maps.Clone(allMeta)
			meta["mode"] = "header"

			err := md.FromMetadata(middleware.Metadata{metadata.Base{Properties: meta}}, log)
			require.NoError(t, err)

			assert.Equal(t, modeHeader, md.Mode)
		})

		t.Run("invalid mode", func(t *testing.T) {
			md := &OAuth2MiddlewareMetadata{}
			meta := maps.Clone(allMeta)
			meta["mode"] = "invalid"

			err := md.FromMetadata(middleware.Metadata{metadata.Base{Properties: meta}}, log)
			require.ErrorContains(t, err, "mode")
		})
	})
}

func TestMetadataKeys(t *testing.T) {
	md := &OAuth2MiddlewareMetadata{
		TokenEncryptionKey: "Ciao.Mamma.Guarda.Come.Mi.Diverto",
	}

	err := md.setTokenKeys()
	require.NoError(t, err)

	require.NotNil(t, md.encKey)
	require.NotNil(t, md.sigKey)

	var encKeyRaw []byte
	err = md.encKey.Raw(&encKeyRaw)
	require.NoError(t, err)
	require.NotEmpty(t, encKeyRaw)

	var sigKeyRaw []byte
	err = md.sigKey.Raw(&sigKeyRaw)
	require.NoError(t, err)
	require.NotEmpty(t, sigKeyRaw)

	// Given the "token encryption key" passed as metadata above, these values should be constants as there's no randomness involved
	assert.Equal(t, "xM6r-SqHO14", md.encKey.KeyID())
	assert.Equal(t, "OV4ot3pasc17z5q4jOP5VA", base64.RawURLEncoding.EncodeToString(encKeyRaw))
	assert.Equal(t, "xM6r-SqHO14", md.sigKey.KeyID())
	assert.Equal(t, "R2w4G0ShOTKVb4KmBZyPych3BuPn4mOC6EYuNSXIiHU", base64.RawURLEncoding.EncodeToString(sigKeyRaw))
}
