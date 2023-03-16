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

package bearer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	newMetadata := func(md map[string]string) (*bearerMiddlewareMetadata, error) {
		obj := &bearerMiddlewareMetadata{
			logger: logger.NewLogger("test"),
		}

		err := obj.fromMetadata(middleware.Metadata{Base: metadata.Base{
			Name:       "test",
			Properties: md,
		}})

		return obj, err
	}

	t.Run("required fields only", func(t *testing.T) {
		md, err := newMetadata(map[string]string{
			"issuer":   "http://localhost",
			"audience": "foo",
		})
		require.NoError(t, err)
		assert.Equal(t, "http://localhost", md.Issuer)
		assert.Equal(t, "foo", md.Audience)
	})

	t.Run("include JWKS URL", func(t *testing.T) {
		md, err := newMetadata(map[string]string{
			"issuer":   "http://localhost",
			"audience": "foo",
			"jwksURL":  "http://localhost/jwks.json",
		})
		require.NoError(t, err)
		assert.Equal(t, "http://localhost", md.Issuer)
		assert.Equal(t, "foo", md.Audience)
		assert.Equal(t, "http://localhost/jwks.json", md.JWKSURL)
	})

	t.Run("issuerURL alias for issuer", func(t *testing.T) {
		md, err := newMetadata(map[string]string{
			"issuerURL": "http://localhost",
			"audience":  "foo",
		})
		require.NoError(t, err)
		assert.Equal(t, "http://localhost", md.Issuer)
		assert.Equal(t, "foo", md.Audience)
	})

	t.Run("clientID alias for audience", func(t *testing.T) {
		md, err := newMetadata(map[string]string{
			"issuer":   "http://localhost",
			"clientID": "foo",
		})
		require.NoError(t, err)
		assert.Equal(t, "http://localhost", md.Issuer)
		assert.Equal(t, "foo", md.Audience)
	})

	t.Run("aliases do not have priority", func(t *testing.T) {
		md, err := newMetadata(map[string]string{
			"issuer":    "http://localhost",
			"issuerURL": "http://localhost2",
			"audience":  "foo",
			"clientID":  "bar",
		})
		require.NoError(t, err)
		assert.Equal(t, "http://localhost", md.Issuer)
		assert.Equal(t, "foo", md.Audience)
	})

	t.Run("missing issuer", func(t *testing.T) {
		_, err := newMetadata(map[string]string{
			"audience": "foo",
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, "metadata property 'issuer' is required")
	})

	t.Run("missing audience", func(t *testing.T) {
		_, err := newMetadata(map[string]string{
			"issuer": "http://localhost",
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, "metadata property 'audience' is required")
	})
}
