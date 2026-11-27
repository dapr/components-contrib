/*
Copyright 2026 The Dapr Authors
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
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/lestrrat-go/jwx/v3/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

func TestBearerMiddlewareTokenSerialization(t *testing.T) {
	const (
		issuer   = "https://issuer.example.com"
		audience = "my-audience"
	)

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	privKey, err := jwk.Import(rsaKey)
	require.NoError(t, err)
	require.NoError(t, privKey.Set(jwk.KeyIDKey, "key-1"))
	pubKey, err := privKey.PublicKey()
	require.NoError(t, err)
	keySet := jwk.NewSet()
	require.NoError(t, keySet.AddKey(pubKey))
	keySetJSON, err := json.Marshal(keySet)
	require.NoError(t, err)

	jwksSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write(keySetJSON)
	}))
	t.Cleanup(jwksSrv.Close)

	handlerFn, err := NewBearerMiddleware(logger.NewLogger("test")).GetHandler(t.Context(), middleware.Metadata{Base: metadata.Base{
		Properties: map[string]string{
			"issuer":   issuer,
			"audience": audience,
			"jwksURL":  jwksSrv.URL,
		},
	}})
	require.NoError(t, err)
	handler := handlerFn(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Returns the status code the middleware responds with for the given token
	invoke := func(t *testing.T, token []byte) int {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+string(token))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Code
	}

	newToken := func(t *testing.T, aud string) jwt.Token {
		t.Helper()
		now := time.Now()
		token, err := jwt.NewBuilder().
			Issuer(issuer).
			Audience([]string{aud}).
			IssuedAt(now).
			Expiration(now.Add(5 * time.Minute)).
			Build()
		require.NoError(t, err)
		return token
	}

	t.Run("compact serialization is accepted", func(t *testing.T) {
		signed, err := jwt.Sign(newToken(t, audience), jwt.WithKey(jwa.RS256(), privKey))
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, invoke(t, signed))
	})

	t.Run("compact serialization for another audience is rejected", func(t *testing.T) {
		signed, err := jwt.Sign(newToken(t, "another-audience"), jwt.WithKey(jwa.RS256(), privKey))
		require.NoError(t, err)
		assert.Equal(t, http.StatusUnauthorized, invoke(t, signed))
	})

	t.Run("JWS JSON serialization is rejected", func(t *testing.T) {
		// jwx v2's jwt.Parse accepted the JWS JSON serialization and read the claims from the outer object,
		// which let a caller re-claim a once-signed token. Only the compact serialization may be accepted,
		// even when the JSON serialization is validly signed.
		payload, err := json.Marshal(newToken(t, audience))
		require.NoError(t, err)
		signed, err := jws.Sign(payload, jws.WithKey(jwa.RS256(), privKey), jws.WithJSON())
		require.NoError(t, err)
		assert.Equal(t, http.StatusUnauthorized, invoke(t, signed))
	})
}
