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

package spiffe

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/common/authentication/spiffe/spiffetest"
	spiffecontext "github.com/dapr/kit/crypto/spiffe/context"
)

func TestMinterFrom(t *testing.T) {
	_, err := MinterFrom(t.Context())
	require.Error(t, err)

	minter := new(spiffetest.Minter)
	got, err := MinterFrom(spiffecontext.WithJWTMinter(t.Context(), minter))
	require.NoError(t, err)
	assert.Same(t, minter, got)
}

func TestExchangeTokenSource(t *testing.T) {
	type exchange struct {
		Body        map[string]string
		ContentType string
	}

	newServer := func(t *testing.T, status int, resp string) (*httptest.Server, *[]exchange) {
		var got []exchange
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodPost, r.Method)
			var body map[string]string
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			got = append(got, exchange{Body: body, ContentType: r.Header.Get("Content-Type")})
			w.WriteHeader(status)
			w.Write([]byte(resp))
		}))
		t.Cleanup(srv.Close)
		return srv, &got
	}

	t.Run("exchanges a new JWT SVID and caches the access token", func(t *testing.T) {
		srv, got := newServer(t, http.StatusOK, `{"access_token":"sk-ant-oat01-test","token_type":"Bearer","expires_in":600}`)
		minter := new(spiffetest.Minter)

		ts := NewExchangeTokenSource(minter, ExchangeConfig{
			TokenURL: srv.URL,
			Audience: "https://api.anthropic.com",
			JWTField: "assertion",
			Body: map[string]string{
				"grant_type":         "urn:ietf:params:oauth:grant-type:jwt-bearer",
				"federation_rule_id": "fdrl_1",
			},
		})

		tok, err := ts.Token()
		require.NoError(t, err)
		assert.Equal(t, "sk-ant-oat01-test", tok.AccessToken)
		assert.Equal(t, "Bearer", tok.Type())
		assert.WithinDuration(t, time.Now().Add(10*time.Minute), tok.Expiry, 5*time.Second)

		_, err = ts.Token()
		require.NoError(t, err)

		require.Len(t, *got, 1, "second Token call must be served from the cache")
		require.Len(t, minter.Tokens(), 1)
		assert.Equal(t, "application/json", (*got)[0].ContentType)
		assert.Equal(t, map[string]string{
			"grant_type":         "urn:ietf:params:oauth:grant-type:jwt-bearer",
			"federation_rule_id": "fdrl_1",
			"assertion":          minter.Tokens()[0],
		}, (*got)[0].Body)
	})

	t.Run("each refresh exchanges a newly minted JWT SVID", func(t *testing.T) {
		// expires_in is inside the early expiry window, so every call refreshes.
		srv, got := newServer(t, http.StatusOK, `{"access_token":"tok","expires_in":60}`)
		minter := new(spiffetest.Minter)
		ts := NewExchangeTokenSource(minter, ExchangeConfig{TokenURL: srv.URL, Audience: "aud", JWTField: "subject_token"})

		for range 2 {
			_, err := ts.Token()
			require.NoError(t, err)
		}

		require.Len(t, *got, 2)
		tokens := minter.Tokens()
		require.Len(t, tokens, 2)
		assert.NotEqual(t, tokens[0], tokens[1])
		assert.Equal(t, tokens[0], (*got)[0].Body["subject_token"])
		assert.Equal(t, tokens[1], (*got)[1].Body["subject_token"])
	})

	t.Run("errors", func(t *testing.T) {
		for name, test := range map[string]struct {
			status int
			resp   string
			expErr string
		}{
			"rejected":           {status: http.StatusUnauthorized, resp: `{"error":"invalid_grant"}`, expErr: "status 401: {\"error\":\"invalid_grant\"}"},
			"no access token":    {status: http.StatusOK, resp: `{"expires_in":600}`, expErr: "no access_token"},
			"no expiry":          {status: http.StatusOK, resp: `{"access_token":"tok"}`, expErr: "no expires_in"},
			"malformed response": {status: http.StatusOK, resp: `not json`, expErr: "decode"},
		} {
			t.Run(name, func(t *testing.T) {
				srv, _ := newServer(t, test.status, test.resp)
				_, err := NewExchangeTokenSource(new(spiffetest.Minter), ExchangeConfig{TokenURL: srv.URL, Audience: "aud", JWTField: "assertion"}).Token()
				require.ErrorContains(t, err, test.expErr)
			})
		}
	})

	t.Run("mint errors are returned without calling the provider", func(t *testing.T) {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
		t.Cleanup(srv.Close)

		minter := &spiffetest.Minter{Err: errors.New("sentry unavailable")}
		_, err := NewExchangeTokenSource(minter, ExchangeConfig{TokenURL: srv.URL, Audience: "aud", JWTField: "assertion"}).Token()
		require.ErrorContains(t, err, "sentry unavailable")
		assert.Zero(t, calls.Load())
	})
}
