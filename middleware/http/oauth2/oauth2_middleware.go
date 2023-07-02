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
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwe"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/spf13/cast"
	"golang.org/x/oauth2"

	"github.com/dapr/components-contrib/internal/httputils"
	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

const (
	// Timeout for authenticating with the IdP
	authenticationTimeout = 10 * time.Minute
	// Allowed clock skew for validating JWTs
	allowedClockSkew = 5 * time.Minute
	// Issuer for JWTs
	jwtIssuer = "oauth2.dapr.io"

	claimToken    = "token"
	claimRedirect = "redirect"
	claimState    = "state"
)

// NewOAuth2Middleware returns a new oAuth2 middleware.
func NewOAuth2Middleware(log logger.Logger) middleware.Middleware {
	return &Middleware{
		logger: log,
	}
}

// Middleware is an oAuth2 authentication middleware.
type Middleware struct {
	logger logger.Logger
	meta   oAuth2MiddlewareMetadata
}

const (
	savedState   = "auth-state"
	redirectPath = "redirect-url"
)

// GetHandler retruns the HTTP handler provided by the middleware.
func (m *Middleware) GetHandler(ctx context.Context, metadata middleware.Metadata) (func(next http.Handler) http.Handler, error) {
	err := m.meta.fromMetadata(metadata, m.logger)
	if err != nil {
		return nil, fmt.Errorf("invalid metadata: %w", err)
	}

	// Derive the cookie keys
	err = m.meta.setCookieKeys()
	if err != nil {
		return nil, fmt.Errorf("failed to derive cookie keys: %w", err)
	}

	// Create the OAuth2 configuration object
	m.meta.setOAuth2Conf()

	return m.handler, nil
}

func (m *Middleware) handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// To check if the request is coming in using HTTPS, we check if the scheme of the URL is "https"
		// Checking for `r.TLS` alone may not work if Dapr is behind a proxy that does TLS termination
		secureCookie := r.URL.Scheme == "https"

		// Get the token from the cookie
		claims, err := m.getClaimsFromCookie(r)
		if err != nil {
			// If the cookie is invalid, redirect to the auth endpoint again
			// This will overwrite the old cookie
			m.logger.Debugf("Invalid session cookie: %v", err)
			m.redirectToAuthenticationEndpoint(w, r.URL, secureCookie)
			return
		}

		// If we already have a token, forward the request to the app
		if claims[claimToken] != "" {
			r.Header.Add(m.meta.AuthHeaderName, claims[claimToken])
			next.ServeHTTP(w, r)
			return
		}

		// If we have the "state" and "code" parameter, we need to exchange the authorization code for the access token
		code := r.URL.Query().Get("code")
		state := r.URL.Query().Get("state")
		if code != "" && state != "" {
			m.exchangeAccessCode(r.Context(), w, claims, code, state, r.URL.Host, secureCookie)
			return
		}

		// Redirect to the auhentication endpoint
		m.redirectToAuthenticationEndpoint(w, r.URL, secureCookie)
	})
}

func (m *Middleware) redirectToAuthenticationEndpoint(w http.ResponseWriter, redirectURL *url.URL, secureCookie bool) {
	// Generate a new state token
	stateObj, err := uuid.NewRandom()
	if err != nil {
		httputils.RespondWithError(w, http.StatusInternalServerError)
		m.logger.Errorf("Failed to generate UUID: %v", err)
		return
	}
	state := stateObj.String()

	if m.meta.ForceHTTPS {
		redirectURL.Scheme = "https"
	}

	// Set the cookie with the state and redirect URL
	err = m.setSecureCookie(w, map[string]string{
		claimState:    state,
		claimRedirect: redirectURL.String(),
	}, authenticationTimeout, redirectURL.Host, secureCookie)
	if err != nil {
		httputils.RespondWithError(w, http.StatusInternalServerError)
		m.logger.Errorf("Failed to set secure cookie: %w", err)
		return
	}

	// Redirect to the auth endpoint
	url := m.meta.oauth2Conf.AuthCodeURL(state, oauth2.AccessTypeOffline)
	httputils.RespondWithRedirect(w, http.StatusFound, url)
}

func (m *Middleware) exchangeAccessCode(ctx context.Context, w http.ResponseWriter, claims map[string]string, code string, state string, domain string, secureCookie bool) {
	if claims[claimRedirect] == "" {
		httputils.RespondWithError(w, http.StatusInternalServerError)
		m.logger.Error("Missing claim 'redirect'")
		return
	}

	if claims[claimState] == "" || state != claims[claimState] {
		httputils.RespondWithErrorAndMessage(w, http.StatusBadRequest, "invalid state")
		return
	}

	// Exchange the authorization code for a token
	token, err := m.meta.oauth2Conf.Exchange(ctx, code)
	if err != nil {
		httputils.RespondWithError(w, http.StatusInternalServerError)
		m.logger.Error("Failed to exchange token")
		return
	}

	// If we don't have an expiration, assume it's 1 hour
	exp := time.Until(token.Expiry)
	if exp <= time.Second {
		exp = time.Hour
	}

	// Set the cookie
	err = m.setSecureCookie(w, map[string]string{
		claimToken: token.Type() + " " + token.AccessToken,
	}, exp, domain, secureCookie)
	if err != nil {
		httputils.RespondWithError(w, http.StatusInternalServerError)
		m.logger.Errorf("Failed to set secure cookie: %w", err)
		return
	}

	// Redirect to the URL set in the request
	httputils.RespondWithRedirect(w, http.StatusFound, claims[claimRedirect])
}

func (m *Middleware) getClaimsFromCookie(r *http.Request) (map[string]string, error) {
	// Get the cookie, which should contain a JWE
	cookie, err := r.Cookie(m.meta.CookieName)
	if errors.Is(err, http.ErrNoCookie) || cookie.Valid() != nil || cookie.Value == "" {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to retrieve cookie: %w", err)
	}

	// Decrypt the encrypted (JWE) cookie
	dec, err := jwe.Decrypt(
		[]byte(cookie.Value),
		jwe.WithKey(jwa.A128KW, m.meta.cek),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt cookie: %w", err)
	}

	// Validate the JWT from the decrypted cookie
	token, err := jwt.Parse(dec,
		jwt.WithKey(jwa.HS256, m.meta.csk),
		jwt.WithIssuer(jwtIssuer),
		jwt.WithAudience(m.meta.ClientID), // Use the client ID as audience
		jwt.WithAcceptableSkew(allowedClockSkew),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to validate JWT token: %w", err)
	}

	return cast.ToStringMapString(token.PrivateClaims()), nil
}

func (m *Middleware) setSecureCookie(w http.ResponseWriter, claims map[string]string, ttl time.Duration, domain string, isSecure bool) error {
	now := time.Now()
	exp := now.Add(ttl)

	// Build the JWT
	builder := jwt.NewBuilder().
		IssuedAt(now).
		Issuer(jwtIssuer).
		Audience([]string{m.meta.ClientID}).
		Expiration(exp).
		NotBefore(now)
	for k, v := range claims {
		builder.Claim(k, v)
	}
	token, err := builder.Build()
	if err != nil {
		return fmt.Errorf("error building JWT: %w", err)
	}

	// Generate the encrypted JWT
	val, err := jwt.NewSerializer().
		Sign(
			jwt.WithKey(jwa.HS256, m.meta.csk),
		).
		Encrypt(
			jwt.WithKey(jwa.A128KW, m.meta.cek),
			jwt.WithEncryptOption(jwe.WithContentEncryption(jwa.A128GCM)),
		).
		Serialize(token)
	if err != nil {
		return fmt.Errorf("failed to serialize token: %w", err)
	}

	// Set the cookie
	http.SetCookie(w, &http.Cookie{
		Name:     m.meta.CookieName,
		Value:    string(val),
		MaxAge:   int(ttl.Seconds()),
		HttpOnly: true,
		Secure:   isSecure,
		Domain:   domain,
		Path:     "/",
		SameSite: http.SameSiteLaxMode,
	})

	return nil
}

func (m *Middleware) unsetCookie(w http.ResponseWriter, isSecure bool) {
	// To delete the cookie, create a new cookie with the same name, but no value and that has already expired
	http.SetCookie(w, &http.Cookie{
		Name:     m.meta.CookieName,
		Value:    "",
		Expires:  time.Now().Add(-24 * time.Hour),
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   isSecure,
		Path:     "/",
		SameSite: http.SameSiteLaxMode,
	})
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*oAuth2MiddlewareMetadata, error) {
	var middlewareMetadata oAuth2MiddlewareMetadata
	err := mdutils.DecodeMetadata(metadata.Properties, &middlewareMetadata)
	if err != nil {
		return nil, err
	}
	return &middlewareMetadata, nil
}

func (m *Middleware) GetComponentMetadata() map[string]string {
	metadataStruct := oAuth2MiddlewareMetadata{}
	metadataInfo := map[string]string{}
	mdutils.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, mdutils.MiddlewareType)
	return metadataInfo
}
