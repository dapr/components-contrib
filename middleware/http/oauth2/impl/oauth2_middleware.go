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

// Package impl contains the abstract, shared implementation for the OAuth2 middleware.
package impl

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
	"github.com/dapr/kit/logger"
)

const (
	// Timeout for authenticating with the IdP
	authenticationTimeout = 10 * time.Minute
	// Allowed clock skew for validating JWTs
	allowedClockSkew = 5 * time.Minute
	// Issuer for JWTs
	jwtIssuer = "oauth2.dapr.io"

	claimToken     = "tkn"
	claimTokenType = "tkt"
	claimRedirect  = "redirect"
	claimState     = "state"
)

// ErrTokenTooLargeForCookie is returned by SetSecureCookie when the token is too large to be stored in a cookie
var ErrTokenTooLargeForCookie = errors.New("token is too large to be stored in a cookie")

// OAuth2Middleware is an oAuth2 authentication middleware.
type OAuth2Middleware struct {
	logger logger.Logger
	meta   OAuth2MiddlewareMetadata

	// GetClaimsFn is the function invoked to retrieve the claims from the request.
	GetClaimsFn func(r *http.Request) (map[string]string, error)
	// SetClaimsFn is the function invoked to store claims in the response.
	SetClaimsFn func(w http.ResponseWriter, claims map[string]string, ttl time.Duration, domain string, secureContext bool) error
}

// SetMetadata sets the metadata in the object.
func (m *OAuth2Middleware) SetMetadata(meta OAuth2MiddlewareMetadata) {
	m.meta = meta
}

// GetHandler retruns the HTTP handler provided by the middleware.
func (m *OAuth2Middleware) GetHandler(ctx context.Context) (func(next http.Handler) http.Handler, error) {
	// Derive the token keys
	err := m.meta.setTokenKeys()
	if err != nil {
		return nil, fmt.Errorf("failed to derive token keys: %w", err)
	}

	// Create the OAuth2 configuration object
	m.meta.setOAuth2Conf()

	return m.handler, nil
}

func (m *OAuth2Middleware) handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// To check if the request is coming in using HTTPS, we check if the scheme of the URL is "https"
		// Checking for `r.TLS` alone may not work if Dapr is behind a proxy that does TLS termination
		secureContext := r.URL.Scheme == "https"

		// If we have the "state" and "code" parameter, we need to exchange the authorization code for the access token
		code := r.URL.Query().Get("code")
		state := r.URL.Query().Get("state")
		if code != "" && state != "" {
			// Always get the claims from the cookies in this case
			claims, err := m.GetClaimsFromCookie(r)
			if err != nil {
				// If the cookie is invalid, redirect to the auth endpoint again
				// This will overwrite the old cookie
				m.logger.Debugf("Invalid session cookie: %v", err)
				m.redirectToAuthenticationEndpoint(w, r.URL, secureContext)
				return
			}

			m.exchangeAccessCode(r.Context(), w, claims, code, state, r.URL.Host, secureContext)
			return
		}

		// Get the token from the request
		// This uses the function provided by the implementation
		claims, err := m.GetClaimsFn(r)
		if err != nil {
			// If the function returns an error, redirect to the auth endpoint again
			m.logger.Debugf("Invalid session cookie: %v", err)
			m.redirectToAuthenticationEndpoint(w, r.URL, secureContext)
			return
		}

		// If we already have a token, forward the request to the app
		if claims[claimToken] != "" {
			if claims[claimTokenType] != "" {
				r.Header.Add(m.meta.AuthHeaderName, claims[claimTokenType]+" "+claims[claimToken])
			} else {
				r.Header.Add(m.meta.AuthHeaderName, claims[claimToken])
			}
			next.ServeHTTP(w, r)
			return
		}

		// Redirect to the auhentication endpoint
		m.redirectToAuthenticationEndpoint(w, r.URL, secureContext)
	})
}

func (m *OAuth2Middleware) redirectToAuthenticationEndpoint(w http.ResponseWriter, redirectURL *url.URL, secureContext bool) {
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
	err = m.SetSecureCookie(w, map[string]string{
		claimState:    state,
		claimRedirect: redirectURL.String(),
	}, authenticationTimeout, redirectURL.Host, secureContext)
	if err != nil {
		httputils.RespondWithError(w, http.StatusInternalServerError)
		m.logger.Errorf("Failed to set secure cookie: %v", err)
		return
	}

	// Redirect to the auth endpoint
	url := m.meta.oauth2Conf.AuthCodeURL(state, oauth2.AccessTypeOffline)
	httputils.RespondWithRedirect(w, http.StatusFound, url)
}

func (m *OAuth2Middleware) exchangeAccessCode(ctx context.Context, w http.ResponseWriter, claims map[string]string, code string, state string, domain string, secureContext bool) {
	if len(claims) == 0 || claims[claimRedirect] == "" {
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

	// Set the claims in the response
	err = m.SetClaimsFn(w, map[string]string{
		claimTokenType: token.Type(),
		claimToken:     token.AccessToken,
	}, exp, domain, secureContext)
	if err != nil {
		httputils.RespondWithError(w, http.StatusInternalServerError)
		m.logger.Errorf("Failed to set token in the response: %v", err)
		return
	}

	// Redirect to the URL set in the request
	httputils.RespondWithRedirect(w, http.StatusFound, claims[claimRedirect])
}

func (m *OAuth2Middleware) ParseToken(token string) (map[string]string, error) {
	// Decrypt the encrypted (JWE) token
	dec, err := jwe.Decrypt(
		[]byte(token),
		jwe.WithKey(jwa.A128KW, m.meta.encKey),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt token: %w", err)
	}

	// Validate the JWT from the decrypted token
	tk, err := jwt.Parse(dec,
		jwt.WithKey(jwa.HS256, m.meta.sigKey),
		jwt.WithIssuer(jwtIssuer),
		jwt.WithAudience(m.meta.ClientID), // Use the client ID as audience
		jwt.WithAcceptableSkew(allowedClockSkew),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to validate JWT token: %w", err)
	}

	return cast.ToStringMapString(tk.PrivateClaims()), nil
}

// GetClaimsFromCookie retrieves the claims from the cookie.
func (m *OAuth2Middleware) GetClaimsFromCookie(r *http.Request) (map[string]string, error) {
	// Get the cookie, which should contain a JWE
	cookie, err := r.Cookie(m.meta.CookieName)
	if errors.Is(err, http.ErrNoCookie) || cookie.Valid() != nil || cookie.Value == "" {
		//nolint:nilerr
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to retrieve cookie: %w", err)
	}

	return m.ParseToken(cookie.Value)
}

// CreateToken generates an encrypted JWT containing the claims.
func (m *OAuth2Middleware) CreateToken(claims map[string]string, ttl time.Duration) (string, error) {
	now := time.Now()
	exp := now.Add(ttl)

	// Build the JWT
	builder := jwt.NewBuilder().
		IssuedAt(now).
		Issuer(jwtIssuer).
		Audience([]string{m.meta.ClientID}).
		Expiration(exp).
		NotBefore(now)
	var claimsSize int
	for k, v := range claims {
		builder.Claim(k, v)
		claimsSize += len(v)
	}
	token, err := builder.Build()
	if err != nil {
		return "", fmt.Errorf("error building JWT: %w", err)
	}

	// Generate the encrypted JWT
	var encryptOpts []jwt.EncryptOption
	if claimsSize > 800 {
		// If the total size of the claims is more than 800 bytes, we should enable compression
		encryptOpts = []jwt.EncryptOption{
			jwt.WithKey(jwa.A128KW, m.meta.encKey),
			jwt.WithEncryptOption(jwe.WithContentEncryption(jwa.A128GCM)),
			jwt.WithEncryptOption(jwe.WithCompress(jwa.Deflate)),
		}
	} else {
		encryptOpts = []jwt.EncryptOption{
			jwt.WithKey(jwa.A128KW, m.meta.encKey),
			jwt.WithEncryptOption(jwe.WithContentEncryption(jwa.A128GCM)),
		}
	}
	val, err := jwt.NewSerializer().
		Sign(
			jwt.WithKey(jwa.HS256, m.meta.sigKey),
		).
		Encrypt(encryptOpts...).
		Serialize(token)
	if err != nil {
		return "", fmt.Errorf("failed to serialize token: %w", err)
	}

	return string(val), nil
}

func (m *OAuth2Middleware) SetSecureCookie(w http.ResponseWriter, claims map[string]string, ttl time.Duration, domain string, isSecure bool) error {
	token, err := m.CreateToken(claims, ttl)
	if err != nil {
		return err
	}

	// Generate the cookie
	cookie := http.Cookie{
		Name:     m.meta.CookieName,
		Value:    token,
		MaxAge:   int(ttl.Seconds()),
		HttpOnly: true,
		Secure:   isSecure,
		Domain:   domain,
		Path:     "/",
		SameSite: http.SameSiteLaxMode,
	}
	cookieStr := cookie.String()

	// Browsers have a maximum size of 4093 bytes, and if the cookie is larger than that (by looking at the entire value of the header), it is silently rejected
	// Some info: https://stackoverflow.com/a/4604212/192024
	if len(cookieStr) > 4093 {
		return ErrTokenTooLargeForCookie
	}

	// Finally set the cookie
	w.Header().Add("Set-Cookie", cookieStr)

	return nil
}

func (m *OAuth2Middleware) UnsetCookie(w http.ResponseWriter, isSecure bool) {
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

func (m *OAuth2Middleware) GetComponentMetadata() map[string]string {
	metadataStruct := OAuth2MiddlewareMetadata{}
	metadataInfo := map[string]string{}
	mdutils.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, mdutils.MiddlewareType)
	return metadataInfo
}
