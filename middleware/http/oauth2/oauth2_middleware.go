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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwe"
	"github.com/lestrrat-go/jwx/v2/jwk"
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
	// Min size of the access token returned by the IdP, in bytes, before it's compressed
	tokenCompressionThreshold = 800

	claimToken     = "tkn"
	claimTokenType = "tkt"
	claimState     = "state"
	claimRedirect  = "redirect"

	headerName   = "Authorization"
	bearerPrefix = "bearer "
)

// ErrTokenTooLargeForCookie is returned by SetSecureCookie when the token is too large to be stored in a cookie
var ErrTokenTooLargeForCookie = errors.New("token is too large to be stored in a cookie")

// OAuth2Middleware is an OAuth2 authentication middleware.
type OAuth2Middleware struct {
	// Function invoked to retrieve the token from the request.
	getTokenFn func(r *http.Request) (map[string]string, error)
	// Optional function invoked before redirecting to the authorization endpoint that allows setting additional claims in the state cookie.
	claimsForAuthFn func(r *http.Request) (map[string]string, error)
	// Function invoked to store the token in the response.
	setTokenFn func(w http.ResponseWriter, r *http.Request, reqClaims map[string]string, token string, exp time.Duration)

	logger logger.Logger
	meta   OAuth2MiddlewareMetadata
}

// NewOAuth2Middleware returns a new OAuth2 middleware.
func NewOAuth2Middleware(log logger.Logger) middleware.Middleware {
	return &OAuth2Middleware{
		logger: log,
	}
}

// GetHandler retruns the HTTP handler provided by the middleware.
func (m *OAuth2Middleware) GetHandler(ctx context.Context, metadata middleware.Metadata) (func(next http.Handler) http.Handler, error) {
	err := m.meta.FromMetadata(metadata, m.logger)
	if err != nil {
		return nil, fmt.Errorf("invalid metadata: %w", err)
	}

	// Derive the token keys
	err = m.meta.setTokenKeys()
	if err != nil {
		return nil, fmt.Errorf("failed to derive token keys: %w", err)
	}

	// Create the OAuth2 configuration object
	m.meta.setOAuth2Conf()

	// Set the callbacks depending on the mode of operation
	switch m.meta.Mode {
	case modeCookie:
		m.getTokenFn = m.GetClaimsFromCookie
		m.setTokenFn = m.cookieModeSetTokenInResponse
		m.claimsForAuthFn = m.cookieModeClaimsForAuth
	case modeHeader:
		m.getTokenFn = m.headerModeGetClaimsFromHeader
		m.setTokenFn = m.headerModeSetTokenResponse
	}

	return m.handler, nil
}

func (m *OAuth2Middleware) handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
				m.redirectToAuthenticationEndpoint(w, r)
				return
			}

			m.exchangeAccessCode(w, r, claims, code, state)
			return
		}

		// Get the token from the request
		// This uses the function provided by the implementation
		claims, err := m.getTokenFn(r)
		if err != nil {
			// If the function returns an error, redirect to the auth endpoint again
			m.logger.Debugf("Invalid session: %v", err)
			m.redirectToAuthenticationEndpoint(w, r)
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
		m.redirectToAuthenticationEndpoint(w, r)
	})
}

func (m *OAuth2Middleware) redirectToAuthenticationEndpoint(w http.ResponseWriter, r *http.Request) {
	// Do this here in case ClaimsForAuthFn modifies the request object
	domain := r.URL.Host
	isSecureContext := IsRequestSecure(r)

	// Generate a new state token
	stateObj, err := uuid.NewRandom()
	if err != nil {
		httputils.RespondWithError(w, http.StatusInternalServerError)
		m.logger.Errorf("Failed to generate UUID: %v", err)
		return
	}
	state := stateObj.String()

	// Get additional claims from the implementation
	var claims map[string]string
	if m.claimsForAuthFn != nil {
		claims, err = m.claimsForAuthFn(r)
		if err != nil {
			httputils.RespondWithError(w, http.StatusInternalServerError)
			m.logger.Errorf("Failed to retrieve claims for the authentication endpoint: %v", err)
			return
		}
	} else {
		claims = make(map[string]string, 1)
	}

	// Set the cookie with the state token
	claims[claimState] = state
	err = m.setCookieWithClaims(w, claims, authenticationTimeout, domain, isSecureContext)
	if err != nil {
		httputils.RespondWithError(w, http.StatusInternalServerError)
		m.logger.Errorf("Failed to set secure cookie: %v", err)
		return
	}

	// Redirect to the auth endpoint
	url := m.meta.oauth2Conf.AuthCodeURL(state, oauth2.AccessTypeOffline)
	httputils.RespondWithRedirect(w, http.StatusFound, url)
}

func (m *OAuth2Middleware) exchangeAccessCode(w http.ResponseWriter, r *http.Request, reqClaims map[string]string, code string, state string) {
	if len(reqClaims) == 0 || reqClaims[claimState] == "" || state != reqClaims[claimState] {
		httputils.RespondWithErrorAndMessage(w, http.StatusBadRequest, "invalid state")
		return
	}

	// Exchange the authorization code for a accessToken
	accessToken, err := m.meta.oauth2Conf.Exchange(r.Context(), code)
	if err != nil {
		httputils.RespondWithError(w, http.StatusInternalServerError)
		m.logger.Error("Failed to exchange token")
		return
	}

	// If we don't have an expiration, set it to 1hr
	exp := time.Until(accessToken.Expiry)
	if exp <= time.Second {
		exp = time.Hour
	}

	// Encrypt the token
	token, err := m.CreateToken(map[string]string{
		claimTokenType: accessToken.Type(),
		claimToken:     accessToken.AccessToken,
	}, exp)
	if err != nil {
		httputils.RespondWithError(w, http.StatusInternalServerError)
		m.logger.Errorf("Failed to generate token: %v", err)
		return
	}

	// Allow the implementation to send the response
	m.setTokenFn(w, r, reqClaims, token, exp)
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
	return createEncryptedJWT(claims, ttl, []string{m.meta.ClientID}, m.meta.encKey, m.meta.sigKey)
}

// Creates an encrypted JWT given the encryption and signing keys.
// Split into a separate function to allow better unit testing.
func createEncryptedJWT(claims map[string]string, ttl time.Duration, audience []string, encKey jwk.Key, sigKey jwk.Key) (string, error) {
	now := time.Now()
	exp := now.Add(ttl)

	// Build the JWT
	builder := jwt.NewBuilder().
		IssuedAt(now).
		Issuer(jwtIssuer).
		Audience(audience).
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
	if claimsSize > tokenCompressionThreshold {
		// If the total size of the claims is more than tokenCompressionThreshold, we should enable compression
		encryptOpts = []jwt.EncryptOption{
			jwt.WithKey(jwa.A128KW, encKey),
			jwt.WithEncryptOption(jwe.WithContentEncryption(jwa.A128GCM)),
			jwt.WithEncryptOption(jwe.WithCompress(jwa.Deflate)),
		}
	} else {
		encryptOpts = []jwt.EncryptOption{
			jwt.WithKey(jwa.A128KW, encKey),
			jwt.WithEncryptOption(jwe.WithContentEncryption(jwa.A128GCM)),
		}
	}
	val, err := jwt.NewSerializer().
		Sign(
			jwt.WithKey(jwa.HS256, sigKey),
		).
		Encrypt(encryptOpts...).
		Serialize(token)
	if err != nil {
		return "", fmt.Errorf("failed to serialize token: %w", err)
	}

	return string(val), nil
}

func (m *OAuth2Middleware) setCookieWithClaims(w http.ResponseWriter, claims map[string]string, ttl time.Duration, domain string, isSecure bool) error {
	token, err := m.CreateToken(claims, ttl)
	if err != nil {
		return err
	}

	return m.SetCookie(w, token, ttl, domain, isSecure)
}

func (m *OAuth2Middleware) SetCookie(w http.ResponseWriter, value string, ttl time.Duration, domain string, isSecure bool) error {
	// Generate the cookie
	cookie := http.Cookie{
		Name:     m.meta.CookieName,
		Value:    value,
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

func (m *OAuth2Middleware) cookieModeClaimsForAuth(r *http.Request) (map[string]string, error) {
	// Get the redirect URL
	redirectURL := r.URL
	if m.meta.ForceHTTPS {
		redirectURL.Scheme = "https"
	}

	return map[string]string{
		claimRedirect: redirectURL.String(),
	}, nil
}

func (m *OAuth2Middleware) cookieModeSetTokenInResponse(w http.ResponseWriter, r *http.Request, reqClaims map[string]string, token string, exp time.Duration) {
	if reqClaims[claimRedirect] == "" {
		httputils.RespondWithError(w, http.StatusInternalServerError)
		m.logger.Error("Missing claim 'redirect'")
		return
	}

	// Set the claims in the response
	err := m.SetCookie(w, token, exp, r.URL.Host, IsRequestSecure(r))
	if err != nil {
		httputils.RespondWithError(w, http.StatusInternalServerError)
		m.logger.Errorf("Failed to set cookie in the response: %v", err)
		return
	}

	// Redirect to the URL set in the request
	httputils.RespondWithRedirect(w, http.StatusFound, reqClaims[claimRedirect])
}

func (m *OAuth2Middleware) headerModeGetClaimsFromHeader(r *http.Request) (map[string]string, error) {
	// Get the header which should contain the JWE
	// The "Bearer " prefix is optional
	header := r.Header.Get(headerName)
	if header == "" {
		return nil, nil
	}
	if len(header) > len(bearerPrefix) && strings.ToLower(header[0:len(bearerPrefix)]) == bearerPrefix {
		header = header[len(bearerPrefix):]
	}

	return m.ParseToken(header)
}

func (m *OAuth2Middleware) headerModeSetTokenResponse(w http.ResponseWriter, r *http.Request, reqClaims map[string]string, token string, exp time.Duration) {
	// Delete the state token cookie
	m.UnsetCookie(w, IsRequestSecure(r))

	// Set the response in the body
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.Encode(map[string]string{
		headerName: token,
	})
}

func (m *OAuth2Middleware) GetComponentMetadata() map[string]string {
	metadataStruct := OAuth2MiddlewareMetadata{}
	metadataInfo := map[string]string{}
	mdutils.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, mdutils.MiddlewareType)
	return metadataInfo
}

// IsRequestSecure returns true if the request is using a secure context.
func IsRequestSecure(r *http.Request) bool {
	// To check if the request is coming in using HTTPS, we check if the scheme of the URL is "https"
	// Checking for `r.TLS` alone may not work if Dapr is behind a proxy that does TLS termination
	return r.URL.Scheme == "https"
}
