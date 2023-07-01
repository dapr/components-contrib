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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"

	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

const (
	defaultAuthHeaderName = "Authorization"
	defaultTokenStorage   = "memory"
	defaultCookieName     = "_dapr_oauth2"

	// Key used to derive cookie encryption keys using HMAC.
	hmacKey = "dapr_oauth2"
)

// Metadata is the oAuth middleware config.
type oAuth2MiddlewareMetadata struct {
	// Client ID of the OAuth2 application.
	// Required.
	ClientID string `json:"clientID" mapstructure:"clientID"`
	// Client secret of the OAuth2 application.
	// Required.
	ClientSecret string `json:"clientSecret" mapstructure:"clientSecret"`
	// Scopes to request, as a comma-separated string
	Scopes string `json:"scopes" mapstructure:"scopes"`
	// URL of the OAuth2 authorization server.
	// Required.
	AuthURL string `json:"authURL" mapstructure:"authURL"`
	// URL of the OAuth2 token endpoint, used to exchange an authorization code for an access token.
	// Required.
	TokenURL string `json:"tokenURL" mapstructure:"tokenURL"`
	// Name of the header forwarded to the application, containing the token.
	// Default: "Authorization".
	AuthHeaderName string `json:"authHeaderName" mapstructure:"authHeaderName"`
	// The URL of your application that the authorization server should redirect to once the user has authenticated.
	// Required.
	RedirectURL string `json:"redirectURL" mapstructure:"redirectURL"`
	// Forces the use of TLS/HTTPS for the redirect URL.
	// Defaults to false.
	ForceHTTPS bool `json:"forceHTTPS" mapstructure:"forceHTTPS"`
	// Name of the cookie where Dapr will store the encrypted access token, when storing tokens in cookies.
	// Defaults to "_dapr_oauth2".
	CookieName string `json:"cookieName" mapstructure:"cookieName"`
	// Cookie encryption key.
	// Required to allow sessions to persist across restarts of the Dapr runtime and to allow multiple instances of Dapr to access the session.
	// Not setting an explicit encryption key is deprecated, and this field will become required in Dapr 1.13.
	// TODO @ItalyPaleAle: make required in Dapr 1.13.
	CookieEncryptionKey string `json:"cookieEncryptionKey" mapstructure:"cookieEncryptionKey"`
}

// Parse the component's metadata into the object.
func (md *oAuth2MiddlewareMetadata) fromMetadata(metadata middleware.Metadata, log logger.Logger) error {
	// Set default values
	if md.AuthHeaderName == "" {
		md.AuthHeaderName = defaultAuthHeaderName
	}
	if md.CookieName == "" {
		md.CookieName = defaultCookieName
	}

	// Decode the properties
	err := mdutils.DecodeMetadata(metadata.Properties, md)
	if err != nil {
		return err
	}

	// Check required fields
	if md.ClientID == "" {
		return errors.New("required field 'clientID' is empty")
	}
	if md.ClientSecret == "" {
		return errors.New("required field 'clientSecret' is empty")
	}
	if md.AuthURL == "" {
		return errors.New("required field 'authURL' is empty")
	}
	if md.TokenURL == "" {
		return errors.New("required field 'tokenURL' is empty")
	}
	if md.RedirectURL == "" {
		return errors.New("required field 'redirectURL' is empty")
	}

	// If there's no cookie encryption key, show a warning
	// TODO @ItalyPaleAle: make required in Dapr 1.13.
	if md.CookieEncryptionKey == "" {
		log.Warnf("[DEPRECATION NOTICE] Initializing the OAuth2 middleware with an empty 'cookieEncryptionKey' is deprecated, and the field will become required in Dapr 1.13. Setting an explicit 'cookieEncryptionKey' is required to allow sessions to be shared across multiple instances of Dapr and to survive a restart of Dapr.")
	}

	return nil
}

// GetCookieEncryptionKey derives a 128-bit cookie encryption key from the user-defined value.
func (md *oAuth2MiddlewareMetadata) GetCookieEncryptionKey() []byte {
	if md.CookieEncryptionKey == "" {
		// TODO @ItalyPaleAle: uncomment for Dapr 1.13 and remove existing code in this block
		/*
			// This should never happen as the validation method ensures that cookieEncryptionKey isn't empty
			// So if we're here, it means there was a development-time error.
			panic("cookie encryption key is empty")
		*/

		// If the user didn't provide a cookie encryption key, generate a random one
		// Naturally, this means that the cookie encryption key is unique to this process and cookies cannot be decrypted by other instances of Dapr or if the process is restarted
		// This is not good, but it is no different than how this component behaved in Dapr 1.11.
		// This behavior is deprecated and will be removed in Dapr 1.13.
		cek := make([]byte, 16)
		_, err := io.ReadFull(rand.Reader, cek)
		if err != nil {
			// This makes Dapr panic, but it's ok here because:
			// 1. This code is temporary and will be removed in Dapr 1.13. I would rather not change the interface of this method to return an error since it won't be needed in the future
			// 2. Errors from io.ReadFull above are possible but highly unlikely (only if the kernel doesn't have enough entropy)
			panic("Failed to generate a random cookie encryption key: " + err.Error())
		}
		return cek
	}

	h := hmac.New(sha256.New, []byte(hmacKey))
	h.Write([]byte(md.CookieEncryptionKey))
	res := h.Sum(nil)

	// Return the first 16 bytes only
	return res[:16]
}
