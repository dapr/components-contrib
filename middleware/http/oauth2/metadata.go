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
	"crypto/sha256"
	"errors"
	"strings"

	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
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
	// Configure token storage.
	// Possible values: "memory" (default) and "cookie"
	TokenStorage string `json:"tokenStorage" mapstructure:"tokenStorage"`
	// Name of the cookie where Dapr will store the encrypted access token, when storing tokens in cookies.
	// Defaults to "_dapr_oauth2".
	CookieName string `json:"cookieName" mapstructure:"cookieName"`
	// Cookie encryption key.
	// Required if storing access tokens in cookies.
	CookieEncryptionKey string `json:"cookieEncryptionKey" mapstructure:"cookieEncryptionKey"`
}

// Parse the component's metadata into the object.
func (md *oAuth2MiddlewareMetadata) fromMetadata(metadata middleware.Metadata) error {
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

	switch strings.ToLower(md.TokenStorage) {
	case "cookie":
		// Re-set to ensure it's lowercase
		md.TokenStorage = "cookie"
		if md.CookieEncryptionKey == "" {
			return errors.New("field 'cookieEncryptionKey' is required when storing tokens in cookies")
		}
	case "memory", "": // Default value
		// Re-set to ensure it's lowercase
		md.TokenStorage = "memory"
	default:
		return errors.New("invalid value for property 'tokenStorage'; supported values: 'memory', 'cookie'")
	}

	return nil
}

// GetCookieEncryptionKey derives a 128-bit cookie encryption key from the user-defined value.
func (md *oAuth2MiddlewareMetadata) GetCookieEncryptionKey() []byte {
	if md.CookieEncryptionKey == "" {
		// This should never happen as the validation method ensures that cookieEncryptionKey isn't empty
		return nil
	}

	h := hmac.New(sha256.New, []byte(hmacKey))
	h.Write([]byte(md.CookieEncryptionKey))
	res := h.Sum(nil)

	// Return the first 16 bytes only
	return res[:16]
}
