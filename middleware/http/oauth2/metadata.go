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
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/lestrrat-go/jwx/v2/jwk"
	"golang.org/x/oauth2"

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
	// Cookie encryption and signing key (technically, seed used to derive those two).
	// It is recommended to provide a random string with sufficient entropy.
	// Required to allow sessions to persist across restarts of the Dapr runtime and to allow multiple instances of Dapr to access the session.
	// Not setting an explicit encryption key is deprecated, and this field will become required in Dapr 1.13.
	// TODO @ItalyPaleAle: make required in Dapr 1.13.
	CookieKey string `json:"cookieKey" mapstructure:"cookieKey"`

	// Internal: cookie encryption key
	cek jwk.Key
	// Internal: cookie signing key
	csk jwk.Key
	// Internal: OAuth2 configuration object
	oauth2Conf oauth2.Config
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
	if md.CookieKey == "" {
		log.Warnf("[DEPRECATION NOTICE] Initializing the OAuth2 middleware with an empty 'cookieKey' is deprecated, and the field will become required in Dapr 1.13. Setting an explicit 'cookieKey' is required to allow sessions to be shared across multiple instances of Dapr and to survive a restart of Dapr.")
	}

	return nil
}

// Derives a 128-bit cookie encryption key and a 256-bit cookie signing key from the user-provided value.
func (md *oAuth2MiddlewareMetadata) setCookieKeys() (err error) {
	var b []byte

	if md.CookieKey == "" {
		// TODO @ItalyPaleAle: uncomment for Dapr 1.13 and remove existing code in this block
		/*
			// This should never happen as the validation method ensures that cookieKey isn't empty
			// So if we're here, it means there was a development-time error.
			panic("cookie encryption key is empty")
		*/

		// If the user didn't provide a cookie key, generate a random one
		// Naturally, this means that the cookie key is unique to this process and cookies cannot be decrypted by other instances of Dapr or if the process is restarted
		// This is not good, but it is no different than how this component behaved in Dapr 1.11.
		// This behavior is deprecated and will be removed in Dapr 1.13.
		b = make([]byte, 48)
		_, err := io.ReadFull(rand.Reader, b)
		if err != nil {
			return fmt.Errorf("failed to generate a random cookie key: %w", err)
		}
	} else {
		// Derive 48 bytes from the cookie key using HMAC with a fixed "HMAC key"
		h := hmac.New(crypto.SHA384.New, []byte(hmacKey))
		h.Write([]byte(md.CookieKey))
		b = h.Sum(nil)
	}

	// We must set a kid for the jwx library to work
	kidH := sha256.New224()
	kidH.Write(b)
	kid := base64.RawURLEncoding.EncodeToString(kidH.Sum(nil))

	// Cookie encryption key uses 128 bits
	md.cek, err = jwk.FromRaw(b[:16])
	if err != nil {
		return fmt.Errorf("failed to import cookie encryption key: %w", err)
	}
	md.cek.Set("kid", kid)

	// Cookie signing key uses 256 bits
	md.csk, err = jwk.FromRaw(b[16:])
	if err != nil {
		return fmt.Errorf("failed to import cookie signing key: %w", err)
	}
	md.csk.Set("kid", kid)

	return nil
}

// Sets the oauth2Conf property in the object.
func (md *oAuth2MiddlewareMetadata) setOAuth2Conf() {
	md.oauth2Conf = oauth2.Config{
		ClientID:     md.ClientID,
		ClientSecret: md.ClientSecret,
		Scopes:       strings.Split(md.Scopes, ","),
		RedirectURL:  md.RedirectURL,
		Endpoint: oauth2.Endpoint{
			AuthURL:  md.AuthURL,
			TokenURL: md.TokenURL,
		},
	}
}
