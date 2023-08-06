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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

type bearerMiddlewareMetadata struct {
	// Issuer authority.
	Issuer string `json:"issuer" mapstructure:"issuer" mapstructurealiases:"issuerURL"`
	// Audience to expect in the token (usually, a client ID).
	Audience string `json:"audience" mapstructure:"audience" mapstructurealiases:"clientID"`
	// Optional address of the JKWS file.
	// If missing, will try to fetch the URL set in the OpenID Configuration document `<issuer>/.well-known/openid-configuration`.
	JWKSURL string `json:"jwksURL" mapstructure:"jwksURL"`

	// Internal properties
	logger logger.Logger `json:"-" mapstructure:"-"`
}

// Parse the component's metadata into the object.
func (md *bearerMiddlewareMetadata) fromMetadata(metadata middleware.Metadata) error {
	// Decode the properties
	err := mdutils.DecodeMetadata(metadata.Properties, md)
	if err != nil {
		return err
	}

	// Validate properties
	if md.Issuer == "" {
		return errors.New("metadata property 'issuer' is required")
	}
	if md.Audience == "" {
		return errors.New("metadata property 'audience' is required")
	}

	return nil
}

// Contains a subset of the properties defined in the openid-configuration document.
// See: https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderConfig .
type openIDConfigurationJSON struct {
	Issuer  string `json:"issuer"`
	JWKSURL string `json:"jwks_uri"`
}

// Retrieves the OpenID Configuration document
func (md *bearerMiddlewareMetadata) retrieveOpenIDConfigurationDocument(ctx context.Context) error {
	// If we already have a fixed JWKS URL, use that
	if md.JWKSURL != "" {
		md.logger.Debug("Using JWKS URL from metadata: " + md.JWKSURL)
		return nil
	}

	// Retrieve the openid-configuration document
	oidcConfigURL := strings.TrimSuffix(md.Issuer, "/") + "/.well-known/openid-configuration"
	md.logger.Debug("Fetching OpenID Configuration: " + oidcConfigURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, oidcConfigURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer func() {
		// Drain before closing
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
	}()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid response status code: %d", res.StatusCode)
	}

	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	// Parse and validate the response
	oidcConfig := openIDConfigurationJSON{}
	err = json.Unmarshal(resBody, &oidcConfig)
	if err != nil {
		return fmt.Errorf("failed to decode JSON response: %w", err)
	}

	if oidcConfig.JWKSURL == "" {
		return errors.New("the OpenID Configuration document does not contain a 'jwks_uri'")
	}

	// Ensure that the issuer matches
	// Note that we do not strip trailing slashes, so the user-supplied issuer must match (and that's per specs)
	if oidcConfig.Issuer != md.Issuer {
		return fmt.Errorf("the issuer found in the OpenID Configuration document ('%s') doesn't match the one provided in the component's metadata ('%s')", oidcConfig.Issuer, md.Issuer)
	}

	// Update the object and return
	md.JWKSURL = oidcConfig.JWKSURL
	md.logger.Debug("Found JWKS URL: " + md.JWKSURL)

	return nil
}
