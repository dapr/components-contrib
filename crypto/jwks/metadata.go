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

package jwks

import (
	"errors"
	"time"

	contribCrypto "github.com/dapr/components-contrib/crypto"
	"github.com/dapr/components-contrib/metadata"
)

const (
	defaultRequestTimeout     = 30 * time.Second
	defaultMinRefreshInterval = 10 * time.Minute
)

type jwksMetadata struct {
	// The JWKS to use. Can be one of:
	// - The actual JWKS as a JSON-encoded string (optionally encoded with Base64-standard).
	// - A URL to a HTTP(S) endpoint returning the JWKS.
	// - A path to a local file containing the JWKS.
	// Required.
	JWKS string `json:"jwks" mapstructure:"jwks"`
	// Timeout for network requests, as a Go duration string (e.g. "30s")
	// Defaults to "30s".
	RequestTimeout time.Duration `json:"requestTimeout" mapstructure:"requestTimeout"`
	// Minimum interval before the JWKS is refreshed, as a Go duration string.
	// Only applies when the JWKS is fetched from a HTTP(S) URL.
	// Defaults to "10m".
	MinRefreshInterval time.Duration `json:"minRefreshInterval" mapstructure:"minRefreshInterval"`
}

func (m *jwksMetadata) InitWithMetadata(meta contribCrypto.Metadata) error {
	m.reset()

	// Decode the metadata
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return err
	}

	// Require the JWKS property to not be empty (further validation will be performed by the component)
	if m.JWKS == "" {
		return errors.New("metadata property 'jwks' is required")
	}

	// Set default requestTimeout and minRefreshInterval if empty
	if m.RequestTimeout < time.Millisecond {
		m.RequestTimeout = defaultRequestTimeout
	}
	if m.MinRefreshInterval < time.Second {
		m.MinRefreshInterval = defaultMinRefreshInterval
	}

	return nil
}

// Reset the object
func (m *jwksMetadata) reset() {
	m.JWKS = ""
	m.RequestTimeout = defaultRequestTimeout
	m.MinRefreshInterval = defaultMinRefreshInterval
}
