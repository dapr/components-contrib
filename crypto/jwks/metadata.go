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
