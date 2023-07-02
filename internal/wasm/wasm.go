package wasm

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/dapr/components-contrib/metadata"
)

type InitMetadata struct {
	// URL is how to load a `%.wasm` file that implements a command, usually
	// compiled to target WASI.
	//
	// If no scheme is present, this defaults to oci://, referencing an OCI
	// image. The wasm is identified be ENTRYPOINT and any other files in the
	// corresponding layers will be mounted read-only as the root file system.
	//
	// Other valid schemes are file:// for a local file or http[s]:// for one
	// retrieved via HTTP. In these cases, no filesystem will be mounted.
	URL string `mapstructure:"url"`

	// Guest is WebAssembly binary implementing the guest, loaded from URL.
	Guest []byte `mapstructure:"-"`

	// GuestName is module name of the guest, loaded from URL.
	GuestName string `mapstructure:"-"`
}

// GetInitMetadata returns InitMetadata from the input metadata.
func GetInitMetadata(ctx context.Context, md metadata.Base) (*InitMetadata, error) {
	// Note: the ctx will be used for other schemes such as HTTP and OCI.

	var m InitMetadata
	// Decode the metadata
	err := metadata.DecodeMetadata(md.Properties, &m)
	if err != nil {
		return nil, err
	}

	if m.URL == "" {
		return nil, errors.New("missing url")
	}

	firstColon := strings.IndexByte(m.URL, ':')
	if firstColon == -1 {
		return nil, fmt.Errorf("invalid URL: %s", m.URL)
	}

	scheme := m.URL[:firstColon]
	switch m.URL[:firstColon] {
	case "oci":
		return nil, fmt.Errorf("TODO %s", scheme)
	case "http", "https":
		_, err = url.Parse(m.URL)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("TODO %s", scheme)
	case "file":
		guestPath := m.URL[7:]
		m.Guest, err = os.ReadFile(guestPath)
		if err != nil {
			return nil, err
		}
		// Use the name of the wasm binary as the module name.
		m.GuestName, _ = strings.CutSuffix(path.Base(guestPath), ".wasm")
	default:
		return nil, fmt.Errorf("unsupported URL scheme: %s", scheme)
	}

	return &m, nil
}
