package wasm

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/sys"

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

	// StrictSandbox when true uses fake sources to avoid vulnerabilities such
	// as timing attacks.
	//
	// # Affected configuration
	//
	//   - sys.Walltime increments with a constant value when read, initially a
	//     second resolution of the current system time.
	//   - sys.Nanotime increments with a constant value when read, initially
	//     zero.
	//   - sys.Nanosleep returns immediately.
	//   - Random number generators are seeded with a deterministic source.
	StrictSandbox bool `mapstructure:"strictSandbox"`

	// Guest is WebAssembly binary implementing the guest, loaded from URL.
	Guest []byte `mapstructure:"-"`

	// GuestName is module name of the guest, loaded from URL.
	GuestName string `mapstructure:"-"`
}

// GetInitMetadata returns InitMetadata from the input metadata.
func GetInitMetadata(md metadata.Base) (*InitMetadata, error) {
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
		u, err := url.Parse(m.URL)
		if err != nil {
			return nil, err
		}
		fetcher := newHTTPFetcher(http.DefaultTransport)
		m.Guest, err = fetcher.fetch(context.Background(), u)
		if err != nil {
			return nil, err
		}
		m.GuestName, _ = strings.CutSuffix(path.Base(u.Path), ".wasm")
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

// NewModuleConfig returns a new module config appropriate for the initialized
// metadata.
func NewModuleConfig(m *InitMetadata) wazero.ModuleConfig {
	if !m.StrictSandbox {
		// The below violate sand-boxing, but allow code to behave as expected.
		return wazero.NewModuleConfig().
			WithRandSource(rand.Reader).
			WithSysNanotime().
			WithSysWalltime().
			WithSysNanosleep()
	}

	// wazero's default is strict as defined here, except walltime. wazero
	// does not return a real clock reading by default for performance and
	// determinism reasons.
	// See https://github.com/tetratelabs/wazero/blob/main/RATIONALE.md#syswalltime-and-nanotime
	return wazero.NewModuleConfig().
		WithWalltime(newFakeWalltime(), sys.ClockResolution(time.Millisecond))
}

func newFakeWalltime() sys.Walltime {
	t := time.Now().Unix() * int64(time.Second)
	return func() (sec int64, nsec int32) {
		wt := atomic.AddInt64(&t, int64(time.Millisecond))
		return wt / 1e9, int32(wt % 1e9)
	}
}
