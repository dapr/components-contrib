package wasm

import (
	"fmt"
	"io"
	"net/http"
	"net/url"

	"golang.org/x/net/context"
)

// httpFetcher decorates an http.Client with convenience methods.
type httpFetcher struct {
	c http.Client
}

// newHTTPFetcher is a constructor for httpFetcher.
//
// It is possible to plug a custom http.RoundTripper to handle other concerns (e.g. retries)
// Compression is handled transparently and automatically by http.Client.
func newHTTPFetcher(transport http.RoundTripper) *httpFetcher {
	return &httpFetcher{
		c: http.Client{Transport: transport},
	}
}

// fetch returns a byte slice of the wasm module found at the given URL, or an error otherwise.
func (f *httpFetcher) fetch(ctx context.Context, u *url.URL) ([]byte, error) {
	h := http.Header{}
	// Clear default user agent.
	h.Set("User-Agent", "")
	req := &http.Request{Method: http.MethodGet, URL: u, Header: h}
	resp, err := f.c.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("received %v status code from %q", resp.StatusCode, u)
	}

	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = resp.Body.Close()
	if err != nil {
		return nil, err
	}
	return bytes, nil
}
