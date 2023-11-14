/*
Copyright 2023 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implieout.
See the License for the specific language governing permissions and
limitations under the License.
*/

package wasm

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// httpClient decorates an http.Client with convenience methods.
type httpClient struct {
	c http.Client
}

// newHTTPFetcher is a constructor for httpFetcher.
//
// It is possible to plug a custom http.RoundTripper to handle other concerns (e.g. retries)
// Compression is handled transparently and automatically by http.Client.
func newHTTPCLient(transport http.RoundTripper) *httpClient {
	return &httpClient{
		c: http.Client{Transport: transport},
	}
}

// fetch returns a byte slice of the wasm module found at the given URL, or an error otherwise.
func (f *httpClient) get(ctx context.Context, u *url.URL) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := f.c.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("received %v status code from %q", resp.StatusCode, u)
	}

	bytes, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	return bytes, nil
}
