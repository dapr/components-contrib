/*
Copyright 2026 The Dapr Authors
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

package spiffe

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"time"

	"golang.org/x/oauth2"

	kitspiffe "github.com/dapr/kit/crypto/spiffe"
)

const (
	// ExchangeTimeout bounds minting a JWT SVID and exchanging it.
	ExchangeTimeout = 30 * time.Second

	// earlyExpiry refreshes access tokens 2 minutes before they expire, as the
	// provider SDKs do.
	earlyExpiry = 2 * time.Minute
)

// ExchangeConfig describes a provider endpoint that trades a JWT for an access
// token over a JSON POST, and answers with access_token and expires_in.
type ExchangeConfig struct {
	TokenURL string
	// Audience is the audience of the minted JWT SVID.
	Audience string
	// JWTField is the request field that carries the JWT SVID: "assertion" for
	// the RFC 7523 JWT bearer grant, "subject_token" for RFC 8693 token exchange.
	JWTField string
	// Body holds the other request fields, such as grant_type.
	Body   map[string]string
	Client *http.Client
}

// NewExchangeTokenSource returns a TokenSource that caches the access token,
// and on every refresh exchanges a newly minted JWT SVID, since providers may
// reject a JWT that was already exchanged.
func NewExchangeTokenSource(minter kitspiffe.JWTSVIDMinter, cfg ExchangeConfig) oauth2.TokenSource {
	client := http.DefaultClient
	if cfg.Client != nil {
		client = cfg.Client
	}
	// A 307 or 308 redirect would resend the JWT SVID in the request body, so
	// redirects are returned as errors instead of followed.
	noRedirect := *client
	noRedirect.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	cfg.Client = &noRedirect

	return oauth2.ReuseTokenSourceWithExpiry(nil, &exchangeSource{minter: minter, cfg: cfg}, earlyExpiry)
}

type exchangeSource struct {
	minter kitspiffe.JWTSVIDMinter
	cfg    ExchangeConfig
}

func (s *exchangeSource) Token() (*oauth2.Token, error) {
	// oauth2.TokenSource has no ctx, so the exchange runs outside the request's
	// deadline. Tokens are cached for minutes, so this is rarely on the request
	// path.
	ctx, cancel := context.WithTimeout(context.Background(), ExchangeTimeout)
	defer cancel()

	jwt, err := MintJWT(ctx, s.minter, s.cfg.Audience)
	if err != nil {
		return nil, err
	}

	body := maps.Clone(s.cfg.Body)
	if body == nil {
		body = make(map[string]string, 1)
	}
	body[s.cfg.JWTField] = jwt
	reqBody, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.TokenURL, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.cfg.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("token exchange with %s failed: %w", s.cfg.TokenURL, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("failed to read token exchange response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("token exchange with %s failed with status %d: %.200s", s.cfg.TokenURL, resp.StatusCode, respBody)
	}

	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err = json.Unmarshal(respBody, &out); err != nil {
		return nil, fmt.Errorf("failed to decode token exchange response: %w", err)
	}
	if out.AccessToken == "" {
		return nil, errors.New("token exchange response has no access_token")
	}
	// A zero expiry would make the token cache keep the token forever.
	if out.ExpiresIn <= 0 {
		return nil, errors.New("token exchange response has no expires_in")
	}

	return &oauth2.Token{
		AccessToken: out.AccessToken,
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(time.Duration(out.ExpiresIn) * time.Second),
	}, nil
}
