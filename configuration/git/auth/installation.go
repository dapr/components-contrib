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

package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// InstallationToken is the GitHub-App installation access token returned
// by the GitHub API.
type InstallationToken struct {
	Token     string
	ExpiresAt time.Time
}

// InstallationTokenFetcher exchanges a JWT for a GitHub App installation
// token. It is injectable so tests can avoid real HTTP. The client argument
// is supplied by the caller so connection pooling is reused across
// refreshes; tests pass a stub or a httptest-backed client.
type InstallationTokenFetcher func(ctx context.Context, client *http.Client, apiBase string, installationID int64, jwtToken string) (*InstallationToken, error)

// RateLimitError indicates the GitHub API rejected the request with a 429
// or 403-with-rate-limit-headers response. RetryAfter conveys the duration
// the server asked us to wait (zero if no Retry-After header was present,
// in which case the caller should fall back to its configured retry-after
// default).
type RateLimitError struct {
	RetryAfter time.Duration
	Err        error
}

func (e *RateLimitError) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("github api rate-limited (retry-after %s): %v", e.RetryAfter, e.Err)
	}
	return fmt.Sprintf("github api rate-limited: %v", e.Err)
}

func (e *RateLimitError) Unwrap() error { return e.Err }

// DefaultInstallationTokenFetcher exchanges a JWT for an installation
// token against the real GitHub API.
//
// On a 429 response (or 403 with rate-limit headers — GitHub uses 403 for
// secondary rate limits), Retry-After is parsed and the call is retried
// once. If the second attempt also fails with a rate-limit response, a
// *RateLimitError is returned so the poll loop can back off.
//
// Error messages exclude the response body — a misconfigured or
// compromised apiBase could echo sensitive content back. The 64 KiB
// LimitReader caps memory in case the body is unexpectedly large.
func DefaultInstallationTokenFetcher(ctx context.Context, client *http.Client, apiBase string, installationID int64, jwtToken string) (*InstallationToken, error) {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	tok, err := doInstallationTokenRequest(ctx, client, apiBase, installationID, jwtToken)
	var rateErr *RateLimitError
	if errors.As(err, &rateErr) && rateErr.RetryAfter > 0 {
		// Wait for the server-suggested duration, then retry once.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(rateErr.RetryAfter):
		}
		tok, err = doInstallationTokenRequest(ctx, client, apiBase, installationID, jwtToken)
	}
	return tok, err
}

func doInstallationTokenRequest(ctx context.Context, client *http.Client, apiBase string, installationID int64, jwtToken string) (*InstallationToken, error) {
	url := fmt.Sprintf("%s/app/installations/%d/access_tokens", apiBase, installationID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+jwtToken)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if isRateLimited(resp) {
		return nil, &RateLimitError{
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
			Err:        fmt.Errorf("github api: status %d (installation %d)", resp.StatusCode, installationID),
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("github api: status %d (installation %d)", resp.StatusCode, installationID)
	}

	var payload struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if payload.Token == "" {
		return nil, errors.New("github api returned empty token")
	}
	return &InstallationToken{Token: payload.Token, ExpiresAt: payload.ExpiresAt}, nil
}

// isRateLimited returns true if the response indicates the request was
// rate-limited by GitHub. GitHub uses both 429 and 403 (with the
// X-RateLimit-Remaining: 0 header) for primary and secondary rate limits.
func isRateLimited(resp *http.Response) bool {
	if resp.StatusCode == http.StatusTooManyRequests {
		return true
	}
	if resp.StatusCode == http.StatusForbidden &&
		(resp.Header.Get("X-RateLimit-Remaining") == "0" ||
			resp.Header.Get("Retry-After") != "") {
		return true
	}
	return false
}

// parseRetryAfter interprets an HTTP Retry-After header value, which may
// be either an integer number of seconds or an HTTP-date. Returns 0 when
// the header is empty or unparseable.
func parseRetryAfter(v string, now time.Time) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if when, err := http.ParseTime(v); err == nil {
		if d := time.Until(when); d > 0 {
			return d
		}
		// HTTP-date already in the past — fall through to "no wait".
		_ = now
	}
	return 0
}
