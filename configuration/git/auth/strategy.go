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

// Package auth implements the auth strategies used by configuration.git.
// Each strategy resolves a go-git AuthMethod, refreshing internal credentials
// over time if needed (GitHub App installation tokens have a ~1h TTL).
package auth

import (
	"context"
	"strings"

	"github.com/go-git/go-git/v5/plumbing/transport"
)

// Strategy resolves a go-git AuthMethod for use against the upstream git
// remote. Implementations must be safe for concurrent calls to AuthMethod
// from the poller and clone paths.
type Strategy interface {
	AuthMethod(ctx context.Context) (transport.AuthMethod, error)
	Close() error
}

// IsTransportRateLimit returns true if err appears to be a rate-limit
// response surfaced from a go-git HTTP/HTTPS transport. go-git does not
// expose structured response status, so this is a string-pattern check —
// best-effort but it covers GitHub's typical error shapes.
func IsTransportRateLimit(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "429") ||
		strings.Contains(msg, "rate limit") ||
		strings.Contains(msg, "secondary rate limit")
}
