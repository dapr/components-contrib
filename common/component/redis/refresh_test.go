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

package redis

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kitlogger "github.com/dapr/kit/logger"
)

// fakeRedisClient records the AUTH command issued via DoWrite. The embedded
// RedisClient interface satisfies the remaining methods (they panic if called,
// which no test does).
type fakeRedisClient struct {
	RedisClient
	doWriteCalls chan []interface{}
}

func (f *fakeRedisClient) DoWrite(_ context.Context, args ...interface{}) error {
	f.doWriteCalls <- args
	return nil
}

func (f *fakeRedisClient) Close() error {
	return nil
}

func TestRunTokenRefreshLoop(t *testing.T) {
	// A token this short-lived schedules its first refresh at
	// minTokenRefreshInterval, so the first AUTH lands after that floor
	// elapses. Subsequent refreshes are scheduled far in the future so the
	// goroutine goes dormant once the assertions are done.
	tests := []struct {
		name         string
		username     string
		expectedAuth []interface{}
	}{
		{
			// No username: the connection must re-AUTH with the 1-argument form.
			name:         "default user uses 1-argument AUTH",
			username:     "",
			expectedAuth: []interface{}{"AUTH", "token-2"},
		},
		{
			// Explicit username: the connection re-AUTHs with the ACL 2-arg form.
			name:         "explicit user uses 2-argument AUTH",
			username:     "alice",
			expectedAuth: []interface{}{"AUTH", "alice", "token-2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger := kitlogger.NewLogger("test")
			fake := &fakeRedisClient{doWriteCalls: make(chan []interface{}, 10)}
			firstExpiry := time.Now().Add(500 * time.Millisecond)

			var fetchCount atomic.Int32
			fetch := func(ctx context.Context) (string, time.Time, error) {
				n := fetchCount.Add(1)
				if n == 1 {
					// Transient failure: exercises the retry path
					return "", time.Time{}, errors.New("transient token error")
				}
				return fmt.Sprintf("token-%d", n), time.Now().Add(24 * time.Hour), nil
			}

			go runTokenRefreshLoop(fake, tc.username, firstExpiry, &logger, "test", fetch)

			select {
			case call := <-fake.doWriteCalls:
				require.Equal(t, tc.expectedAuth, call)
			case <-time.After(minTokenRefreshInterval + 10*time.Second):
				t.Fatal("timed out waiting for AUTH after token refresh")
			}
			require.Equal(t, int32(2), fetchCount.Load(), "expected one failed and one successful fetch")
		})
	}
}

func TestNextTokenRefreshInterval(t *testing.T) {
	tests := []struct {
		name     string
		lifetime time.Duration
		expected time.Duration
	}{
		{
			// Long-lived tokens refresh exactly maxRefreshGracePeriod early.
			name:     "long lived token refreshes a full grace period early",
			lifetime: time.Hour,
			expected: time.Hour - maxRefreshGracePeriod,
		},
		{
			name:     "24h token refreshes a full grace period early",
			lifetime: 24 * time.Hour,
			expected: 24*time.Hour - maxRefreshGracePeriod,
		},
		{
			// Boundary: the midpoint and the full grace period coincide.
			name:     "lifetime of exactly twice the grace period refreshes at its midpoint",
			lifetime: 2 * maxRefreshGracePeriod,
			expected: maxRefreshGracePeriod,
		},
		{
			// Regression: a token whose lifetime equals the grace period used to
			// collapse to minTokenRefreshInterval and refresh every 5 seconds
			// forever. It must refresh at its midpoint instead.
			name:     "lifetime equal to the grace period refreshes at its midpoint",
			lifetime: maxRefreshGracePeriod,
			expected: maxRefreshGracePeriod / 2,
		},
		{
			name:     "lifetime shorter than the grace period refreshes at its midpoint",
			lifetime: time.Minute,
			expected: 30 * time.Second,
		},
		{
			// Boundary: the midpoint sits just above the floor.
			name:     "midpoint just above the floor is used",
			lifetime: 2*minTokenRefreshInterval + 2*time.Second,
			expected: minTokenRefreshInterval + time.Second,
		},
		{
			// Boundary: the midpoint lands exactly on the floor, which is not
			// strictly greater than it, so the floor applies.
			name:     "midpoint equal to the floor falls back to the floor",
			lifetime: 2 * minTokenRefreshInterval,
			expected: minTokenRefreshInterval,
		},
		{
			name:     "very short lifetime is floored",
			lifetime: time.Second,
			expected: minTokenRefreshInterval,
		},
		{
			name:     "zero lifetime is floored",
			lifetime: 0,
			expected: minTokenRefreshInterval,
		},
		{
			// An already-expired token must not produce a non-positive wait, or
			// time.After would fire immediately and spin the loop.
			name:     "already expired token is floored",
			lifetime: -time.Hour,
			expected: minTokenRefreshInterval,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, nextTokenRefreshInterval(tc.lifetime))
		})
	}
}

// TestNextTokenRefreshIntervalInvariants asserts the two properties the refresh
// loop depends on across a wide range of token lifetimes: the wait is never
// short enough to spin, and a token that lives longer than the floor is always
// refreshed strictly before it expires.
func TestNextTokenRefreshIntervalInvariants(t *testing.T) {
	lifetimes := []time.Duration{
		-time.Hour, -time.Second, 0, time.Millisecond, time.Second,
		5 * time.Second, 10 * time.Second, 11 * time.Second, 30 * time.Second,
		time.Minute, 5 * time.Minute, 10 * time.Minute, 15 * time.Minute,
		time.Hour, 8 * time.Hour, 24 * time.Hour, 90 * 24 * time.Hour,
	}

	for _, lifetime := range lifetimes {
		t.Run(lifetime.String(), func(t *testing.T) {
			interval := nextTokenRefreshInterval(lifetime)

			require.GreaterOrEqual(t, interval, minTokenRefreshInterval,
				"refresh interval must never drop below the floor, or the loop spins against the IdP")

			if lifetime > minTokenRefreshInterval {
				require.Less(t, interval, lifetime,
					"a token must be refreshed strictly before it expires")
			}
		})
	}
}
