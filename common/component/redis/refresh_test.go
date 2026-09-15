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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/stretchr/testify/require"

	kitlogger "github.com/dapr/kit/logger"
)

// fakeRedisClient records the AUTH command issued via DoWrite. The embedded
// RedisClient interface satisfies the remaining methods (they panic if called,
// which no test does).
type fakeRedisClient struct {
	RedisClient
	doWriteCalls chan []interface{}
	authACLCalls chan []string
}

func (f *fakeRedisClient) DoWrite(_ context.Context, args ...interface{}) error {
	f.doWriteCalls <- args
	return nil
}

// AuthACL records the AUTH command issued by the EntraID refresh loop, which
// re-authenticates through the pipelined ACL form instead of DoWrite.
func (f *fakeRedisClient) AuthACL(_ context.Context, username, password string) error {
	f.authACLCalls <- []string{username, password}
	return nil
}

func (f *fakeRedisClient) Close() error {
	return nil
}

// fakeTokenCredential issues a new token on every call, each with the same
// lifetime.
type fakeTokenCredential struct {
	lifetime time.Duration
	calls    atomic.Int32
}

func (f *fakeTokenCredential) GetToken(_ context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
	n := f.calls.Add(1)
	return azcore.AccessToken{
		Token:     fmt.Sprintf("entraid-token-%d", n),
		ExpiresOn: time.Now().Add(f.lifetime),
	}, nil
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

// TestStartEntraIDTokenRefreshBackgroundRoutine covers the EntraID refresh
// schedule. The loop previously subtracted a fixed 5 minute grace period from
// the expiry with no floor under the result, so any token issued with a
// lifetime at or under that period produced a non-positive wait. time.After
// then fired immediately on every iteration and the loop refreshed without
// pause.
func TestStartEntraIDTokenRefreshBackgroundRoutine(t *testing.T) {
	// Each subtest starts a goroutine that outlives it. The credential hands
	// back a long-lived token, so the loop reschedules hours out and stays
	// dormant once the assertions are done.
	const refreshedTokenLifetime = 24 * time.Hour

	t.Run("token shorter than the floor waits for the floor", func(t *testing.T) {
		logger := kitlogger.NewLogger("test")
		fake := &fakeRedisClient{authACLCalls: make(chan []string, 10)}
		cred := &fakeTokenCredential{lifetime: refreshedTokenLifetime}
		var tokenCredential azcore.TokenCredential = cred

		// A 500ms lifetime is below twice the floor, so the wait is the floor.
		// Before the fix this was time.Until(expiry-5m), a negative duration.
		start := time.Now()
		StartEntraIDTokenRefreshBackgroundRoutine(fake, "alice", time.Now().Add(500*time.Millisecond), &tokenCredential, &logger)

		select {
		case call := <-fake.authACLCalls:
			require.GreaterOrEqual(t, time.Since(start), minTokenRefreshInterval,
				"the first refresh must not fire before the floor elapses")
			require.Equal(t, []string{"alice", "entraid-token-1"}, call)
		case <-time.After(minTokenRefreshInterval + 10*time.Second):
			t.Fatal("timed out waiting for AUTH after token refresh")
		}
	})

	t.Run("token shorter than the grace period refreshes at its midpoint", func(t *testing.T) {
		logger := kitlogger.NewLogger("test")
		fake := &fakeRedisClient{authACLCalls: make(chan []string, 10)}
		cred := &fakeTokenCredential{lifetime: refreshedTokenLifetime}
		var tokenCredential azcore.TokenCredential = cred

		// A 20s lifetime is shorter than the 5 minute grace period, so the
		// refresh must land on its 10s midpoint: later than the floor, and far
		// later than the immediate refresh the fixed grace period produced.
		StartEntraIDTokenRefreshBackgroundRoutine(fake, "alice", time.Now().Add(20*time.Second), &tokenCredential, &logger)

		select {
		case call := <-fake.authACLCalls:
			t.Fatalf("token refreshed after less than the midpoint: %v", call)
		case <-time.After(minTokenRefreshInterval + time.Second):
		}
		require.Equal(t, int32(0), cred.calls.Load(), "no token must be requested before the midpoint")
	})
}
