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
	"github.com/alicebob/miniredis/v2"
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
			// Boundary: the midpoint and the full grace period coincide.
			name:     "lifetime of exactly twice the grace period refreshes at its midpoint",
			lifetime: 2 * maxRefreshGracePeriod,
			expected: maxRefreshGracePeriod,
		},
		{
			// A token whose lifetime equals the grace period must refresh at its
			// midpoint, not collapse to the floor and refresh every 5 seconds.
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
			// Boundary: the midpoint lands exactly on the floor, which is not
			// strictly greater than it, so the floor applies.
			name:     "midpoint equal to the floor falls back to the floor",
			lifetime: 2 * minTokenRefreshInterval,
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

// fakeTokenCredential hands out a token with a fixed lifetime and counts calls.
type fakeTokenCredential struct {
	calls  atomic.Int32
	expiry time.Time
}

func (f *fakeTokenCredential) GetToken(_ context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
	n := f.calls.Add(1)
	return azcore.AccessToken{
		Token:     fmt.Sprintf("entra-token-%d", n),
		ExpiresOn: f.expiry,
	}, nil
}

// TestEntraIDTokenRefresh covers the Entra ID loop's scheduling: a token too
// short-lived to refresh early is floored rather than producing a non-positive
// delay, and the refreshed token reschedules far out so the goroutine goes
// dormant instead of spinning.
func TestEntraIDTokenRefresh(t *testing.T) {
	logger := kitlogger.NewLogger("test")
	fake := &fakeRedisClient{doWriteCalls: make(chan []interface{}, 10)}

	cred := &fakeTokenCredential{expiry: time.Now().Add(24 * time.Hour)}

	// Already expired: a fixed grace period would make this negative and fire
	// time.After immediately, so the floor is what keeps the loop off the IdP.
	start := time.Now()
	go runTokenRefreshLoop(fake, "user", time.Now().Add(-time.Hour), &logger, "entraID", entraTokenFetch(cred))

	select {
	case call := <-fake.doWriteCalls:
		require.Equal(t, []interface{}{"AUTH", "user", "entra-token-1"}, call)
		require.GreaterOrEqual(t, time.Since(start), minTokenRefreshInterval,
			"expired token must not refresh immediately, or the loop spins against the IdP")
	case <-time.After(minTokenRefreshInterval + 10*time.Second):
		t.Fatal("timed out waiting for AUTH after entraID token refresh")
	}

	// The 24h token reschedules a full grace period out, so no second refresh.
	select {
	case call := <-fake.doWriteCalls:
		t.Fatalf("unexpected second refresh scheduled: %v", call)
	case <-time.After(2 * time.Second):
	}
	require.Equal(t, int32(1), cred.calls.Load())
}

// rotatingCredential hands out the token stored last, standing in for an Azure
// credential whose cached token has been refreshed.
type rotatingCredential struct{ token atomic.Value }

func (r *rotatingCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: r.token.Load().(string), ExpiresOn: time.Now().Add(time.Hour)}, nil
}

// TestTokenRotation proves that a connection opened after the token rotated
// authenticates with the new token, on both go-redis clients
// (dapr/components-contrib#3554), and that a password snapshot does not.
func TestTokenRotation(t *testing.T) {
	const tokenA, tokenB = "token-a", "token-b"
	clients := []struct {
		name string
		new  func(*Settings) (RedisClient, error)
	}{{"v8", newV8Client}, {"v9", newV9Client}}
	tests := []struct {
		name     string
		username string
		snapshot bool
	}{
		{name: "ACL user authenticates new connections with the fresh token", username: "entra-oid"},
		{name: "default user authenticates new connections with the fresh token"},
		{name: "password snapshot fails once the token rotates", username: "entra-oid", snapshot: true},
	}

	for _, c := range clients {
		for _, tc := range tests {
			t.Run(c.name+"/"+tc.name, func(t *testing.T) {
				aclUser := tc.username
				if aclUser == "" {
					aclUser = "default"
				}
				srv := miniredis.RunT(t)
				srv.RequireUserAuth(aclUser, tokenA)

				cred := &rotatingCredential{}
				cred.token.Store(tokenA)
				s := &Settings{Host: srv.Addr(), DB: 1, Username: tc.username, MaxConnAge: Duration(time.Millisecond)}
				if tc.snapshot {
					s.Password = tokenA
				} else {
					s.fetchToken = entraTokenFetch(cred)
				}
				client, err := c.new(s)
				require.NoError(t, err)
				t.Cleanup(func() { client.Close() })

				require.NoError(t, client.DoWrite(t.Context(), "SET", "before", "1"))

				// Revoke token A. The open connection stays authenticated, as on Redis.
				srv.RequireUserAuth(aclUser, tokenB)
				cred.token.Store(tokenB)
				// Outlive MaxConnAge so the next command dials a new connection.
				time.Sleep(10 * time.Millisecond)

				err = client.DoWrite(t.Context(), "SET", "after", "1")
				if tc.snapshot {
					require.ErrorContains(t, err, "WRONGPASS")
					return
				}
				require.NoError(t, err)
				// SELECT ran after AUTH on the new connection.
				got, err := srv.DB(1).Get("after")
				require.NoError(t, err)
				require.Equal(t, "1", got)
			})
		}
	}
}
