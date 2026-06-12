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

type authACLCall struct {
	username string
	password string
}

// fakeRedisClient records AuthACL calls. The embedded RedisClient interface
// satisfies the remaining methods (they panic if called, which no test does).
type fakeRedisClient struct {
	RedisClient
	authCalls chan authACLCall
}

func (f *fakeRedisClient) AuthACL(_ context.Context, username, password string) error {
	f.authCalls <- authACLCall{username: username, password: password}
	return nil
}

func (f *fakeRedisClient) Close() error {
	return nil
}

func TestRunTokenRefreshLoop(t *testing.T) {
	logger := kitlogger.NewLogger("test")
	fake := &fakeRedisClient{authCalls: make(chan authACLCall, 10)}

	// The loop refreshes 5 minutes before expiry; make the first refresh fire
	// almost immediately, and schedule all subsequent refreshes far in the
	// future so the goroutine goes dormant after the assertions.
	const refreshGracePeriod = 5 * time.Minute
	firstExpiry := time.Now().Add(refreshGracePeriod + 50*time.Millisecond)

	var fetchCount atomic.Int32
	fetch := func(ctx context.Context) (string, time.Time, error) {
		n := fetchCount.Add(1)
		if n == 1 {
			// Transient failure: exercises the retry path
			return "", time.Time{}, errors.New("transient token error")
		}
		return fmt.Sprintf("token-%d", n), time.Now().Add(refreshGracePeriod + 24*time.Hour), nil
	}

	go runTokenRefreshLoop(fake, "default", firstExpiry, &logger, "test", fetch)

	select {
	case call := <-fake.authCalls:
		require.Equal(t, "default", call.username)
		require.Equal(t, "token-2", call.password)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for AuthACL after token refresh")
	}
	require.Equal(t, int32(2), fetchCount.Load(), "expected one failed and one successful fetch")
}
