//go:build integration_test
// +build integration_test

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
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

// These tests need a Redis server of their own, e.g. `docker run -d -p 6379:6379 redis:7`:
//
//	go test -tags integration_test -run TestIntegration ./pubsub/redis/
//
// They identify the component's connections in CLIENT LIST by database number, so nothing
// else should use database integrationDB on that server.
const integrationDB = 9

func testRedisHost() string {
	if h := os.Getenv("REDIS_HOST"); h != "" {
		return h
	}
	return "localhost:6379"
}

type readerConn struct {
	id      string
	blocked bool
}

// xreadgroupConns lists the connections in integrationDB whose last command was XREADGROUP.
func xreadgroupConns(t *testing.T, admin *redis.Client) []readerConn {
	t.Helper()

	list, err := admin.ClientList(t.Context()).Result()
	require.NoError(t, err)

	var conns []readerConn
	for _, line := range strings.Split(list, "\n") {
		fields := map[string]string{}
		for _, f := range strings.Fields(line) {
			if k, v, ok := strings.Cut(f, "="); ok {
				fields[k] = v
			}
		}
		if fields["db"] != strconv.Itoa(integrationDB) || fields["cmd"] != "xreadgroup" {
			continue
		}
		conns = append(conns, readerConn{id: fields["id"], blocked: strings.Contains(fields["flags"], "b")})
	}

	return conns
}

func blockedReaders(t *testing.T, admin *redis.Client) int {
	n := 0
	for _, c := range xreadgroupConns(t, admin) {
		if c.blocked {
			n++
		}
	}
	return n
}

func setupIntegration(t *testing.T, props map[string]string) (*redisStreams, *redis.Client) {
	t.Helper()

	admin := redis.NewClient(&redis.Options{Addr: testRedisHost(), DB: integrationDB})
	require.NoError(t, admin.Ping(t.Context()).Err(), "Redis must be running at %s", testRedisHost())
	require.NoError(t, admin.FlushDB(t.Context()).Err())
	t.Cleanup(func() {
		admin.FlushDB(context.Background())
		admin.Close()
	})

	base := map[string]string{
		"redisHost": testRedisHost(),
		"redisDB":   strconv.Itoa(integrationDB),
		consumerID:  "integration",
		concurrency: "1",
	}
	for k, v := range props {
		base[k] = v
	}

	r := NewRedisStreams(logger.NewLogger("test")).(*redisStreams)
	require.NoError(t, r.Init(t.Context(), pubsub.Metadata{Base: mdata.Base{Properties: base}}))
	t.Cleanup(func() { r.Close() })

	return r, admin
}

// TestIntegrationResubscribeReleasesBlockedReads cancels and restarts a subscription on a
// stream that receives no messages, as the runtime does when app health flaps. Each cancelled
// read has to give its pooled connection back, otherwise the pool fills up and Subscribe fails
// with "redis: connection pool timeout".
func TestIntegrationResubscribeReleasesBlockedReads(t *testing.T) {
	r, admin := setupIntegration(t, map[string]string{
		"poolSize":    "2",
		"poolTimeout": "10s",
	})

	const topic = "quiet"
	noop := func(context.Context, *pubsub.NewMessage) error { return nil }
	for i := range 4 {
		ctx, cancel := context.WithCancel(t.Context())
		require.NoError(t, r.Subscribe(ctx, pubsub.SubscribeRequest{Topic: topic}, noop), "subscribe %d", i)
		require.Eventually(t, func() bool {
			return blockedReaders(t, admin) > 0
		}, 5*time.Second, 20*time.Millisecond, "read %d did not block on the server", i)
		cancel()
	}

	// Every cancelled read returns and nothing is left blocked on the server.
	require.Eventually(t, func() bool {
		return blockedReaders(t, admin) == 0
	}, 2*defaultPollBlock, 100*time.Millisecond, "cancelled reads are still blocked on the server")

	// A new subscription still gets a connection and receives messages.
	received := make(chan struct{}, 1)
	require.NoError(t, r.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: topic}, func(context.Context, *pubsub.NewMessage) error {
		select {
		case received <- struct{}{}:
		default:
		}
		return nil
	}))
	require.NoError(t, r.Publish(t.Context(), &pubsub.PublishRequest{Topic: topic, Data: []byte(`"hello"`)}))
	select {
	case <-received:
	case <-time.After(10 * time.Second):
		t.Fatal("message was not delivered after resubscribing")
	}
	require.Eventually(t, func() bool {
		pending, err := admin.XPending(t.Context(), topic, "integration").Result()
		return err == nil && pending.Count == 0
	}, 5*time.Second, 50*time.Millisecond, "message was not acknowledged")
}

// TestIntegrationIdleReadDoesNotReconnect checks that a reader on a quiet stream with a
// readTimeout keeps its connection: the read returns empty when BLOCK elapses instead of
// failing with an i/o timeout that makes go-redis drop and redial the connection.
func TestIntegrationIdleReadDoesNotReconnect(t *testing.T) {
	r, admin := setupIntegration(t, map[string]string{
		"readTimeout": "500ms",
	})

	require.NoError(t, r.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "quiet"}, func(context.Context, *pubsub.NewMessage) error { return nil }))

	var first []readerConn
	require.Eventually(t, func() bool {
		first = xreadgroupConns(t, admin)
		return len(first) == 1
	}, 5*time.Second, 20*time.Millisecond)

	// Several BLOCK periods on an idle stream.
	time.Sleep(3 * time.Second)

	conns := xreadgroupConns(t, admin)
	require.Len(t, conns, 1)
	assert.Equal(t, first[0].id, conns[0].id, "the reader reconnected while idle")
}
