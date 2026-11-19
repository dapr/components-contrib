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

package redis_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/go-redis/redis/v8"
	"github.com/dapr/go-sdk/service/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pubsub_redis "github.com/dapr/components-contrib/pubsub/redis"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	"github.com/dapr/dapr/pkg/config/protocol"
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/kit/logger"
)

const (
	sidecarName       = "dapr-1"
	appID             = "app-1"
	clusterName       = "redispubsubcertification"
	dockerComposeYAML = "docker-compose.yml"
	pubsubName        = "messagebus"
	readTimeoutPubsub = "messagebus-readtimeout"
	idleTopic         = "idle"
	redisAddr         = "localhost:6399"
	appPort           = 8000
	healthPath        = "/healthz"
	flaps             = 4
)

// Topics that receive no messages until the end of the test.
var quietTopics = []string{"quiet1", "quiet2"}

// TestRedisResubscribeOnQuietTopics flaps app health, which makes the runtime stop and
// restart the app's subscriptions. A cancelled subscription has to give its pooled
// connection back even if its topic receives no messages; otherwise each flap leaves a
// read blocked on the server, the pool fills up and subscribing fails with
// "redis: connection pool timeout".
func TestRedisResubscribeOnQuietTopics(t *testing.T) {
	log := logger.NewLogger("dapr.components")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() { rdb.Close() })

	messages := watcher.NewUnordered()

	var (
		unhealthy  atomic.Bool
		okProbes   atomic.Int64
		failProbes atomic.Int64
	)

	application := func(ctx flow.Context, s common.Service) error {
		err := s.AddHealthCheckHandler(healthPath, func(context.Context) error {
			if unhealthy.Load() {
				failProbes.Add(1)
				return errors.New("unhealthy")
			}
			okProbes.Add(1)
			return nil
		})
		if err != nil {
			return err
		}

		for _, topic := range quietTopics {
			err = s.AddTopicEventHandler(&common.Subscription{
				PubsubName: pubsubName,
				Topic:      topic,
				Route:      "/" + topic,
			}, func(_ context.Context, e *common.TopicEvent) (bool, error) {
				messages.Observe(e.Data)
				return false, nil
			})
			if err != nil {
				return err
			}
		}

		return s.AddTopicEventHandler(&common.Subscription{
			PubsubName: readTimeoutPubsub,
			Topic:      idleTopic,
			Route:      "/" + idleTopic,
		}, func(context.Context, *common.TopicEvent) (bool, error) {
			return false, nil
		})
	}

	// xreadgroupConns lists the connections to database db whose last command was XREADGROUP.
	xreadgroupConns := func(ctx context.Context, db string) ([]map[string]string, error) {
		list, err := rdb.ClientList(ctx).Result()
		if err != nil {
			return nil, err
		}
		var conns []map[string]string
		for _, line := range strings.Split(list, "\n") {
			fields := map[string]string{}
			for _, f := range strings.Fields(line) {
				if k, v, ok := strings.Cut(f, "="); ok {
					fields[k] = v
				}
			}
			if fields["db"] == db && fields["cmd"] == "xreadgroup" {
				conns = append(conns, fields)
			}
		}
		return conns, nil
	}

	// blockedReaders counts the messagebus connections blocked in XREADGROUP on the server.
	blockedReaders := func(ctx context.Context) (int, error) {
		conns, err := xreadgroupConns(ctx, "0")
		if err != nil {
			return 0, err
		}
		n := 0
		for _, c := range conns {
			if strings.Contains(c["flags"], "b") {
				n++
			}
		}
		return n, nil
	}

	expectBlockedReaders := func(want int, timeout time.Duration) flow.Runnable {
		return func(ctx flow.Context) error {
			assert.EventuallyWithT(ctx, func(c *assert.CollectT) {
				n, err := blockedReaders(ctx)
				require.NoError(c, err)
				assert.Equal(c, want, n, "XREADGROUP connections blocked on the server")
			}, timeout, 100*time.Millisecond)
			return nil
		}
	}

	waitForProbe := func(ctx flow.Context, counter *atomic.Int64) {
		before := counter.Load()
		require.Eventually(ctx, func() bool {
			return counter.Load() > before
		}, 10*time.Second, 50*time.Millisecond, "no health probe received")
	}

	// With readTimeout set, an idle read has to return empty when BLOCK elapses. If it fails
	// with an i/o timeout instead, go-redis drops the connection and the reader redials.
	idleReaderKeepsConnection := func(ctx flow.Context) error {
		var first []map[string]string
		require.EventuallyWithT(ctx, func(c *assert.CollectT) {
			var err error
			first, err = xreadgroupConns(ctx, "1")
			require.NoError(c, err)
			assert.Len(c, first, 1)
		}, 10*time.Second, 50*time.Millisecond, "reader on %s did not start", idleTopic)

		// Several BLOCK periods on an idle stream.
		time.Sleep(3 * time.Second)

		conns, err := xreadgroupConns(ctx, "1")
		require.NoError(ctx, err)
		require.Len(ctx, conns, 1)
		assert.Equal(ctx, first[0]["id"], conns[0]["id"], "the idle reader reconnected")
		return nil
	}

	flapAppHealth := func(ctx flow.Context) error {
		for i := range flaps {
			log.Infof("App health flap %d", i+1)

			// The runtime stops the app's subscriptions when the app becomes unhealthy...
			unhealthy.Store(true)
			waitForProbe(ctx, &failProbes)
			time.Sleep(2 * time.Second)

			// ...and starts them again when it is healthy.
			unhealthy.Store(false)
			waitForProbe(ctx, &okProbes)
			time.Sleep(2 * time.Second)
		}
		return nil
	}

	publishAndReceive := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName)
		for _, topic := range quietTopics {
			msg := "message on " + topic
			messages.ExpectStrings(msg)
			require.NoError(ctx, client.PublishEvent(ctx, pubsubName, topic, msg), "publishing to %s", topic)
		}
		messages.Assert(ctx, 30*time.Second)
		return nil
	}

	flow.New(t, "redis pubsub resubscribe on quiet topics").
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for redis readiness", retry.Do(time.Second, 30, func(ctx flow.Context) error {
			return rdb.Ping(ctx).Err()
		})).
		Step(app.Run(appID, fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run(sidecarName,
			append(componentRuntimeOptions(),
				embedded.WithResourcesPath("./components/default"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
				embedded.WithGracefulShutdownDuration(2*time.Second),
				withAppHealthCheck(),
			)...,
		)).
		Step("wait for subscriptions", expectBlockedReaders(len(quietTopics), 20*time.Second)).
		Step("idle reader keeps its connection", idleReaderKeepsConnection).
		Step("flap app health", flapAppHealth).
		Step("no reads left behind", expectBlockedReaders(len(quietTopics), 10*time.Second)).
		Step("publish and receive", publishAndReceive).
		Run()
}

func withAppHealthCheck() embedded.Option {
	return func(c *runtime.Config) {
		c.EnableAppHealthCheck = true
		c.AppHealthCheckPath = healthPath
		c.AppHealthProbeInterval = 1   // seconds
		c.AppHealthProbeTimeout = 500 // milliseconds
		c.AppHealthThreshold = 1
	}
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	pubsubRegistry := pubsub_loader.NewRegistry()
	pubsubRegistry.Logger = log
	pubsubRegistry.RegisterComponent(pubsub_redis.NewRedisStreams, "redis")

	return []embedded.Option{
		embedded.WithPubSubs(pubsubRegistry),
	}
}
