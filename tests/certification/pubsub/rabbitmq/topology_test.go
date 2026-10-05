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

package rabbitmq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dapr/go-sdk/service/common"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/dapr/pkg/config/protocol"
	"github.com/dapr/dapr/pkg/runtime"
	daprClient "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
)

const (
	// Externally managed topology ("Topology Operator owns everything").
	sidecarNamePassive = "dapr-passive"
	appIDPassive       = "app-passive"
	pubsubPassive      = "mq-passive"
	topicPassive       = "passive-topic"
	// Named by the external owner, not derived from consumerID.
	queuePassive = "operator-owned-queue"

	// Consistent hash partitioning.
	sidecarNameHash0 = "dapr-hash-0"
	sidecarNameHash1 = "dapr-hash-1"
	appIDHash0       = "app-hash-0"
	appIDHash1       = "app-hash-1"
	pubsubHash       = "mq-hash"
	topicHash        = "hash-topic"

	rabbitMQMgmtURL = "http://localhost:15672"

	// Bucket weight each partition queue is bound with. The weight is the
	// number of buckets the queue occupies on the hash ring, so a larger
	// weight spreads the key space more evenly across a small number of
	// queues; binding every queue with weight 1 gives a workable but coarse
	// split.
	hashBucketWeight = "10"
)

// topologySnapshot is the set of exchange and queue names present on the
// broker, used to prove that a passive component created nothing.
type topologySnapshot struct {
	exchanges []string
	queues    []string
}

func fetchTopologySnapshot(t require.TestingT) topologySnapshot {
	get := func(path string) []string {
		req, err := http.NewRequest(http.MethodGet, rabbitMQMgmtURL+path, nil)
		require.NoError(t, err)
		req.SetBasicAuth("test", "test")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var items []struct {
			Name string `json:"name"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&items))

		names := make([]string, 0, len(items))
		for _, item := range items {
			// Skip the default nameless exchange and RabbitMQ's built-ins.
			if item.Name == "" || strings.HasPrefix(item.Name, "amq.") {
				continue
			}
			names = append(names, item.Name)
		}
		sort.Strings(names)

		return names
	}

	return topologySnapshot{exchanges: get("/api/exchanges"), queues: get("/api/queues")}
}

// declareTopologyOutOfBand stands in for the RabbitMQ Cluster Kubernetes
// Topology Operator: it creates the exchange, the queue and the binding before
// any Dapr sidecar starts.
func declareTopologyOutOfBand(exchange, exchangeKind, queue, bindingKey string) flow.Runnable {
	return func(ctx flow.Context) error {
		conn, err := amqp.Dial(rabbitMQURL)
		if err != nil {
			return err
		}
		defer conn.Close()

		ch, err := conn.Channel()
		if err != nil {
			return err
		}
		defer ch.Close()

		// Durable and not auto-deleted, which is how an operator-managed
		// exchange is normally defined and differs from what the component
		// would declare by default.
		if err = ch.ExchangeDeclare(exchange, exchangeKind, true, false, false, false, nil); err != nil {
			return err
		}

		if queue == "" {
			return nil
		}

		if _, err = ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
			return err
		}

		return ch.QueueBind(queue, bindingKey, exchange, false, nil)
	}
}

// TestRabbitMQPassiveTopology covers a topology owned end to end by an external
// manager: the component must bind to the existing exchange, queue and binding,
// and must not create any topology object of its own.
func TestRabbitMQPassiveTopology(t *testing.T) {
	log := logger.NewLogger("dapr.components")

	const numPassiveMessages = 100

	var (
		mu       sync.Mutex
		received []string
	)

	application := func(ctx flow.Context, s common.Service) error {
		return s.AddTopicEventHandler(&common.Subscription{
			PubsubName: pubsubPassive,
			Topic:      topicPassive,
			Route:      "/passive",
			Metadata: map[string]string{
				// The queue the external owner created for us.
				"queueName": queuePassive,
			},
		}, func(_ context.Context, e *common.TopicEvent) (bool, error) {
			mu.Lock()
			received = append(received, fmt.Sprintf("%v", e.Data))
			mu.Unlock()

			return false, nil
		})
	}

	var before topologySnapshot

	flow.New(t, "rabbitmq passive topology certification").
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for rabbitmq readiness",
			retry.Do(time.Second, 30, amqpReady(rabbitMQURL))).
		Step("declare topology out-of-band",
			declareTopologyOutOfBand(topicPassive, "topic", queuePassive, "orders.#")).
		Step("snapshot topology", func(ctx flow.Context) error {
			before = fetchTopologySnapshot(ctx.T)
			require.Contains(ctx.T, before.exchanges, topicPassive)
			require.Contains(ctx.T, before.queues, queuePassive)

			return nil
		}).
		Step(app.Run(appIDPassive, fmt.Sprintf(":%d", appPort+40), application)).
		Step(sidecar.Run(sidecarNamePassive,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/passive"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+40)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+30)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+20)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+20)),
				embedded.WithGracefulShutdownDuration(2*time.Second),
			)...,
		)).
		Step("wait for subscription setup", flow.Sleep(5*time.Second)).
		Step("publish and verify", func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarNamePassive)

			expected := make([]string, numPassiveMessages)
			for i := range expected {
				expected[i] = fmt.Sprintf("passive-%03d", i)
				err := client.PublishEvent(ctx, pubsubPassive, topicPassive, expected[i],
					daprClient.PublishEventWithMetadata(map[string]string{"routingKey": "orders.created"}))
				require.NoError(ctx, err, "error publishing message")
			}

			assert.Eventually(ctx.T, func() bool {
				mu.Lock()
				defer mu.Unlock()

				return len(received) >= numPassiveMessages
			}, 60*time.Second, 100*time.Millisecond, "all messages should be delivered through the externally managed topology")

			mu.Lock()
			got := append([]string(nil), received...)
			mu.Unlock()
			sort.Strings(got)
			assert.Equal(ctx.T, expected, got)

			log.Infof("received %d messages through the operator-managed topology", len(got))

			return nil
		}).
		Step("verify no topology was created by the component", func(ctx flow.Context) error {
			after := fetchTopologySnapshot(ctx.T)

			// The decisive assertion: no queue named after the consumerID, no
			// dead letter objects, no second exchange.
			assert.Equal(ctx.T, before.exchanges, after.exchanges, "the component must not create exchanges in passive mode")
			assert.Equal(ctx.T, before.queues, after.queues, "the component must not create queues in passive mode")

			return nil
		}).
		Run()
}

// TestRabbitMQConsistentHashPartitioning covers partitioned ordering with
// scale-out: an x-consistent-hash exchange owned by an external manager, two
// independently scaled consumers, and a per-message routing key.
//
// It asserts the three properties that make this an alternative to
// concurrency=single: every key is handled by exactly one consumer, the key
// space is spread across all of them, and per-key order is preserved.
func TestRabbitMQConsistentHashPartitioning(t *testing.T) {
	log := logger.NewLogger("dapr.components")

	const (
		numKeys    = 24
		msgsPerKey = 10
		totalMsgs  = numKeys * msgsPerKey
	)

	// partition name -> ordered list of "<key>#<seq>" as observed.
	var (
		mu       sync.Mutex
		observed = map[string][]string{}
	)

	application := func(partition string) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			return s.AddTopicEventHandler(&common.Subscription{
				PubsubName: pubsubHash,
				Topic:      topicHash,
				Route:      "/hash",
				Metadata: map[string]string{
					// For a consistent hash exchange the binding key is the
					// bucket weight of the bound queue.
					"routingKey": hashBucketWeight,
				},
			}, func(_ context.Context, e *common.TopicEvent) (bool, error) {
				mu.Lock()
				observed[partition] = append(observed[partition], fmt.Sprintf("%v", e.Data))
				mu.Unlock()

				return false, nil
			})
		}
	}

	flow.New(t, "rabbitmq consistent hash partitioning certification").
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for rabbitmq readiness",
			retry.Do(time.Second, 30, amqpReady(rabbitMQURL))).
		Step("declare consistent hash exchange out-of-band",
			declareTopologyOutOfBand(topicHash, "x-consistent-hash", "", "")).
		Step(app.Run(appIDHash0, fmt.Sprintf(":%d", appPort+41), application("partition-0"))).
		Step(sidecar.Run(sidecarNameHash0,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/hash0"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+41)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+40)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+21)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+21)),
				embedded.WithGracefulShutdownDuration(2*time.Second),
			)...,
		)).
		Step(app.Run(appIDHash1, fmt.Sprintf(":%d", appPort+42), application("partition-1"))).
		Step(sidecar.Run(sidecarNameHash1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/hash1"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+42)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+50)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+22)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+22)),
				embedded.WithGracefulShutdownDuration(2*time.Second),
			)...,
		)).
		Step("wait for subscription setup", flow.Sleep(5*time.Second)).
		Step("publish partitioned messages", func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarNameHash0)

			// The customer's partition key is a composite of tenant and device
			// serial; here it is modelled the same way.
			for k := range numKeys {
				key := fmt.Sprintf("tenant-%d/serial-%d", k%3, k)
				for seq := range msgsPerKey {
					err := client.PublishEvent(ctx, pubsubHash, topicHash,
						fmt.Sprintf("%s#%d", key, seq),
						daprClient.PublishEventWithMetadata(map[string]string{"routingKey": key}))
					require.NoError(ctx, err, "error publishing message")
				}
			}

			assert.Eventually(ctx.T, func() bool {
				mu.Lock()
				defer mu.Unlock()

				return len(observed["partition-0"])+len(observed["partition-1"]) >= totalMsgs
			}, 90*time.Second, 200*time.Millisecond, "all partitioned messages should be delivered")

			return nil
		}).
		Step("verify partitioning and per-key ordering", func(ctx flow.Context) error {
			mu.Lock()
			snapshot := map[string][]string{}
			for partition, msgs := range observed {
				snapshot[partition] = append([]string(nil), msgs...)
			}
			mu.Unlock()

			total := 0
			keyOwner := map[string]string{}
			lastSeq := map[string]int{}

			for partition, msgs := range snapshot {
				total += len(msgs)
				for _, msg := range msgs {
					parts := strings.Split(msg, "#")
					require.Len(ctx.T, parts, 2)
					key := parts[0]
					seq, err := strconv.Atoi(parts[1])
					require.NoError(ctx.T, err)

					// 1. Stickiness: a key never moves between partitions,
					//    which is what makes per-key ordering possible at all.
					if owner, ok := keyOwner[key]; ok {
						assert.Equalf(ctx.T, owner, partition, "key %s was delivered to more than one partition", key)
					} else {
						keyOwner[key] = partition
					}

					// 2. Ordering: within a key, sequence numbers only advance.
					prev, seen := lastSeq[key]
					if seen {
						assert.Equalf(ctx.T, prev+1, seq, "key %s was processed out of order", key)
					} else {
						assert.Equalf(ctx.T, 0, seq, "key %s did not start at the first message", key)
					}
					lastSeq[key] = seq
				}
			}

			assert.Equal(ctx.T, totalMsgs, total, "every message should be delivered exactly once")
			assert.Len(ctx.T, keyOwner, numKeys)

			// 3. Scale-out: the key space is actually spread, so this is not
			//    equivalent to a single serial consumer.
			assert.Len(ctx.T, snapshot, 2, "both partitions should receive work")
			for partition, msgs := range snapshot {
				assert.NotEmptyf(ctx.T, msgs, "partition %s received no messages", partition)
				log.Infof("partition %s handled %d messages", partition, len(msgs))
			}

			return nil
		}).
		Run()
}
