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

package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/dapr/pkg/config/protocol"
	"github.com/dapr/dapr/pkg/runtime"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
)

const (
	appIDExhausted       = "app-exhausted"
	sidecarNameExhausted = "dapr-exhausted"
	poisonTopicName      = "poisonorders"

	// The inbound retry policy in components/consumerExhausted is constant
	// with maxRetries 3, so a permanently failing message is delivered to the
	// app four times in total and then given up on.
	expectedDeliveries = 4

	poisonPayload = "this message can never be processed"
	probePayload  = "probe"
)

// TestKafkaExhaustedRetriesCommitOffset covers dapr/components-contrib#4362.
//
// When an inbound resiliency policy exhausts its retries and no dead letter
// topic is configured, the runtime has permanently given up on the message.
// That decision has to be committed to Kafka. Leaving the offset uncommitted
// parks the partition behind a message nobody will ever accept, and the next
// time the consumer group reconnects (a restart, a rebalance, a node moving)
// Kafka replays it and the whole exhausted cycle runs again.
//
// The test makes that concrete: publish one message the app always rejects,
// watch it exhaust its retry budget, then restart the sidecar and assert the
// app is not handed the same message a second time.
func TestKafkaExhaustedRetriesCommitOffset(t *testing.T) {
	var poisonDeliveries, probeDeliveries atomic.Int64

	application := func(ctx flow.Context, s common.Service) error {
		return s.AddTopicEventHandler(&common.Subscription{
			PubsubName: pubsubName,
			Topic:      poisonTopicName,
			Route:      "/poison",
		}, func(_ context.Context, e *common.TopicEvent) (bool, error) {
			if fmt.Sprintf("%s", e.Data) == probePayload {
				probeDeliveries.Add(1)
				ctx.Logf("======== %s received the probe message", appIDExhausted)
				return false, nil
			}

			n := poisonDeliveries.Add(1)
			ctx.Logf("======== %s received the poison message, delivery %d", appIDExhausted, n)
			// Retriable, and never satisfied. The resiliency policy is what
			// bounds it.
			return true, errors.New(poisonPayload)
		})
	}

	exhaustedAppPort := appPort + portOffset*4
	sidecarOptions := func() []embedded.Option {
		return append(componentRuntimeOptions(),
			embedded.WithResourcesPath("./components/consumerExhausted"),
			embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(exhaustedAppPort)),
			embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset*4)),
			embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset*4)),
			embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset*4)),
		)
	}

	// Both messages carry the same partition key so they land on the same
	// partition. Offsets are committed per partition, so a probe on any other
	// partition would say nothing about the poison message's one.
	publish := func(payload string) flow.Runnable {
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarNameExhausted)
			err := client.PublishEvent(ctx, pubsubName, poisonTopicName, payload,
				dapr.PublishEventWithMetadata(map[string]string{messageKey: "exhausted"}))
			require.NoError(ctx, err, "error publishing message")
			return nil
		}
	}

	// waitForExhaustion waits for the retry policy to run out, then confirms
	// that nothing keeps redelivering the message afterwards.
	waitForExhaustion := func(ctx flow.Context) error {
		require.Eventually(ctx, func() bool {
			return poisonDeliveries.Load() >= expectedDeliveries
		}, time.Minute, 100*time.Millisecond,
			"the poison message should be delivered once plus its configured retries")

		time.Sleep(5 * time.Second)

		assert.Equal(ctx, int64(expectedDeliveries), poisonDeliveries.Load(),
			"delivery should stop once the retry policy is exhausted")
		return nil
	}

	// assertNotRedelivered is the regression assertion.
	//
	// The probe is published only after the restart, and deliberately so: a
	// message consumed before it would mark a higher offset and commit past
	// the poison message, hiding the bug. Published after, it is the barrier
	// that makes this assertion mean something. It sits at a higher offset on
	// the same partition, so if the poison message were replayed it would
	// arrive first. Once the probe lands, a replay can no longer be pending,
	// and a poison count that has not moved proves its offset was committed.
	//
	// Waiting on the probe rather than on a fixed sleep is also what stops the
	// assertion passing for the wrong reason: if the restarted consumer never
	// rejoins the group, the probe never arrives and the test fails loudly
	// instead of silently observing no redelivery.
	assertNotRedelivered := func(ctx flow.Context) error {
		require.Eventually(ctx, func() bool {
			return probeDeliveries.Load() >= 1
		}, time.Minute, 100*time.Millisecond,
			"the restarted consumer should receive the probe message; without it this test proves nothing")

		// Compared against the pre-restart total, not a snapshot taken here: a
		// redelivery can land while the consumer is still rejoining, and a
		// baseline read after the restart would quietly absorb it.
		assert.Equal(ctx, int64(expectedDeliveries), poisonDeliveries.Load(),
			"a message whose retry policy was exhausted must not be redelivered after the consumer reconnects; its offset should have been committed")
		return nil
	}

	flow.New(t, "kafka exhausted retries commit offset").
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for broker sockets",
			network.WaitForAddresses(5*time.Minute, brokers...)).
		Step("wait", flow.Sleep(5*time.Second)).
		Step("wait for kafka readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			config := sarama.NewConfig()
			config.ClientID = "test-consumer-exhausted"
			config.Consumer.Return.Errors = true

			client, err := sarama.NewConsumer(brokers, config)
			if err != nil {
				return err
			}
			defer client.Close()

			_, err = client.ConsumePartition("myTopic", 0, sarama.OffsetOldest)
			return err
		})).
		//
		Step(app.Run(appIDExhausted, fmt.Sprintf(":%d", exhaustedAppPort), application)).
		Step(sidecar.Run(sidecarNameExhausted, sidecarOptions()...)).
		//
		Step("publish a message the app always rejects", publish(poisonPayload)).
		Step("wait for the retry policy to be exhausted", waitForExhaustion).
		//
		// Give sarama's offset manager time to flush the commit, then take the
		// consumer down and bring it back: the same thing a pod restart or a
		// rebalance does to a consumer group member.
		Step("wait for the offset commit to flush", flow.Sleep(5*time.Second)).
		Step("stop sidecar", sidecar.Stop(sidecarNameExhausted)).
		Step("stop app", app.Stop(appIDExhausted)).
		Step("wait", flow.Sleep(5*time.Second)).
		Step(app.Run(appIDExhausted, fmt.Sprintf(":%d", exhaustedAppPort), application)).
		Step(sidecar.Run(sidecarNameExhausted, sidecarOptions()...)).
		//
		Step("publish a probe message behind the poison one", publish(probePayload)).
		Step("assert the poison message is not redelivered", assertNotRedelivered).
		Run()
}
