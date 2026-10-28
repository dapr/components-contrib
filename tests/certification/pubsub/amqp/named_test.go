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

package amqp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	"github.com/dapr/dapr/pkg/config/protocol"
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
)

const (
	replicaSidecar1 = "dapr-replica-1"
	replicaSidecar2 = "dapr-replica-2"
	replicaSidecar3 = "dapr-replica-3"
	replicaApp1     = "replica-1"
	replicaApp2     = "replica-2"
	replicaApp3     = "replica-3"

	// Offset from the ports TestAMQP uses.
	replicaPortOffset = 10

	// Both replicas set this container ID in ./components/named, so they share
	// each named subscription as replicas of one app ID would.
	namedContainerID = "orders-service"
	namedTopic       = "orders"

	brokerURL      = "amqp://localhost:5673"
	brokerUser     = "artemis"
	brokerPassword = "artemis"
	jolokiaURL     = "http://localhost:8163/console/jolokia"
	brokerMBean    = `org.apache.activemq.artemis:broker="0.0.0.0"`
)

// namedSubscription is one pub/sub component in ./components/named with one
// subscription on namedTopic. The runtime keeps one declarative or programmatic
// subscription per component and topic, so each subscription on the same topic
// needs its own component.
type namedSubscription struct {
	pubsub    string
	selector  string
	eventType string
}

var namedSubscriptions = []namedSubscription{
	{pubsub: "billing", selector: "eventType = 'invoice'", eventType: "invoice"},
	{pubsub: "audit", selector: "eventType = 'login'", eventType: "login"},
}

// unmatchedEventType is published too. No subscription selects it, so it must
// reach no app.
const unmatchedEventType = "refund"

// changedSelector is the selector a third replica brings to the billing
// subscription, as a rolling update that changes the selector would.
const changedSelector = "eventType IN ('invoice', 'refund')"

// TestAMQPNamedSubscriptions covers durable, shared, named and filtered
// subscriptions against ActiveMQ Artemis: two replicas of one app, two named
// subscriptions with their own selector on one multicast address.
func TestAMQPNamedSubscriptions(t *testing.T) {
	logger.ApplyOptionsToLoggers(&logger.Options{OutputLevel: "debug"})

	tallies := map[string]*tally{}
	for _, sub := range namedSubscriptions {
		tallies[sub.pubsub] = newTally()
	}

	replica := func(ctx flow.Context, s common.Service) error {
		var errs error
		for _, sub := range namedSubscriptions {
			received := tallies[sub.pubsub]
			errs = multierr.Append(errs, s.AddTopicEventHandler(&common.Subscription{
				PubsubName: sub.pubsub,
				Topic:      namedTopic,
				Route:      "/" + sub.pubsub,
				Metadata: map[string]string{
					"subscriptionName": sub.pubsub,
					"durable":          "true",
					"shared":           "true",
					"selector":         sub.selector,
				},
			}, func(_ context.Context, e *common.TopicEvent) (bool, error) {
				received.observe(fmt.Sprint(e.Data))
				return false, nil
			}))
		}

		return errs
	}

	// The replica of a rolling update that changes the selector of billing.
	changed := newTally()
	changedReplica := func(ctx flow.Context, s common.Service) error {
		return s.AddTopicEventHandler(&common.Subscription{
			PubsubName: namedSubscriptions[0].pubsub,
			Topic:      namedTopic,
			Route:      "/" + namedSubscriptions[0].pubsub,
			Metadata: map[string]string{
				"subscriptionName": namedSubscriptions[0].pubsub,
				"durable":          "true",
				"shared":           "true",
				"selector":         changedSelector,
			},
		}, func(_ context.Context, e *common.TopicEvent) (bool, error) {
			changed.observe(fmt.Sprint(e.Data))
			return false, nil
		})
	}

	replicaOptions := func(n int) []embedded.Option {
		offset := replicaPortOffset + n*portOffset
		return append(componentRuntimeOptions(),
			embedded.WithResourcesPath("./components/named"),
			embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+offset)),
			embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+offset)),
			embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+offset)),
			embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+offset)),
			embedded.WithGracefulShutdownDuration(0),
		)
	}

	// The backlog is published in one step and verified in a later one.
	var backlog []event

	flow.New(t, "amqp named subscriptions").
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for the broker socket",
			network.WaitForAddresses(5*time.Minute, brokers...)).
		Step("wait for broker readiness", flow.Sleep(20*time.Second)).
		Step(app.Run(replicaApp1, fmt.Sprintf(":%d", appPort+replicaPortOffset), replica)).
		Step(sidecar.Run(replicaSidecar1, replicaOptions(0)...)).
		Step(app.Run(replicaApp2, fmt.Sprintf(":%d", appPort+replicaPortOffset+portOffset), replica)).
		Step(sidecar.Run(replicaSidecar2, replicaOptions(1)...)).
		Step("wait for the subscriptions to attach", flow.Sleep(10*time.Second)).
		//
		// Each named subscription is one durable broker queue with its
		// selector as the queue filter, consumed by both replicas.
		Step("each subscription is a shared, durable, filtered queue",
			assertNamedQueues(2, 0)).
		//
		// Each message reaches exactly one replica, and only the subscription
		// whose selector matches it.
		Step("publish and verify", func(ctx flow.Context) error {
			events := newEvents("live", 30)
			require.NoError(ctx, publishEvents(ctx, events, 0))
			for _, sub := range namedSubscriptions {
				tallies[sub.pubsub].expectExactlyOnce(ctx, eventsOfType(events, sub.eventType), time.Minute)
			}

			return nil
		}).
		Step("reset", resetTallies(tallies)).
		//
		// Restart one replica at a time while events are published. The other
		// replica keeps consuming, so no event may be lost. An event that was
		// in flight on a stopping replica is delivered again, so this step
		// allows duplicates.
		Step("publish during a rolling restart", func(ctx flow.Context) error {
			events := newEvents("rolling", 80)
			published := make(chan error, 1)
			go func() { published <- publishEvents(ctx, events, 250*time.Millisecond) }()

			require.NoError(ctx, sidecar.Stop(replicaSidecar1)(ctx))
			require.NoError(ctx, sidecar.Start(replicaSidecar1, replicaOptions(0)...)(ctx))
			require.NoError(ctx, sidecar.Stop(replicaSidecar2)(ctx))
			require.NoError(ctx, sidecar.Start(replicaSidecar2, replicaOptions(1)...)(ctx))
			require.NoError(ctx, <-published)

			for _, sub := range namedSubscriptions {
				tallies[sub.pubsub].expectAtLeastOnce(ctx, eventsOfType(events, sub.eventType), 2*time.Minute)
			}

			return nil
		}).
		Step("reset", resetTallies(tallies)).
		//
		// A durable subscription outlives its consumers. Stopping the sidecars
		// must not delete it, and it keeps what is published meanwhile, also
		// across a broker restart.
		Step("stop replica 1", sidecar.Stop(replicaSidecar1)).
		Step("stop replica 2", sidecar.Stop(replicaSidecar2)).
		Step("the subscriptions outlive their consumers", assertNamedQueues(0, 0)).
		Step("publish while no replica runs", func(ctx flow.Context) error {
			backlog = newEvents("backlog", 15)
			return publishEvents(ctx, backlog, 0)
		}).
		Step("stop the broker",
			dockercompose.Stop(clusterName, dockerComposeYAML, brokerService)).
		Step("start the broker",
			dockercompose.Start(clusterName, dockerComposeYAML, brokerService)).
		Step("wait for the broker socket",
			network.WaitForAddresses(5*time.Minute, brokers...)).
		Step("wait for broker readiness", flow.Sleep(20*time.Second)).
		Step("the subscriptions and their messages survive a broker restart",
			assertNamedQueues(0, 5)).
		Step("start replica 1", sidecar.Start(replicaSidecar1, replicaOptions(0)...)).
		Step("start replica 2", sidecar.Start(replicaSidecar2, replicaOptions(1)...)).
		Step("verify the backlog", func(ctx flow.Context) error {
			for _, sub := range namedSubscriptions {
				tallies[sub.pubsub].expectExactlyOnce(ctx, eventsOfType(backlog, sub.eventType), 2*time.Minute)
			}

			return nil
		}).
		Step("reset", resetTallies(tallies)).
		//
		// A replica that brings another selector, as a rolling update that
		// changes it does, is refused while the other replicas are attached.
		// Its subscription must end there: retrying until the other replicas
		// stop would have the broker recreate the queue and drop its messages.
		Step(app.Run(replicaApp3, fmt.Sprintf(":%d", appPort+replicaPortOffset+2*portOffset), changedReplica)).
		Step(sidecar.Run(replicaSidecar3, replicaOptions(2)...)).
		Step("wait for the refusal", flow.Sleep(10*time.Second)).
		Step("the subscription keeps its selector and its consumers", assertNamedQueues(2, 0)).
		Step("stop replica 1", sidecar.Stop(replicaSidecar1)).
		Step("stop replica 2", sidecar.Stop(replicaSidecar2)).
		Step("give a retry time to recreate the queue", flow.Sleep(10*time.Second)).
		Step("the subscription keeps its selector with no consumer", assertNamedQueues(0, 0)).
		Step("publish while no replica consumes", func(ctx flow.Context) error {
			backlog = newEvents("kept", 15)
			return publishEvents(ctx, backlog, 0)
		}).
		Step("the subscription keeps its messages", assertNamedQueues(0, 5)).
		Step("stop replica 3", sidecar.Stop(replicaSidecar3)).
		Step("start replica 1", sidecar.Start(replicaSidecar1, replicaOptions(0)...)).
		Step("start replica 2", sidecar.Start(replicaSidecar2, replicaOptions(1)...)).
		Step("verify the kept messages", func(ctx flow.Context) error {
			for _, sub := range namedSubscriptions {
				tallies[sub.pubsub].expectExactlyOnce(ctx, eventsOfType(backlog, sub.eventType), 2*time.Minute)
			}

			// Nothing may have reached the refused replica.
			changed.expectExactlyOnce(ctx, nil, time.Second)

			return nil
		}).
		Run()
}

// event is one message published straight to the broker.
type event struct {
	data      string
	eventType string
}

// newEvents returns n events that cycle through the event types of both
// subscriptions and one type no subscription selects.
func newEvents(prefix string, n int) []event {
	types := []string{namedSubscriptions[0].eventType, namedSubscriptions[1].eventType, unmatchedEventType}

	events := make([]event, n)
	for i := range events {
		eventType := types[i%len(types)]
		events[i] = event{data: fmt.Sprintf("%s-%s-%03d", prefix, eventType, i), eventType: eventType}
	}

	return events
}

func eventsOfType(events []event, eventType string) []string {
	var data []string
	for _, e := range events {
		if e.eventType == eventType {
			data = append(data, e.data)
		}
	}

	return data
}

// publishEvents sends events to the broker with a plain AMQP client, the way a
// non-Dapr publisher would. The selectors match on the eventType application
// property, which a Dapr publish does not set. Each body is a CloudEvent, so
// the subscribers need no rawPayload, and each message is durable, so it
// survives a broker restart. interval spaces the events out.
func publishEvents(ctx context.Context, events []event, interval time.Duration) error {
	conn, err := amqp.Dial(ctx, brokerURL, &amqp.ConnOptions{SASLType: amqp.SASLTypePlain(brokerUser, brokerPassword)})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		return fmt.Errorf("session: %w", err)
	}

	sender, err := session.NewSender(ctx, namedTopic, nil)
	if err != nil {
		return fmt.Errorf("sender: %w", err)
	}
	defer sender.Close(context.Background())

	for _, e := range events {
		body, err := json.Marshal(map[string]any{
			"specversion":     "1.0",
			"id":              e.data,
			"source":          "amqp-certification",
			"type":            "com.example." + e.eventType,
			"datacontenttype": "text/plain",
			"data":            e.data,
		})
		if err != nil {
			return err
		}

		msg := amqp.NewMessage(body)
		msg.Header = &amqp.MessageHeader{Durable: true}
		msg.ApplicationProperties = map[string]any{"eventType": e.eventType}
		if err = sender.Send(ctx, msg, nil); err != nil {
			return fmt.Errorf("send %s: %w", e.data, err)
		}

		time.Sleep(interval)
	}

	return nil
}

// tally counts deliveries per message. The flow watcher ignores duplicates and
// messages it does not expect, and this test is about exactly those.
type tally struct {
	mu   sync.Mutex
	seen map[string]int
}

func newTally() *tally {
	return &tally{seen: map[string]int{}}
}

func (t *tally) observe(data string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.seen[data]++
}

func (t *tally) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.seen = map[string]int{}
}

func resetTallies(tallies map[string]*tally) flow.Runnable {
	return func(flow.Context) error {
		for _, tl := range tallies {
			tl.reset()
		}

		return nil
	}
}

// expectExactlyOnce waits until every message in want arrived. It then waits a
// little longer, so that a duplicate or a message that does not belong has time
// to arrive, and checks that each message arrived once and nothing else did.
func (t *tally) expectExactlyOnce(ctx flow.Context, want []string, timeout time.Duration) {
	t.expect(ctx, want, timeout, true)
}

// expectAtLeastOnce is expectExactlyOnce for a step that redelivers, such as a
// rolling restart. A message may arrive more than once, but none may be
// missing and none may reach a subscription whose selector does not match it.
func (t *tally) expectAtLeastOnce(ctx flow.Context, want []string, timeout time.Duration) {
	t.expect(ctx, want, timeout, false)
}

func (t *tally) expect(ctx flow.Context, want []string, timeout time.Duration, once bool) {
	require.Eventually(ctx, func() bool {
		t.mu.Lock()
		defer t.mu.Unlock()

		for _, w := range want {
			if t.seen[w] == 0 {
				return false
			}
		}

		return true
	}, timeout, 250*time.Millisecond, "not every message arrived")

	time.Sleep(5 * time.Second)

	t.mu.Lock()
	defer t.mu.Unlock()

	expected := make(map[string]bool, len(want))
	for _, w := range want {
		expected[w] = true
		if once {
			require.Equalf(ctx, 1, t.seen[w], "message %q was delivered %d times", w, t.seen[w])
		}
	}

	for data := range t.seen {
		require.Truef(ctx, expected[data], "message %q was delivered to a subscription whose selector does not match it", data)
	}
}

// assertNamedQueues checks through the broker management API that each named
// subscription is one durable queue, named after the container ID and the
// subscription name, filtered by its selector, with the given number of
// consumers and messages.
//
// The broker drops the consumers of a stopped sidecar, and starts answering
// its management API, a moment after the event, so this polls for up to a
// minute.
func assertNamedQueues(consumers, messages int) flow.Runnable {
	return func(ctx flow.Context) error {
		deadline := time.Now().Add(time.Minute)
		for {
			mismatch := namedQueuesMismatch(consumers, messages)
			if mismatch == "" {
				return nil
			}

			if time.Now().After(deadline) {
				require.FailNow(ctx, mismatch)
			}

			time.Sleep(time.Second)
		}
	}
}

// namedQueuesMismatch describes the first way the broker queues differ from
// what assertNamedQueues expects, or returns an empty string.
func namedQueuesMismatch(consumers, messages int) string {
	queues, err := listBrokerQueues()
	if err != nil {
		return fmt.Sprintf("cannot list the broker queues: %v", err)
	}

	for _, sub := range namedSubscriptions {
		name := namedContainerID + "." + sub.pubsub
		q, ok := queues[name]

		switch {
		case !ok:
			return fmt.Sprintf("the broker has no queue %q", name)
		case q.Address != namedTopic, q.Durable != "true", q.Filter != sub.selector:
			return fmt.Sprintf("queue %q has address %q, durable %s and filter %q", name, q.Address, q.Durable, q.Filter)
		case q.ConsumerCount != strconv.Itoa(consumers), q.MessageCount != strconv.Itoa(messages):
			return fmt.Sprintf("queue %q has %s consumers and %s messages, want %d and %d", name, q.ConsumerCount, q.MessageCount, consumers, messages)
		}
	}

	return ""
}

// brokerQueue is the part of an Artemis listQueues row this test reads. Artemis
// reports every value as a string.
type brokerQueue struct {
	Name          string `json:"name"`
	Address       string `json:"address"`
	Filter        string `json:"filter"`
	Durable       string `json:"durable"`
	ConsumerCount string `json:"consumerCount"`
	MessageCount  string `json:"messageCount"`
}

// listBrokerQueues reads every queue through the Jolokia endpoint of the
// Artemis management console.
func listBrokerQueues() (map[string]brokerQueue, error) {
	body, err := json.Marshal(map[string]any{
		"type":      "exec",
		"mbean":     brokerMBean,
		"operation": "listQueues(java.lang.String,int,int)",
		"arguments": []any{`{"field":"","operation":"","value":""}`, 1, 1000},
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, jolokiaURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(brokerUser, brokerPassword)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://localhost")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result struct {
		Status int    `json:"status"`
		Value  string `json:"value"`
		Error  string `json:"error"`
	}
	if err = json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("decode the management response: %w", err)
	}

	if result.Status != http.StatusOK {
		return nil, errors.New(result.Error)
	}

	var page struct {
		Data []brokerQueue `json:"data"`
	}
	if err = json.Unmarshal([]byte(result.Value), &page); err != nil {
		return nil, fmt.Errorf("decode the queue list: %w", err)
	}

	queues := make(map[string]brokerQueue, len(page.Data))
	for _, q := range page.Data {
		queues[q.Name] = q
	}

	return queues, nil
}
