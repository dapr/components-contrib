/*
Copyright 2024 The Dapr Authors
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

package servicebus_queues_test

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub-Sub.
	pubsub_servicebus "github.com/dapr/components-contrib/pubsub/azure/servicebus/queues"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	"github.com/dapr/dapr/pkg/config/protocol"
	"github.com/dapr/kit/logger"

	"github.com/dapr/dapr/pkg/runtime"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"

	// Certification testing runnables
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
)

const (
	sidecarName1 = "dapr-1"
	sidecarName2 = "dapr-2"

	appID1 = "app-1"
	appID2 = "app-2"

	numMessages = 10
	appPort     = 8000
	portOffset  = 2
	pubsubName  = "messagebus"
)

var (
	sessionIDRegex = regexp.MustCompile("sessionId: (.*)")
)

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")
	log.SetOutputLevel(logger.DebugLevel)

	pubsubRegistry := pubsub_loader.NewRegistry()
	pubsubRegistry.Logger = log
	pubsubRegistry.RegisterComponent(pubsub_servicebus.NewAzureServiceBusQueues, "azure.servicebus.queues")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []embedded.Option{
		embedded.WithPubSubs(pubsubRegistry),
		embedded.WithSecretStores(secretstoreRegistry),
	}
}

// TestServicebusQueuesWithSessionsFIFO tests that if we publish messages to the same
// queue but with 2 different session ids (session1 and session2), then the
// receiver only receives messages from a single session and in FIFO order.
// This is the key test for verifying that session support works correctly for queues.
func TestServicebusQueuesWithSessionsFIFO(t *testing.T) {
	queue := "sessions-fifo-queue"
	session1 := "session1"
	session2 := "session2"

	sessionWatcher := watcher.NewOrdered()

	// subscriber of the given queue with sessions enabled
	subscriberApplicationWithSessions := func(appID string, queueName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Setup the /orders event handler with sessions enabled.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queueName,
					Route:      "/orders",
					Metadata: map[string]string{
						"requireSessions":       "true",
						"maxConcurrentSessions": "1",
					},
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					// Track/Observe the data of the event.
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s, pubsub: %s, queue: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, queueName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				var msgSuffix string
				if metadata["SessionId"] != "" {
					msgSuffix = fmt.Sprintf(", sessionId: %s", metadata["SessionId"])
				}
				messages[i] = fmt.Sprintf("message for queue: %s, index: %03d, uniqueId: %s%s", queueName, i, uuid.New().String(), msgSuffix)
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, queueName: %s, sessionId: %s", sidecarName, queueName, metadata["SessionId"])

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, queueName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, queueName, message)
				}
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessagesInOrder := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				tctx, exp, obs := m.Partial(ctx, timeout)

				var observed []string
				if obs != nil {
					for _, v := range obs.([]interface{}) {
						observed = append(observed, v.(string))
					}
				}
				var expected []string
				if exp != nil {
					for _, v := range exp.([]interface{}) {
						expected = append(expected, v.(string))
					}
				}

				// ensure all the observed messages are present in the expected messages.
				for _, msg := range observed {
					found := false
					for _, expMsg := range expected {
						if msg == expMsg {
							found = true
							break
						}
					}
					if !found {
						tctx.Errorf("message not found in expected messages: %s", msg)
					}
				}

				// Group messages by session id to verify FIFO order within each session.
				// With maxConcurrentSessions=1, sessions are processed one at a time sequentially,
				// so we may receive messages from multiple sessions, but each session's messages
				// must be in FIFO order.
				sessionMessages := make(map[string][]string)
				for _, msg := range observed {
					match := sessionIDRegex.FindStringSubmatch(msg)
					if len(match) > 0 {
						sessionID := match[1]
						sessionMessages[sessionID] = append(sessionMessages[sessionID], msg)
					} else {
						tctx.Error("session id not found in message")
					}
				}

				// Verify at least one session was received
				if len(sessionMessages) == 0 {
					tctx.Error("no messages received from any session")
				}

				// For each session, verify messages are in FIFO order
				for sessionID, msgs := range sessionMessages {
					// Build expected messages for this session in order
					var expectedForSession []string
					for _, msg := range expected {
						match := sessionIDRegex.FindStringSubmatch(msg)
						if len(match) > 0 && match[1] == sessionID {
							expectedForSession = append(expectedForSession, msg)
						}
					}

					// Verify the observed messages for this session match expected order
					if !assert.Equal(t, expectedForSession, msgs) {
						tctx.Errorf("session %s: expected: %v, observed: %v", sessionID, expectedForSession, msgs)
					} else {
						t.Logf("session %s: FIFO order verified for %d messages", sessionID, len(msgs))
					}
				}
			}

			return nil
		}
	}

	flow.New(t, "servicebus queues certification sessions FIFO test").

		// Run subscriberApplicationWithSessions app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplicationWithSessions(appID1, queue, sessionWatcher))).

		// Run the Dapr sidecar with the servicebus queues component
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/sessions"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish messages to queue on session 2", publishMessages(map[string]string{
			"SessionId": session2,
		}, sidecarName1, queue, sessionWatcher)).
		Step("publish messages to queue on session 1", publishMessages(map[string]string{
			"SessionId": session1,
		}, sidecarName1, queue, sessionWatcher)).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("verify messages received in order from a single session", assertMessagesInOrder(time.Minute, sessionWatcher)).
		Step("reset", flow.Reset(sessionWatcher)).
		Run()
}

// TestServicebusQueuesWithSessionsRoundRobin tests that if we publish messages to the same
// queue but with 2 different session ids (session1 and session2), then eventually
// the receiver will receive messages from both the sessions.
func TestServicebusQueuesWithSessionsRoundRobin(t *testing.T) {
	queue := "sessions-rr-queue"
	session1 := "session1"
	session2 := "session2"

	sessionWatcher := watcher.NewUnordered()

	// subscriber of the given queue with sessions enabled
	subscriberApplicationWithSessions := func(appID string, queueName string, messageWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Setup the /orders event handler with sessions enabled.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queueName,
					Route:      "/orders",
					Metadata: map[string]string{
						"requireSessions":         "true",
						"maxConcurrentSessions":   "1",
						"sessionIdleTimeoutInSec": "2", // timeout and try another session
					},
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					// Track/Observe the data of the event.
					messageWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s, pubsub: %s, queue: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, queueName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				var msgSuffix string
				if metadata["SessionId"] != "" {
					msgSuffix = fmt.Sprintf(", sessionId: %s", metadata["SessionId"])
				}
				messages[i] = fmt.Sprintf("message for queue: %s, index: %03d, uniqueId: %s%s", queueName, i, uuid.New().String(), msgSuffix)
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, queueName: %s, sessionId: %s", sidecarName, queueName, metadata["SessionId"])

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, queueName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, queueName, message)
				}
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}

			return nil
		}
	}

	flow.New(t, "servicebus queues certification sessions round robin test").

		// Run subscriberApplicationWithSessions app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplicationWithSessions(appID1, queue, sessionWatcher))).

		// Run the Dapr sidecar with the servicebus queues component
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/sessions"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish messages to queue on session 2", publishMessages(map[string]string{
			"SessionId": session2,
		}, sidecarName1, queue, sessionWatcher)).
		Step("publish messages to queue on session 1", publishMessages(map[string]string{
			"SessionId": session1,
		}, sidecarName1, queue, sessionWatcher)).
		Step("verify messages received from both sessions", assertMessages(1*time.Second, sessionWatcher)).
		Step("reset", flow.Reset(sessionWatcher)).
		Run()
}

// TestServicebusQueuesWithMultipleSessions tests that with maxConcurrentSessions > 1,
// messages from multiple sessions can be processed concurrently while maintaining
// FIFO order within each session.
func TestServicebusQueuesWithMultipleSessions(t *testing.T) {
	queue := "multi-sessions-queue"
	session1 := "order-001"
	session2 := "order-002"
	session3 := "order-003"

	sessionWatcher := watcher.NewUnordered() // Unordered because we have multiple concurrent sessions

	// subscriber with multiple concurrent sessions
	subscriberApplicationWithMultipleSessions := func(appID string, queueName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queueName,
					Route:      "/orders",
					Metadata: map[string]string{
						"requireSessions":       "true",
						"maxConcurrentSessions": "3", // Allow 3 concurrent sessions
					},
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s, pubsub: %s, queue: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessagesToSession := func(sessionID string, sidecarName string, queueName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			messages := make([]string, 5) // 5 messages per session
			for i := range messages {
				messages[i] = fmt.Sprintf("queue: %s, session: %s, index: %03d, uuid: %s, sessionId: %s", queueName, sessionID, i, uuid.New().String(), sessionID)
			}

			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			client := sidecar.GetClient(ctx, sidecarName)

			ctx.Logf("Publishing messages. sidecarName: %s, queueName: %s, sessionId: %s", sidecarName, queueName, sessionID)

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				err := client.PublishEvent(ctx, pubsubName, queueName, message, dapr.PublishEventWithMetadata(map[string]string{
					"SessionId": sessionID,
				}))
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertAllMessagesReceived := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			for _, m := range messageWatchers {
				m.Assert(ctx, timeout)
			}
			return nil
		}
	}

	flow.New(t, "servicebus queues certification multiple sessions test").

		// Run subscriber app with multiple sessions
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplicationWithMultipleSessions(appID1, queue, sessionWatcher))).

		// Run the Dapr sidecar
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/sessions"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish messages to session 1", publishMessagesToSession(session1, sidecarName1, queue, sessionWatcher)).
		Step("publish messages to session 2", publishMessagesToSession(session2, sidecarName1, queue, sessionWatcher)).
		Step("publish messages to session 3", publishMessagesToSession(session3, sidecarName1, queue, sessionWatcher)).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("verify all messages received", assertAllMessagesReceived(2*time.Minute, sessionWatcher)).
		Step("reset", flow.Reset(sessionWatcher)).
		Run()
}

// TestServicebusQueuesDefaultBehavior tests that when NO session parameters are specified,
// the queue works as before (without sessions). This ensures backward compatibility.
func TestServicebusQueuesDefaultBehavior(t *testing.T) {
	queue := "default-behavior-queue"

	messagesWatcher := watcher.NewUnordered()

	// subscriber WITHOUT any session metadata - should work as normal queue
	subscriberApplicationDefault := func(appID string, queueName string, msgWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queueName,
					Route:      "/orders",
					// NO session metadata - default behavior
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					msgWatcher.Observe(e.Data)
					ctx.Logf("Message Received (default mode) appID: %s, pubsub: %s, queue: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessagesWithoutSession := func(sidecarName string, queueName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			messages := make([]string, numMessages)
			for i := range messages {
				// Messages WITHOUT session ID
				messages[i] = fmt.Sprintf("message for queue: %s, index: %03d, uniqueId: %s", queueName, i, uuid.New().String())
			}

			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			client := sidecar.GetClient(ctx, sidecarName)
			ctx.Logf("Publishing messages WITHOUT session. sidecarName: %s, queueName: %s", sidecarName, queueName)

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				// Publish WITHOUT SessionId metadata
				err := client.PublishEvent(ctx, pubsubName, queueName, message)
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			for _, m := range messageWatchers {
				m.Assert(ctx, timeout)
			}
			return nil
		}
	}

	flow.New(t, "servicebus queues certification - default behavior (no sessions)").

		// Run subscriber app WITHOUT session configuration
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplicationDefault(appID1, queue, messagesWatcher))).

		// Run the Dapr sidecar with DEFAULT component (no session config)
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/default"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish messages without session", publishMessagesWithoutSession(sidecarName1, queue, messagesWatcher)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("verify all messages received", assertMessages(time.Minute, messagesWatcher)).
		Step("reset", flow.Reset(messagesWatcher)).
		Run()
}

// TestServicebusQueuesOptionalSessionParameters tests that session parameters are truly optional:
// - When requireSessions is NOT set, it defaults to false (no sessions)
// - When requireSessions is set to "true", sessions are enabled
// - maxConcurrentSessions and sessionIdleTimeoutInSec have sensible defaults
func TestServicebusQueuesOptionalSessionParameters(t *testing.T) {
	t.Run("no session params - works without sessions", func(t *testing.T) {
		queue := "optional-params-no-session"
		messagesWatcher := watcher.NewUnordered()

		subscriberNoSession := func(appID string, queueName string, msgWatcher *watcher.Watcher) app.SetupFn {
			return func(ctx flow.Context, s common.Service) error {
				return multierr.Combine(
					s.AddTopicEventHandler(&common.Subscription{
						PubsubName: pubsubName,
						Topic:      queueName,
						Route:      "/orders",
						// Explicitly NO session metadata
					}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
						msgWatcher.Observe(e.Data)
						ctx.Logf("Message Received (no session) appID: %s, data: %s", appID, e.Data)
						return false, nil
					}),
				)
			}
		}

		publishMessages := func(sidecarName string, queueName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
			return func(ctx flow.Context) error {
				messages := make([]string, 5)
				for i := range messages {
					messages[i] = fmt.Sprintf("no-session-msg: %s, index: %d, uuid: %s", queueName, i, uuid.New().String())
				}
				for _, mw := range messageWatchers {
					mw.ExpectStrings(messages...)
				}
				client := sidecar.GetClient(ctx, sidecarName)
				for _, msg := range messages {
					err := client.PublishEvent(ctx, pubsubName, queueName, msg)
					require.NoError(ctx, err)
				}
				return nil
			}
		}

		assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
			return func(ctx flow.Context) error {
				for _, m := range messageWatchers {
					m.Assert(ctx, timeout)
				}
				return nil
			}
		}

		flow.New(t, "no session params test").
			Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset*2),
				subscriberNoSession(appID1, queue, messagesWatcher))).
			Step(sidecar.Run(sidecarName1,
				append(componentRuntimeOptions(),
					embedded.WithComponentsPath("./components/default"),
					embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset*2)),
					embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset*2)),
					embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset*2)),
					embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset*2)),
				)...,
			)).
			Step("publish without session", publishMessages(sidecarName1, queue, messagesWatcher)).
			Step("wait", flow.Sleep(10*time.Second)).
			Step("verify messages received", assertMessages(time.Minute, messagesWatcher)).
			Run()
	})

	t.Run("with session params - works with sessions", func(t *testing.T) {
		queue := "optional-params-with-session"
		sessionID := "test-session-123"
		messagesWatcher := watcher.NewOrdered()

		subscriberWithSession := func(appID string, queueName string, msgWatcher *watcher.Watcher) app.SetupFn {
			return func(ctx flow.Context, s common.Service) error {
				return multierr.Combine(
					s.AddTopicEventHandler(&common.Subscription{
						PubsubName: pubsubName,
						Topic:      queueName,
						Route:      "/orders",
						Metadata: map[string]string{
							"requireSessions": "true",
							// maxConcurrentSessions and sessionIdleTimeoutInSec use defaults
						},
					}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
						msgWatcher.Observe(e.Data)
						ctx.Logf("Message Received (with session) appID: %s, data: %s", appID, e.Data)
						return false, nil
					}),
				)
			}
		}

		publishMessagesWithSession := func(sidecarName string, queueName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
			return func(ctx flow.Context) error {
				messages := make([]string, 5)
				for i := range messages {
					messages[i] = fmt.Sprintf("with-session-msg: %s, index: %d, uuid: %s, sessionId: %s", queueName, i, uuid.New().String(), sessionID)
				}
				for _, mw := range messageWatchers {
					mw.ExpectStrings(messages...)
				}
				client := sidecar.GetClient(ctx, sidecarName)
				for _, msg := range messages {
					err := client.PublishEvent(ctx, pubsubName, queueName, msg, dapr.PublishEventWithMetadata(map[string]string{
						"SessionId": sessionID,
					}))
					require.NoError(ctx, err)
				}
				return nil
			}
		}

		assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
			return func(ctx flow.Context) error {
				for _, m := range messageWatchers {
					m.Assert(ctx, timeout)
				}
				return nil
			}
		}

		flow.New(t, "with session params test").
			Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset*3),
				subscriberWithSession(appID2, queue, messagesWatcher))).
			Step(sidecar.Run(sidecarName2,
				append(componentRuntimeOptions(),
					embedded.WithComponentsPath("./components/sessions"),
					embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset*3)),
					embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset*3)),
					embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset*3)),
					embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset*3)),
				)...,
			)).
			Step("publish with session", publishMessagesWithSession(sidecarName2, queue, messagesWatcher)).
			Step("wait", flow.Sleep(15*time.Second)).
			Step("verify messages received", assertMessages(time.Minute, messagesWatcher)).
			Run()
	})
}

// TestServicebusQueuesPartitionedSessions tests the partitioned session mode where
// explicit session IDs are defined in the component configuration. This mode enables
// controlled, predictable scaling with FIFO ordering per partition.
func TestServicebusQueuesPartitionedSessions(t *testing.T) {
	queue := "partitioned-sessions-queue-" + uuid.New().String()[:8]
	partitions := []string{"partition-A", "partition-B", "partition-C"}

	messagesWatcher := watcher.NewOrdered()

	// subscriber for partitioned sessions - no metadata override needed since sessionIds is in component
	subscriberPartitioned := func(appID string, queueName string, msgWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			return s.AddTopicEventHandler(&common.Subscription{
				PubsubName: pubsubName,
				Topic:      queueName,
				Route:      "/partitioned-orders",
				// No session metadata needed - component has sessionIds configured
			}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
				msgWatcher.Observe(e.Data)
				ctx.Logf("Partitioned Message Received appID: %s, data: %s", appID, e.Data)
				return false, nil
			})
		}
	}

	publishToPartitions := func(sidecarName string, queueName string, msgWatcher *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			var allMessages []string

			// Publish messages to each partition
			for _, partition := range partitions {
				for i := 0; i < 5; i++ {
					msg := fmt.Sprintf("partition: %s, index: %03d, uuid: %s, sessionId: %s",
						partition, i, uuid.New().String(), partition)
					allMessages = append(allMessages, msg)
				}
			}

			msgWatcher.ExpectStrings(allMessages...)

			client := sidecar.GetClient(ctx, sidecarName)

			for _, msg := range allMessages {
				// Extract partition from message
				var partition string
				for _, p := range partitions {
					if regexp.MustCompile(fmt.Sprintf("partition: %s", p)).MatchString(msg) {
						partition = p
						break
					}
				}

				ctx.Logf("Publishing to partition %s: %s", partition, msg)
				err := client.PublishEvent(ctx, pubsubName, queueName, msg,
					dapr.PublishEventWithMetadata(map[string]string{
						"SessionId": partition,
					}))
				require.NoError(ctx, err)
			}

			return nil
		}
	}

	verifyPartitionedFIFO := func(timeout time.Duration, msgWatcher *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			_, expected, observed := msgWatcher.Partial(ctx, timeout)

			var observedMsgs []string
			if observed != nil {
				for _, v := range observed.([]interface{}) {
					observedMsgs = append(observedMsgs, v.(string))
				}
			}

			var expectedMsgs []string
			if expected != nil {
				for _, v := range expected.([]interface{}) {
					expectedMsgs = append(expectedMsgs, v.(string))
				}
			}

			// Group by partition and verify FIFO within each
			partitionMsgs := make(map[string][]string)
			for _, msg := range observedMsgs {
				match := sessionIDRegex.FindStringSubmatch(msg)
				if len(match) > 0 {
					partitionMsgs[match[1]] = append(partitionMsgs[match[1]], msg)
				}
			}

			t.Logf("Received messages from %d partitions", len(partitionMsgs))

			for partition, msgs := range partitionMsgs {
				// Build expected order for this partition
				var expectedForPartition []string
				for _, msg := range expectedMsgs {
					match := sessionIDRegex.FindStringSubmatch(msg)
					if len(match) > 0 && match[1] == partition {
						expectedForPartition = append(expectedForPartition, msg)
					}
				}

				if !assert.Equal(t, expectedForPartition, msgs) {
					t.Errorf("partition %s: FIFO order violated", partition)
				} else {
					t.Logf("partition %s: FIFO verified for %d messages", partition, len(msgs))
				}
			}

			return nil
		}
	}

	flow.New(t, "partitioned sessions test").
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset*4),
			subscriberPartitioned(appID1, queue, messagesWatcher))).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/partitioned"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset*4)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset*4)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset*4)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset*4)),
			)...,
		)).
		Step("publish to all partitions", publishToPartitions(sidecarName1, queue, messagesWatcher)).
		Step("wait for processing", flow.Sleep(20*time.Second)).
		Step("verify partitioned FIFO", verifyPartitionedFIFO(time.Minute, messagesWatcher)).
		Run()
}
