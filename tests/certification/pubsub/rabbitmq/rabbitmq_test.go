/*
Copyright 2021 The Dapr Authors
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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dapr/go-sdk/service/common"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	pubsub_rabbitmq "github.com/dapr/components-contrib/pubsub/rabbitmq"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/simulate"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	"github.com/dapr/dapr/pkg/config/protocol"
	"github.com/dapr/dapr/pkg/runtime"
	daprClient "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
	kit_retry "github.com/dapr/kit/retry"
)

const (
	sidecarName1             = "dapr-1"
	sidecarName2             = "dapr-2"
	sidecarName3             = "dapr-3"
	sidecarName4             = "dapr-4"
	sidecarNameMetadata      = "dapr-metadata"
	sidecarNameTTLClient     = "dapr-ttl-client"
	appID1                   = "app-1"
	appID2                   = "app-2"
	appID3                   = "app-3"
	appID4                   = "app-4"
	appIDMetadata            = "app-metadata"
	clusterName              = "rabbitmqcertification"
	dockerComposeYAML        = "docker-compose.yml"
	extSaslDockerComposeYAML = "mtls_sasl_external/docker-compose.yml"
	numMessages              = 1000
	errFrequency             = 100
	appPort                  = 8000

	rabbitMQURL        = "amqp://test:test@localhost:5672"
	rabbitMQURLExtAuth = "amqps://localhost:5671"

	pubsubAlpha          = "mq-alpha"
	pubsubBeta           = "mq-beta"
	pubsubMtlsExternal   = "mq-mtls"
	pubsubMetadata       = "mq-metadata"
	pubsubMessageOnlyTTL = "msg-ttl-pubsub"
	pubsubQueueOnlyTTL   = "overwrite-ttl-pubsub"
	pubsubOverwriteTTL   = "queue-ttl-pubsub"
	pubsubPriority       = "mq-priority"

	topicRed   = "red"
	topicBlue  = "blue"
	topicGreen = "green"

	topicTTL1 = "ttl1"
	topicTTL2 = "ttl2"
	topicTTL3 = "ttl3"

	topicMetadata = "metadata-topic"
)

type Consumer struct {
	pubsub   string
	messages map[string]*watcher.Watcher
}

func amqpReady(url string) flow.Runnable {
	return func(ctx flow.Context) error {
		conn, err := amqp.Dial(url)
		if err != nil {
			return err
		}
		defer conn.Close()

		ch, err := conn.Channel()
		if err != nil {
			return err
		}
		defer ch.Close()

		return nil
	}
}

func amqpMtlsExternalAuthReady(url string) flow.Runnable {
	return func(ctx flow.Context) error {
		cer, err := tls.LoadX509KeyPair("./mtls_sasl_external/docker_sasl_external/certs/client/cert.pem", "./mtls_sasl_external/docker_sasl_external/certs/client/key.pem")
		if err != nil {
			log.Println(err)
		}

		tlsConfig := &tls.Config{Certificates: []tls.Certificate{cer}}
		tlsConfig.InsecureSkipVerify = false
		tlsConfig.RootCAs = x509.NewCertPool()
		if ok := tlsConfig.RootCAs.AppendCertsFromPEM([]byte("-----BEGIN CERTIFICATE-----\nMIIC8DCCAdigAwIBAgIUHyqaUOmitCL9oR5ut9c9A7kfapEwDQYJKoZIhvcNAQEL\nBQAwEzERMA8GA1UEAwwITXlUZXN0Q0EwHhcNMjMwMjA4MjMxNTI2WhcNMjQwMjA4\nMjMxNTI2WjATMREwDwYDVQQDDAhNeVRlc3RDQTCCASIwDQYJKoZIhvcNAQEBBQAD\nggEPADCCAQoCggEBAOb8I5ng1cnKw37YbMBrgJQnsFOuqamSWT2AQAnzet/ZIHnE\n9cl/wjNNxluku7bR/YW1AB5syoNjyoFmLb9R8rx5awP/DrYjhyEp7DWE4attTTWB\nZQp4nFp9PDlGee5pQjZl/hq3ceqMVuCDP9OQnCv9fMYmZtpzEJuoAxOTuvc4NaNS\nFzKhvUWkpq/6lelk4r8a7nmxT7KgPbLohhXJmrfy81bQRrMz0m4eDlNDeDHm5IUg\n4dbUCsTPs8hibeogbz1DtSQh8wPe2IgsSKrJc94KSzrdhY7UohlkSxsQBXZlm/g0\nGyGdLmf39/iMn2x9bbqQodO+CiSoNm0rXdi+5zsCAwEAAaM8MDowDAYDVR0TBAUw\nAwEB/zALBgNVHQ8EBAMCAQYwHQYDVR0OBBYEFG8vXs0iB+ovHV1aISx/aJSYAOnF\nMA0GCSqGSIb3DQEBCwUAA4IBAQCOyfgf4TszN9jq+/CKJaTCC/Lw7Wkrzjx88/Sj\nCs8efyuM2ps/7+ce71jM5oUnSysg4cZcdEdKTVgd/ZQxcOyksQRskjhG/Y5MUHRl\nO2JH3zRSRKP3vKyHQ6K9DWIQw6RgC1PB+qG+MjU5MJONpn/H/7sjCeSCZqSWoled\nUhAKF0YAipYtMgpuE+lrwIu0LVQFvbK3QFPo59LYazjI4JG6mLC0mPE1rKOY4+cZ\nuDA6D/qYtM1344ZIYHrV1jhWRI8cwS0AUoYPTGb+muSXKpW0qeOJZmJli6wkAqZx\n0BULAkIRi0nBXhTP5w53TjAWwvNQ7IK+5MXBPr/f+ZjjtHIG\n-----END CERTIFICATE-----")); !ok {
			os.Exit(1)
		}
		log.Println("Trying to connect...")
		conn, err := amqp.DialTLS_ExternalAuth(url, tlsConfig)
		if err != nil {
			return err
		}
		defer conn.Close()

		ch, err := conn.Channel()
		if err != nil {
			return err
		}
		defer ch.Close()

		return nil
	}
}

func TestRabbitMQ(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	log := logger.NewLogger("dapr.components")
	// log.SetOutputLevel(logger.DebugLevel)

	pubTopics := []string{topicRed, topicBlue, topicGreen}
	subTopics := []string{topicRed, topicBlue}

	alpha := &Consumer{pubsub: pubsubAlpha, messages: make(map[string]*watcher.Watcher)}
	beta := &Consumer{pubsub: pubsubBeta, messages: make(map[string]*watcher.Watcher)}
	consumers := []*Consumer{alpha, beta}

	for _, consumer := range consumers {
		for _, topic := range pubTopics {
			// In RabbitMQ, messages might not come in order.
			consumer.messages[topic] = watcher.NewUnordered()
		}
	}

	// subscribed is used to synchronize between publisher and subscriber
	subscribed := make(chan struct{}, 1)

	// Test logic that sends messages to topics and
	// verifies the two consumers with different IDs have received them.
	test := func(ctx flow.Context) error {
		// Declare what is expected BEFORE performing any steps
		// that will satisfy the test.
		msgs := make([]string, numMessages)
		for i := range msgs {
			msgs[i] = fmt.Sprintf("Hello, Messages %03d", i)
		}

		for _, consumer := range consumers {
			for _, topic := range subTopics {
				consumer.messages[topic].ExpectStrings(msgs...)
			}
		}

		<-subscribed

		// sidecar client array []{sidecar client, pubsub component name}
		sidecars := []struct {
			client *sidecar.Client
			pubsub string
		}{
			{sidecar.GetClient(ctx, sidecarName1), pubsubAlpha},
			{sidecar.GetClient(ctx, sidecarName2), pubsubBeta},
			{sidecar.GetClient(ctx, sidecarName3), pubsubBeta},
		}

		var wg sync.WaitGroup
		wg.Add(len(pubTopics))
		for _, topic := range pubTopics {
			go func(topic string) {
				defer wg.Done()

				// Send events that the application above will observe.
				log.Infof("Sending messages on topic '%s'", topic)

				for _, msg := range msgs {
					// randomize publishers
					indx := rand.Intn(len(sidecars))
					log.Debugf("Sending: '%s' on topic '%s'", msg, topic)
					err := sidecars[indx].client.PublishEvent(ctx, sidecars[indx].pubsub, topic, msg)
					require.NoError(ctx, err, "error publishing message")
				}
			}(topic)
		}
		wg.Wait()
		log.Debugf("All messages sent - start receiving messages")

		// Do the messages we observed match what we expect?
		wg.Add(len(consumers) * len(pubTopics))
		for _, topic := range pubTopics {
			for _, consumer := range consumers {
				go func(topic string, consumer *Consumer) {
					defer wg.Done()
					consumer.messages[topic].Assert(ctx, 3*time.Minute)
				}(topic, consumer)
			}
		}
		wg.Wait()

		return nil
	}

	// Application logic that tracks messages from a topic.
	application := func(consumer *Consumer, routeIndex int, queueType string) app.SetupFn {
		return func(ctx flow.Context, s common.Service) (err error) {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, errFrequency)

			for _, topic := range subTopics {
				// Setup the /orders event handler.
				err = multierr.Combine(
					err,
					s.AddTopicEventHandler(&common.Subscription{
						PubsubName: consumer.pubsub,
						Topic:      topic,
						Route:      fmt.Sprintf("/%s-%d", topic, routeIndex),
						Metadata:   map[string]string{"queueType": queueType},
					}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
						if err := sim(); err != nil {
							log.Debugf("Simulated error - consumer: %s, pubsub: %s, topic: %s, id: %s, data: %s", consumer.pubsub, e.PubsubName, e.Topic, e.ID, e.Data)
							return true, err
						}

						// Track/Observe the data of the event.
						consumer.messages[e.Topic].Observe(e.Data)
						log.Debugf("Event - consumer: %s, pubsub: %s, topic: %s, id: %s, data: %s", consumer.pubsub, e.PubsubName, e.Topic, e.ID, e.Data)
						return false, nil
					}),
				)
			}
			return err
		}
	}

	// sendMessagesInBackground and assertMessages are
	// Runnables for testing publishing and consuming
	// messages reliably when infrastructure and network
	// interruptions occur.
	var task flow.AsyncTask
	sendMessagesInBackground := func(consumer *Consumer) flow.Runnable {
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName1)
			for _, topic := range subTopics {
				consumer.messages[topic].Reset()
			}
			t := time.NewTicker(100 * time.Millisecond)
			defer t.Stop()

			counter := 1
			for {
				select {
				case <-task.Done():
					return nil
				case <-t.C:
					for _, topic := range subTopics {
						msg := fmt.Sprintf("Background message - %03d", counter)
						consumer.messages[topic].Prepare(msg) // Track for observation

						// Publish with retries.
						bo := backoff.WithContext(backoff.NewConstantBackOff(time.Second), task)
						if err := kit_retry.NotifyRecover(func() error {
							return client.PublishEvent(
								// Using ctx instead of task here is deliberate.
								// We don't want cancelation to prevent adding
								// the message, only to interrupt between tries.
								ctx, consumer.pubsub, topic, msg)
						}, bo, func(err error, t time.Duration) {
							ctx.Logf("Error publishing message, retrying in %s", t)
						}, func() {}); err == nil {
							consumer.messages[topic].Add(msg) // Success
							counter++
						}
					}
				}
			}
		}
	}
	assertMessages := func(consumer *Consumer) flow.Runnable {
		return func(ctx flow.Context) error {
			// Signal sendMessagesInBackground to stop and wait for it to complete.
			task.CancelAndWait()
			for _, topic := range subTopics {
				consumer.messages[topic].Assert(ctx, 5*time.Minute)
			}

			return nil
		}
	}

	flow.New(t, "rabbitmq certification").
		// Run RabbitMQ using Docker Compose.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for rabbitmq readiness",
			retry.Do(time.Second, 30, amqpReady(rabbitMQURL))).
		// Run the application1 logic above.
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			application(alpha, 1, "quorum"))).
		// Run the Dapr sidecar with the RabbitMQ component.
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/alpha"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+10)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort)),
				embedded.WithGracefulShutdownDuration(2*time.Second),
			)...,
		)).
		// Run the application2 logic above.
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+2),
			application(beta, 2, "classic"))).
		// Run the Dapr sidecar with the RabbitMQ component.
		Step(sidecar.Run(sidecarName2,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/beta"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+2)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+11)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+2)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+2)),
				embedded.WithGracefulShutdownDuration(2*time.Second),
			)...,
		)).
		// Run the application3 logic above.
		Step(app.Run(appID3, fmt.Sprintf(":%d", appPort+4),
			application(beta, 3, "classic"))).
		// Run the Dapr sidecar with the RabbitMQ component.
		Step(sidecar.Run(sidecarName3,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/beta"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+4)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+12)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+4)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+4)),
				embedded.WithGracefulShutdownDuration(2*time.Second),
			)...,
		)).
		Step("wait", flow.Sleep(5*time.Second)).
		Step("signal subscribed", flow.MustDo(func() {
			close(subscribed)
		})).
		Step("send and wait", test).
		// Simulate a network interruption.
		// This tests the components ability to handle reconnections
		// when Dapr is disconnected abnormally.
		StepAsync("steady flow of messages to publish", &task, sendMessagesInBackground(alpha)).
		Step("wait", flow.Sleep(5*time.Second)).
		//
		// Errors will occurring here.
		//Step("interrupt network", network.InterruptNetwork(30*time.Second, nil, nil, "5672")).
		//
		// Component should recover at this point.
		Step("wait", flow.Sleep(30*time.Second)).
		Step("assert messages", assertMessages(alpha)).
		Run()
}

func TestRabbitMQTTL(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	log := logger.NewLogger("dapr.components")
	// log.SetOutputLevel(logger.DebugLevel)

	MessageOnlyMessages, QueueOnlyMessages, OverwriteMessages := watcher.NewUnordered(), watcher.NewUnordered(), watcher.NewUnordered()
	fullMessages := watcher.NewUnordered()
	// Application logic that tracks messages from a topic.
	application := func(pubsubName, topic string, w *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) (err error) {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, errFrequency)
			return s.AddTopicEventHandler(&common.Subscription{
				PubsubName: pubsubName,
				Topic:      topic,
				Route:      "/" + topic,
			}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
				if err := sim(); err != nil {
					log.Debugf("Simulated error - pubsub: %s, topic: %s, id: %s, data: %s", e.PubsubName, e.Topic, e.ID, e.Data)
					return true, err
				}

				// Track/Observe the data of the event.
				w.Observe(e.Data)
				log.Debugf("Event - pubsub: %s, topic: %s, id: %s, data: %s", e.PubsubName, e.Topic, e.ID, e.Data)
				return false, nil
			})
		}
	}

	sendMessage := func(sidecarName, pubsubName, topic string, ttlInSecond int) func(ctx flow.Context) error {
		fullMessages.Reset()
		return func(ctx flow.Context) error {
			// Declare what is expected BEFORE performing any steps
			// that will satisfy the test.
			msgs := make([]string, numMessages)
			for i := range msgs {
				msgs[i] = fmt.Sprintf("Hello, Messages %03d", i)
			}

			sc := sidecar.GetClient(ctx, sidecarName)

			// Send events that the application above will observe.
			log.Infof("Sending messages on topic '%s'", topic)

			var err error
			for _, msg := range msgs {
				log.Debugf("Sending: '%s' on topic '%s'", msg, topic)
				if ttlInSecond > 0 {
					err = sc.PublishEvent(ctx, pubsubName, topic, msg, daprClient.PublishEventWithMetadata(map[string]string{"ttlInSecond": strconv.Itoa(ttlInSecond)}))
				} else {
					err = sc.PublishEvent(ctx, pubsubName, topic, msg)
				}
				require.NoError(ctx, err, "error publishing message")
				fullMessages.Add(msg)
				fullMessages.Prepare(msg)
			}

			return nil
		}
	}

	flow.New(t, "rabbitmq ttl certification").
		// Run RabbitMQ using Docker Compose.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for rabbitmq readiness",
			retry.Do(time.Second, 30, amqpReady(rabbitMQURL))).
		// Start dapr and app to precreate all 3 queue in rabbitmq,
		// if topic is not subscribed, then the message will be lost.
		// Sidecar will block to wait app, so we need to start app first.
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+1),
			application(pubsubMessageOnlyTTL, topicTTL1, fullMessages))).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/ttl"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+1)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+10)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+1)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+1)),
				embedded.WithGracefulShutdownDuration(2*time.Second),
			)...,
		)).
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+2),
			application(pubsubQueueOnlyTTL, topicTTL2, QueueOnlyMessages))).
		Step(sidecar.Run(sidecarName2,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/ttl"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+2)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+11)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+2)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+2)),
				embedded.WithGracefulShutdownDuration(2*time.Second),
			)...,
		)).
		Step(app.Run(appID3, fmt.Sprintf(":%d", appPort+4),
			application(pubsubOverwriteTTL, topicTTL3, OverwriteMessages))).
		Step(sidecar.Run(sidecarName3,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/ttl"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+4)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+12)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+4)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+4)),
				embedded.WithGracefulShutdownDuration(2*time.Second),
			)...,
		)).
		// Wait for all queue crated and then stop.
		Step("wait", flow.Sleep(10*time.Second)).
		Step("stop sidecar1", sidecar.Stop(sidecarName1)).
		Step("stop app1", app.Stop(appID1)).
		Step("stop sidecar2", sidecar.Stop(sidecarName2)).
		Step("stop app2", app.Stop(appID2)).
		Step("stop sidecar3", sidecar.Stop(sidecarName3)).
		Step("stop app3", app.Stop(appID3)).
		// Run publishing sidecars and send to RabbitMQ.
		Step(sidecar.Run(sidecarNameTTLClient,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/ttl"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, "0"),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+14)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort)),
				embedded.WithGracefulShutdownDuration(2*time.Second),
			)...,
		)).
		Step("wait", flow.Sleep(5*time.Second)).
		// base test case, send message with large ttl and check messages are received.
		Step("send message only ttl messages", sendMessage(sidecarNameTTLClient, pubsubMessageOnlyTTL, topicTTL1, 100)).
		Step("wait", flow.Sleep(5*time.Second)).
		// Run the application1 logic above.
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+1),
			application(pubsubMessageOnlyTTL, topicTTL1, fullMessages))).
		// Run the Dapr sidecar with the RabbitMQ component.
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/ttl"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+1)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+10)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+1)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+1)),
				embedded.WithGracefulShutdownDuration(2*time.Second),
			)...,
		)).
		Step("verify full messages", func(ctx flow.Context) error {
			// Assertion on the data.
			fullMessages.Assert(t, 3*time.Minute)
			return nil
		}).
		Step("stop sidecar", sidecar.Stop(sidecarName1)).
		Step("stop app", app.Stop(appID1)).
		// test case 1, send message with ttl and check messages are expired.
		Step("send message only ttl messages", sendMessage(sidecarNameTTLClient, pubsubMessageOnlyTTL, topicTTL1, 10)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+1),
			application(pubsubMessageOnlyTTL, topicTTL1, MessageOnlyMessages))).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/ttl"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+1)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+10)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+1)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+1)),
				embedded.WithGracefulShutdownDuration(2*time.Second),
			)...,
		)).
		Step("verify messages only ttl", func(ctx flow.Context) error {
			// Assertion on the data.
			MessageOnlyMessages.Assert(t, 3*time.Minute)
			return nil
		}).
		// test case 2, send message without ttl to a queue with ttl, check messages are expired.
		Step("send message without ttl to queue with ttl", sendMessage(sidecarNameTTLClient, pubsubQueueOnlyTTL, topicTTL2, 0)).
		Step("wait", flow.Sleep(10*time.Second)).
		// Run the application2 logic above.
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+2),
			application(pubsubQueueOnlyTTL, topicTTL2, QueueOnlyMessages))).
		// Run the Dapr sidecar with the RabbitMQ component.
		Step(sidecar.Run(sidecarName2,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/ttl"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+2)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+11)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+2)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+2)),
				embedded.WithGracefulShutdownDuration(2*time.Second),
			)...,
		)).
		Step("verify messages only ttl", func(ctx flow.Context) error {
			// Assertion on the data.
			QueueOnlyMessages.Assert(t, 3*time.Minute)
			return nil
		}).
		Step("send message with ttl 10 to queue with ttl 30",
			sendMessage(sidecarNameTTLClient, pubsubOverwriteTTL, topicTTL3, 10)).
		Step("wait", flow.Sleep(10*time.Second)).
		// test case 3, send message with ttl 10s to a queue with ttl 30s, wait for 10s, check messages are expired.
		Step(app.Run(appID3, fmt.Sprintf(":%d", appPort+4),
			application(pubsubOverwriteTTL, topicTTL3, OverwriteMessages))).
		// Run the Dapr sidecar with the RabbitMQ component.
		Step(sidecar.Run(sidecarName3,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/ttl"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+4)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+12)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+4)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+4)),
				embedded.WithGracefulShutdownDuration(2*time.Second),
			)...,
		)).
		Step("verify messages only ttl", func(ctx flow.Context) error {
			// Assertion on the data.
			OverwriteMessages.Assert(t, 3*time.Minute)
			return nil
		}).
		Run()
}

func TestRabbitMQExtAuth(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	log := logger.NewLogger("dapr.components")
	// log.SetOutputLevel(logger.DebugLevel)

	pubTopics := []string{topicRed}
	subTopics := []string{topicRed}

	mtlsClient := &Consumer{pubsub: pubsubMtlsExternal, messages: make(map[string]*watcher.Watcher)}
	consumers := []*Consumer{mtlsClient}

	for _, consumer := range consumers {
		for _, topic := range pubTopics {
			// In RabbitMQ, messages might not come in order.
			consumer.messages[topic] = watcher.NewUnordered()
		}
	}

	// subscribed is used to synchronize between publisher and subscriber
	subscribed := make(chan struct{}, 1)

	// Test logic that sends messages to topics and
	// verifies the two consumers with different IDs have received them.
	test := func(ctx flow.Context) error {
		// Declare what is expected BEFORE performing any steps
		// that will satisfy the test.
		msgs := make([]string, numMessages)
		for i := range msgs {
			msgs[i] = fmt.Sprintf("Hello, Messages %03d", i)
		}

		for _, consumer := range consumers {
			for _, topic := range subTopics {
				consumer.messages[topic].ExpectStrings(msgs...)
			}
		}

		<-subscribed

		// sidecar client array []{sidecar client, pubsub component name}
		sidecars := []struct {
			client *sidecar.Client
			pubsub string
		}{
			{sidecar.GetClient(ctx, sidecarName1), pubsubMtlsExternal},
		}

		var wg sync.WaitGroup
		wg.Add(len(pubTopics))
		for _, topic := range pubTopics {
			go func(topic string) {
				defer wg.Done()

				// Send events that the application above will observe.
				log.Infof("Sending messages on topic '%s'", topic)

				for _, msg := range msgs {
					// randomize publishers
					indx := rand.Intn(len(sidecars))
					log.Debugf("Sending: '%s' on topic '%s'", msg, topic)
					err := sidecars[indx].client.PublishEvent(ctx, sidecars[indx].pubsub, topic, msg)
					require.NoError(ctx, err, "error publishing message")
				}
			}(topic)
		}
		wg.Wait()

		return nil
	}

	// Application logic that tracks messages from a topic.
	application := func(consumer *Consumer, routeIndex int) app.SetupFn {
		return func(ctx flow.Context, s common.Service) (err error) {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, errFrequency)

			for _, topic := range subTopics {
				// Setup the /orders event handler.
				err = multierr.Combine(
					err,
					s.AddTopicEventHandler(&common.Subscription{
						PubsubName: consumer.pubsub,
						Topic:      topic,
						Route:      fmt.Sprintf("/%s-%d", topic, routeIndex),
					}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
						if err := sim(); err != nil {
							log.Debugf("Simulated error - consumer: %s, pubsub: %s, topic: %s, id: %s, data: %s", consumer.pubsub, e.PubsubName, e.Topic, e.ID, e.Data)
							return true, err
						}

						// Track/Observe the data of the event.
						consumer.messages[e.Topic].Observe(e.Data)
						log.Debugf("Event - consumer: %s, pubsub: %s, topic: %s, id: %s, data: %s", consumer.pubsub, e.PubsubName, e.Topic, e.ID, e.Data)
						return false, nil
					}),
				)
			}
			return err
		}
	}

	flow.New(t, "rabbitmq mtls external authentication").
		// Run RabbitMQ using Docker Compose.
		Step(dockercompose.Run(clusterName, extSaslDockerComposeYAML)).
		Step("wait for rabbitmq readiness (external auth)",
			retry.Do(time.Second, 30, amqpMtlsExternalAuthReady(rabbitMQURLExtAuth))).
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			application(mtlsClient, 1))).
		// Run the Dapr sidecar with the RabbitMQ component.
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./mtls_sasl_external/components/mtls_external"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+10)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort)),
				embedded.WithGracefulShutdownDuration(2*time.Second),
			)...,
		)).
		Step("wait", flow.Sleep(5*time.Second)).
		Step("signal subscribed", flow.MustDo(func() {
			close(subscribed)
		})).
		Step("send and wait", test)
	// Disabled this test because the certificates have expired and it is unclear how to generate the new certs
	// Run()
}

func TestRabbitMQPriority(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	log := logger.NewLogger("dapr.components")
	// log.SetOutputLevel(logger.DebugLevel)

	pubTopics := []string{topicRed}
	subTopics := []string{topicRed}

	priorityClient := &Consumer{pubsub: pubsubPriority, messages: make(map[string]*watcher.Watcher)}
	consumers := []*Consumer{priorityClient}

	for _, consumer := range consumers {
		for _, topic := range pubTopics {
			// In RabbitMQ, messages might not come in order.
			consumer.messages[topic] = watcher.NewUnordered()
		}
	}

	// subscribed is used to synchronize between publisher and subscriber
	subscribed := make(chan struct{}, 1)

	// Test logic that sends messages to topics and
	// verifies the two consumers with different IDs have received them.
	test := func(ctx flow.Context) error {
		// Declare what is expected BEFORE performing any steps
		// that will satisfy the test.
		msgs := make([]string, numMessages)
		for i := range msgs {
			msgs[i] = fmt.Sprintf("Hello, Messages %03d", i)
		}

		for _, consumer := range consumers {
			for _, topic := range subTopics {
				consumer.messages[topic].ExpectStrings(msgs...)
			}
		}

		<-subscribed

		// sidecar client array []{sidecar client, pubsub component name}
		sidecars := []struct {
			client *sidecar.Client
			pubsub string
		}{
			{sidecar.GetClient(ctx, sidecarName4), pubsubPriority},
		}

		var wg sync.WaitGroup
		wg.Add(len(pubTopics))
		for _, topic := range pubTopics {
			go func(topic string) {
				defer wg.Done()

				// Send events that the application above will observe.
				log.Infof("Sending messages on topic '%s'", topic)

				for _, msg := range msgs {
					// randomize publishers
					indx := rand.Intn(len(sidecars))
					log.Debugf("Sending: '%s' on topic '%s'", msg, topic)
					err := sidecars[indx].client.PublishEvent(ctx, sidecars[indx].pubsub, topic, msg, daprClient.PublishEventWithMetadata(map[string]string{"priority": "1"}))
					require.NoError(ctx, err, "error publishing message")
				}
			}(topic)
		}
		wg.Wait()

		return nil
	}

	// Application logic that tracks messages from a topic.
	application := func(consumer *Consumer, routeIndex int) app.SetupFn {
		return func(ctx flow.Context, s common.Service) (err error) {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, errFrequency)

			for _, topic := range subTopics {
				// Setup the /orders event handler.
				err = multierr.Combine(
					err,
					s.AddTopicEventHandler(&common.Subscription{
						PubsubName: consumer.pubsub,
						Topic:      topic,
						Route:      fmt.Sprintf("/%s-%d", topic, routeIndex),
						Metadata:   map[string]string{"maxPriority": "1"},
					}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
						if err := sim(); err != nil {
							log.Debugf("Simulated error - consumer: %s, pubsub: %s, topic: %s, id: %s, data: %s", consumer.pubsub, e.PubsubName, e.Topic, e.ID, e.Data)
							return true, err
						}

						// Track/Observe the data of the event.
						consumer.messages[e.Topic].Observe(e.Data)
						log.Debugf("Event - consumer: %s, pubsub: %s, topic: %s, id: %s, data: %s", consumer.pubsub, e.PubsubName, e.Topic, e.ID, e.Data)
						return false, nil
					}),
				)
			}
			return err
		}
	}

	flow.New(t, "rabbitmq priority certification").
		// Run RabbitMQ using Docker Compose.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for rabbitmq readiness",
			retry.Do(time.Second, 30, amqpReady(rabbitMQURL))).
		// Start dapr and app to precreate all queues in rabbitmq,
		// if topic is not subscribed, then the message will be lost.
		// Sidecar will block to wait app, so we need to start app first.
		Step(app.Run(appID4, fmt.Sprintf(":%d", appPort+1),
			application(priorityClient, 1))).
		Step(sidecar.Run(sidecarName4,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/priority"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+1)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+10)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+1)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+1)),
				embedded.WithGracefulShutdownDuration(2*time.Second),
			)...,
		)).
		Step("signal subscribed", flow.MustDo(func() {
			close(subscribed)
		})).
		Step("send and wait", test).
		Run()
}

func getMetadataValueCI(metadata map[string]string, key string) (string, bool) {
	for k, v := range metadata {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return "", false
}

func TestRabbitMQMetadataProperties(t *testing.T) {
	messagesWatcher := watcher.NewUnordered()

	// Define the test values for metadata with fixed IDs
	const messageCount = 10
	const msgID = "msg-id-123"
	const corrID = "corr-id-456"
	const msgType = "test-type"
	const contentType = "application/json"

	messages := make([]string, messageCount)
	for i := range messageCount {
		messages[i] = fmt.Sprintf("Test message %d", i+1)
	}

	// Use a channel to collect metadata validation errors
	metadataErrors := make(chan error, 1)

	// Application logic that tracks messages with their metadata
	metadataApp := func(ctx flow.Context, s common.Service) (err error) {
		// Setup the topic event handler for metadata testing
		err = s.AddTopicEventHandler(&common.Subscription{
			PubsubName: pubsubMetadata,
			Topic:      topicMetadata,
			Route:      "/metadata",
			Metadata:   map[string]string{},
		}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
			// ENHANCED DEBUGGING - Print every detail of the message
			ctx.Logf("==================== MESSAGE RECEIVED ====================")
			ctx.Logf("Topic: %s", e.Topic)
			ctx.Logf("PubsubName: %s", e.PubsubName)
			ctx.Logf("ID: %s", e.ID)
			ctx.Logf("Data Type: %T", e.Data)
			ctx.Logf("Data: %s", e.Data)
			ctx.Logf("---------------- METADATA DUMP ----------------")
			ctx.Logf("Total metadata entries: %d", len(e.Metadata))
			ctx.Logf("Raw metadata map: %+v", e.Metadata)

			ctx.Logf("---------------- TARGET METADATA VALUES ----------------")
			msgIdVal, _ := getMetadataValueCI(e.Metadata, "messageid")
			corrIdVal, _ := getMetadataValueCI(e.Metadata, "correlationid")
			contentTypeVal, _ := getMetadataValueCI(e.Metadata, "contenttype")
			typeVal, _ := getMetadataValueCI(e.Metadata, "type")

			ctx.Logf("  → messageID: '%s' (expected: '%s')", msgIdVal, msgID)
			ctx.Logf("  → correlationID: '%s' (expected: '%s')", corrIdVal, corrID)
			ctx.Logf("  → contentType: '%s' (expected: '%s')", contentTypeVal, contentType)
			ctx.Logf("  → type: '%s' (expected: '%s')", typeVal, msgType)

			// Instead of failing silently, collect errors and send them to the channel
			var metadataErr error

			if msgIdVal != msgID {
				ctx.Logf("ERROR: messageID not found or incorrect: value=%s", msgIdVal)
				metadataErr = fmt.Errorf("expected messageID: %s, got: %s", msgID, msgIdVal)
			}

			if corrIdVal != corrID {
				ctx.Logf("ERROR: correlationID not found or incorrect: value=%s", corrIdVal)
				if metadataErr != nil {
					metadataErr = fmt.Errorf("%w; expected correlationID: %s, got: %s",
						metadataErr, corrID, corrIdVal)
				} else {
					metadataErr = fmt.Errorf("expected correlationID: %s, got: %s", corrID, corrIdVal)
				}
			}

			if contentTypeVal != contentType {
				ctx.Logf("ERROR: contentType not found or incorrect: value=%s", contentTypeVal)
				if metadataErr != nil {
					metadataErr = fmt.Errorf("%w; expected contentType: %s, got: %s",
						metadataErr, contentType, contentTypeVal)
				} else {
					metadataErr = fmt.Errorf("expected contentType: %s, got: %s", contentType, contentTypeVal)
				}
			}

			if typeVal != msgType {
				ctx.Logf("ERROR: type not found or incorrect: value=%s", typeVal)
				if metadataErr != nil {
					metadataErr = fmt.Errorf("%w; expected type: %s, got: %s",
						metadataErr, msgType, typeVal)
				} else {
					metadataErr = fmt.Errorf("expected type: %s, got: %s", msgType, typeVal)
				}
			}

			dataStr, ok := e.Data.(string)
			if !ok {
				return false, fmt.Errorf("e.Data is not a string, got %T", e.Data)
			}
			// If there are any metadata errors, send them to the channel
			if metadataErr != nil {
				ctx.Logf("Metadata validation failed: %s", metadataErr)
				metadataErrors <- metadataErr
			}

			messagesWatcher.Observe(dataStr)
			ctx.Logf("Got message: %s with all expected metadata properties", e.Data)
			return false, nil
		})

		return err
	}

	// Test function to publish messages with metadata
	testMetadata := func(ctx flow.Context) error {
		// Get the Dapr client
		client := sidecar.GetClient(ctx, sidecarNameMetadata)
		messagesWatcher.ExpectStrings(messages...)

		// Publish messages with metadata properties
		ctx.Log("Publishing messages with metadata properties")
		for i := 0; i < messageCount; i++ {
			err := client.PublishEvent(ctx, pubsubMetadata, topicMetadata, messages[i],
				daprClient.PublishEventWithMetadata(map[string]string{
					"messageID":     msgID,
					"correlationID": corrID,
					"contentType":   contentType,
					"type":          msgType,
				}))
			require.NoError(ctx, err, "Failed publishing message with metadata")
		}

		// Check for metadata errors with timeout
		select {
		case err := <-metadataErrors:
			return fmt.Errorf("metadata validation failed: %w", err)
		case <-time.After(5 * time.Second):
			// No errors within timeout, continue with message assertion
		}

		// Verify all messages were processed correctly
		ctx.Log("Verifying messages were received...")
		messagesWatcher.Assert(t, 20*time.Second)

		return nil
	}

	// Run the test flow
	flow.New(t, "rabbitmq metadata properties pubsub certification").
		// Start RabbitMQ container
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for rabbitmq readiness", retry.Do(time.Second, 30, amqpReady(rabbitMQURL))).
		// Run the metadata app and sidecar
		Step(app.Run(appIDMetadata, fmt.Sprintf(":%d", appPort+10), metadataApp)).
		Step(sidecar.Run(sidecarNameMetadata,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/metadata"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+10)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+20)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+10)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+10)),
				embedded.WithGracefulShutdownDuration(2*time.Second),
			)...,
		)).
		// Wait for subscription to complete
		Step("wait for subscription setup", flow.Sleep(5*time.Second)).
		// Run the test with timeout
		Step("publish and verify metadata properties", testMetadata).
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	pubsubRegistry := pubsub_loader.NewRegistry()
	pubsubRegistry.Logger = log
	pubsubRegistry.RegisterComponent(pubsub_rabbitmq.NewRabbitMQ, "rabbitmq")

	return []embedded.Option{
		embedded.WithPubSubs(pubsubRegistry),
	}
}
