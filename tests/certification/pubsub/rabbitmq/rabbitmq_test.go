// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rabbitmq_test

import (
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub/Sub.
	"github.com/dapr/components-contrib/pubsub"
	pubsub_rabbitmq "github.com/dapr/components-contrib/pubsub/rabbitmq"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/simulate"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
)

const (
	sidecarName       = "dapr-1"
	applicationID     = "app-1"
	clusterName       = "rabbitmqcertification"
	dockerComposeYAML = "docker-compose.yml"
	numMessages       = 1000
	errFrequency      = 100
	appPort           = 8000

	pubsubAlpha = "mq-alpha"
	pubsubBeta  = "mq-beta"

	topicRed   = "red"
	topicBlue  = "blue"
	topicGreen = "green"
)

type consumer struct {
	pubsub   string
	messages map[string]*watcher.Watcher
}

func rabbitmqStatus(c flow.Context) error {
	ctx, cancel := context.WithTimeout(c, time.Minute)
	defer cancel()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			cmd := exec.Command("docker", "exec", "rabbitmq", "rabbitmq-diagnostics", "status")
			_, err := cmd.CombinedOutput()
			if err == nil {
				return nil
			}
		}
	}
}

func daprStatus(c flow.Context) error {
	url := fmt.Sprintf("http://localhost:%d/v1.0/healthz", runtime.DefaultDaprHTTPPort)

	ctx, cancel := context.WithTimeout(c, time.Minute)
	defer cancel()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			resp, err := http.Get(url)
			if err == nil && resp.StatusCode == http.StatusNoContent {
				return nil
			}
		}
	}
}

func TestSingleTopicSingleConsumer(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	// In RabbitMQ, messages might not come in order.
	messages := watcher.NewUnordered()

	// subscribed is used to synchronize between publisher and subscriber
	subscribed := make(chan struct{}, 1)

	// Test logic that sends messages to a topic and
	// verifies the application has received them.
	test := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName)

		// Declare what is expected BEFORE performing any steps
		// that will satisfy the test.
		msgs := make([]string, numMessages)
		for i := range msgs {
			msgs[i] = fmt.Sprintf("Hello, Messages %03d", i)
		}
		messages.ExpectStrings(msgs...)
		<-subscribed
		// Send events that the application above will observe.
		ctx.Log("Sending messages!")
		for _, msg := range msgs {
			ctx.Logf("Sending: %q", msg)
			err := client.PublishEvent(
				ctx, pubsubAlpha, topicRed, msg)
			require.NoError(ctx, err, "error publishing message")
		}

		// Do the messages we observed match what we expect?
		messages.Assert(ctx, time.Minute)

		return nil
	}

	// Application logic that tracks messages from a topic.
	application := func(ctx flow.Context, s common.Service) (err error) {
		defer func() {
			subscribed <- struct{}{}
		}()
		// Simulate periodic errors.
		sim := simulate.PeriodicError(ctx, errFrequency)

		// Setup topic event handler.
		err = multierr.Append(err,
			s.AddTopicEventHandler(&common.Subscription{
				PubsubName: pubsubAlpha,
				Topic:      topicRed,
				Route:      "/" + topicRed,
			}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
				if err := sim(); err != nil {
					return true, err
				}

				// Track/Observe the data of the event.
				messages.Observe(e.Data)
				ctx.Logf("Event - pubsub: %s, topic: %s, id: %s, data: %s",
					e.PubsubName, e.Topic, e.ID, e.Data)
				return false, nil
			}))

		return err
	}

	flow.New(t, "rabbitmq certification").
		// Run RabbitMQ using Docker Compose.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		//Step("rabbitmq up", rabbitmqStatus).
		// Run the application logic above.
		Step(app.Run(applicationID, fmt.Sprintf(":%d", appPort), application)).
		// Run the Dapr sidecar with the RabbitMQ component.
		Step(sidecar.Run(sidecarName,
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithPubSubs(
				pubsub_loader.New("rabbitmq", func() pubsub.PubSub {
					return pubsub_rabbitmq.NewRabbitMQ(log)
				}),
			))).
		//Step("dapr up", daprStatus).
		Step("dapr up", flow.Sleep(15*time.Second)).
		Step("send and wait", test).
		Run()
}

func TestMultiTopicSingleConsumer(t *testing.T) {
	log := logger.NewLogger("dapr.components")

	topics := []string{topicRed, topicBlue, topicGreen}

	messages := make(map[string]*watcher.Watcher)
	for _, topic := range topics {
		// In RabbitMQ, messages might not come in order.
		messages[topic] = watcher.NewUnordered()
	}

	// subscribed is used to synchronize between publisher and subscriber
	subscribed := make(chan struct{}, 1)

	// Test logic that sends messages to a topic and
	// verifies the application has received them.
	test := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName)

		// Declare what is expected BEFORE performing any steps
		// that will satisfy the test.
		msgs := make([]string, numMessages)
		for i := range msgs {
			msgs[i] = fmt.Sprintf("Hello, Messages %03d", i)
		}
		<-subscribed

		// expecting no messages in topicGreen
		messages[topicGreen].ExpectStrings()

		var wg sync.WaitGroup
		wg.Add(2)
		for _, topic := range []string{topicRed, topicBlue} {
			go func(topic string) {
				defer wg.Done()
				messages[topic].ExpectStrings(msgs...)

				// Send events that the application above will observe.
				ctx.Log("Sending messages!")
				for _, msg := range msgs {
					ctx.Logf("Sending: %q to topic %q", msg, topic)
					err := client.PublishEvent(ctx, pubsubAlpha, topic, msg)
					require.NoError(ctx, err, "error publishing message")
				}

				// Do the messages we observed match what we expect?
				messages[topic].Assert(ctx, time.Minute)
			}(topic)
		}
		wg.Wait()
		messages[topicGreen].Assert(ctx, time.Second)
		return nil
	}

	// Application logic that tracks messages from a topic.
	application := func(ctx flow.Context, s common.Service) (err error) {
		defer func() {
			subscribed <- struct{}{}
		}()

		for _, topic := range topics {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, errFrequency)

			// Setup topic event handler.
			err = multierr.Append(err,
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubAlpha,
					Topic:      topic,
					Route:      "/" + topic,
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						return true, err
					}

					// Track/Observe the data of the event.
					messages[e.Topic].Observe(e.Data)
					ctx.Logf("Event - pubsub: %s, topic: %s, id: %s, data: %s",
						e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}))
		}
		return err
	}

	flow.New(t, "rabbitmq certification").
		// Run RabbitMQ using Docker Compose.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		//Step("rabbitmq up", rabbitmqStatus).
		// Run the application logic above.
		Step(app.Run(applicationID, fmt.Sprintf(":%d", appPort), application)).
		// Run the Dapr sidecar with the RabbitMQ component.
		Step(sidecar.Run(sidecarName,
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithPubSubs(
				pubsub_loader.New("rabbitmq", func() pubsub.PubSub {
					return pubsub_rabbitmq.NewRabbitMQ(log)
				}),
			))).
		//Step("dapr up", daprStatus).
		Step("dapr up", flow.Sleep(15*time.Second)).
		Step("send and wait", test).
		Run()
}

func TestMultiTopicMuliConsumer(t *testing.T) {
	log := logger.NewLogger("dapr.components")

	topics := []string{topicRed, topicBlue, topicGreen}

	alpha := &consumer{pubsub: pubsubAlpha, messages: make(map[string]*watcher.Watcher)}
	beta := &consumer{pubsub: pubsubBeta, messages: make(map[string]*watcher.Watcher)}
	for _, topic := range topics {
		// In RabbitMQ, messages might not come in order.
		alpha.messages[topic] = watcher.NewUnordered()
		beta.messages[topic] = watcher.NewUnordered()
	}

	// subscribed is used to synchronize between publisher and subscriber
	subscribed := make(chan struct{}, 1)

	// Test logic that sends messages to a topic and
	// verifies the application has received them.
	test := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName)

		// Declare what is expected BEFORE performing any steps
		// that will satisfy the test.
		msgs := make([]string, numMessages)
		for i := range msgs {
			msgs[i] = fmt.Sprintf("Hello, Messages %03d", i)
		}
		<-subscribed

		// expecting no messages in topicGrey
		alpha.messages[topicGreen].ExpectStrings()
		beta.messages[topicGreen].ExpectStrings()

		var wg sync.WaitGroup
		wg.Add(2)
		for _, topic := range []string{topicRed, topicBlue} {
			go func(topic string) {
				defer wg.Done()
				alpha.messages[topic].ExpectStrings(msgs...)
				beta.messages[topic].ExpectStrings(msgs...)

				// Send events that the application above will observe.
				ctx.Log("Sending messages!")
				for i, msg := range msgs {
					// alternate publisher
					pubsub := pubsubAlpha
					if i%3 == 0 {
						pubsub = pubsubBeta
					}
					ctx.Logf("Sending: %q to topic %q", msg, topic)
					err := client.PublishEvent(ctx, pubsub, topic, msg)
					require.NoError(ctx, err, "error publishing message")
				}

				// Do the messages we observed match what we expect?
				alpha.messages[topic].Assert(ctx, time.Minute)
				beta.messages[topic].Assert(ctx, time.Second)
			}(topic)
		}
		wg.Wait()
		alpha.messages[topicGreen].Assert(ctx, time.Second)
		beta.messages[topicGreen].Assert(ctx, time.Second)
		return nil
	}

	// Application logic that tracks messages from a topic.
	application := func(ctx flow.Context, s common.Service) (err error) {
		defer func() {
			subscribed <- struct{}{}
		}()

		for _, topic := range topics {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, errFrequency)
			// Setup topic event handler.
			err = multierr.Append(err,
				s.AddTopicEventHandler(
					&common.Subscription{
						PubsubName: alpha.pubsub,
						Topic:      topic,
						Route:      fmt.Sprintf("/%s-alpha", topic),
					},
					eventHandler(ctx, alpha, topic, sim),
				),
			)
			err = multierr.Append(err,
				s.AddTopicEventHandler(
					&common.Subscription{
						PubsubName: beta.pubsub,
						Topic:      topic,
						Route:      fmt.Sprintf("/%s-beta1", topic),
					},
					eventHandler(ctx, beta, topic, sim),
				),
			)
			err = multierr.Append(err,
				s.AddTopicEventHandler(
					&common.Subscription{
						PubsubName: beta.pubsub,
						Topic:      topic,
						Route:      fmt.Sprintf("/%s-beta2", topic),
					},
					eventHandler(ctx, beta, topic, sim),
				),
			)
		}
		return err
	}

	flow.New(t, "rabbitmq certification").
		// Run RabbitMQ using Docker Compose.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		//Step("rabbitmq up", rabbitmqStatus).
		// Run the application logic above.
		Step(app.Run(applicationID, fmt.Sprintf(":%d", appPort), application)).
		// Run the Dapr sidecar with the RabbitMQ component.
		Step(sidecar.Run(sidecarName,
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithPubSubs(
				pubsub_loader.New("rabbitmq", func() pubsub.PubSub {
					return pubsub_rabbitmq.NewRabbitMQ(log)
				}),
			))).
		//Step("dapr up", daprStatus).
		Step("dapr up", flow.Sleep(15*time.Second)).
		Step("send and wait", test).
		Run()
}

func eventHandler(ctx flow.Context, cons *consumer, topic string, sim func() error) func(context.Context, *common.TopicEvent) (bool, error) {
	return func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
		if err := sim(); err != nil {
			return true, err
		}

		// Track/Observe the data of the event.
		cons.messages[topic].Observe(e.Data)
		ctx.Logf("Event - consumer: %s, pubsub: %s, topic: %s, id: %s, data: %s",
			cons.pubsub, e.PubsubName, e.Topic, e.ID, e.Data)
		return false, nil
	}
}
