/*
Copyright 2023 The Dapr Authors
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

package pulsar_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/multierr"

	"github.com/dapr/components-contrib/common/authentication/oauth2"
	pubsub_pulsar "github.com/dapr/components-contrib/pubsub/pulsar"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	"github.com/dapr/dapr/pkg/config/protocol"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/dapr/dapr/pkg/runtime"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/simulate"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
)

const (
	sidecarName1 = "dapr-1"
	sidecarName2 = "dapr-2"

	appID1 = "app-1"
	appID2 = "app-2"

	numMessages                 = 10
	appPort                     = 8001
	portOffset                  = 2
	messageKey                  = "partitionKey"
	pubsubName                  = "messagebus"
	topicActiveName             = "certification-pubsub-topic-active"
	topicPassiveName            = "certification-pubsub-topic-passive"
	topicToBeCreated            = "certification-topic-per-test-run"
	topicDefaultName            = "certification-topic-default"
	topicMultiPartitionName     = "certification-topic-multi-partition8"
	partition0                  = "partition-0"
	partition1                  = "partition-1"
	clusterName                 = "pulsarcertification"
	dockerComposeAuthNoneYAML   = "./config/docker-compose_auth-none.yaml"
	dockerComposeAuthOAuth2YAML = "./config/docker-compose_auth-oauth2.yaml.tmpl"
	dockerComposeMockOAuth2YAML = "./config/docker-compose_auth-mock-oauth2-server.yaml"
	pulsarURL                   = "localhost:6650"

	subscribeTypeKey = "subscribeType"

	subscribeTypeExclusive = "exclusive"
	subscribeTypeShared    = "shared"
	subscribeTypeFailover  = "failover"
	subscribeTypeKeyShared = "key_shared"

	processModeKey   = "processMode"
	processModeAsync = "async"
	processModeSync  = "sync"
)

type pulsarSuite struct {
	suite.Suite

	authType          string
	oauth2CAPEM       []byte
	dockerComposeYAML string
	componentsPath    string
	services          []string
}

func TestPulsar(t *testing.T) {
	t.Run("Auth:None", func(t *testing.T) {
		suite.Run(t, &pulsarSuite{
			authType:          "none",
			dockerComposeYAML: dockerComposeAuthNoneYAML,
			componentsPath:    "./components/auth-none",
			services:          []string{"standalone"},
		})
	})

	t.Run("Auth:OAuth2", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.Chmod(dir, 0o777))

		t.Log("Starting OAuth2 server...")
		out, err := exec.Command(
			"docker", "compose",
			"-p", "oauth2",
			"-f", dockerComposeMockOAuth2YAML,
			"up", "-d").CombinedOutput()
		require.NoError(t, err, string(out))
		t.Log(string(out))

		t.Cleanup(func() {
			t.Log("Stopping OAuth2 server...")
			out, err = exec.Command(
				"docker", "compose",
				"-p", "oauth2",
				"-f", dockerComposeMockOAuth2YAML,
				"down", "-v",
				"--remove-orphans").CombinedOutput()
			require.NoError(t, err, string(out))
			t.Log(string(out))
		})

		t.Log("Waiting for OAuth server to be ready...")
		oauth2CA := peerCertificate(t, "localhost:8085")
		t.Log("OAuth server is ready")

		require.NoError(t, os.WriteFile(filepath.Join(dir, "ca.pem"), oauth2CA, 0o644))
		outf, err := os.OpenFile("./config/pulsar_auth-oauth2.conf", os.O_RDONLY, 0o644)
		require.NoError(t, err)
		inf, err := os.OpenFile(filepath.Join(dir, "pulsar_auth-oauth2.conf"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
		require.NoError(t, err)
		_, err = io.Copy(inf, outf)
		require.NoError(t, err)
		outf.Close()
		inf.Close()

		td := struct {
			TmpDir      string
			OAuth2CAPEM string
		}{
			TmpDir:      dir,
			OAuth2CAPEM: strings.ReplaceAll(string(oauth2CA), "\n", "\\n"),
		}

		tmpl, err := template.New("").ParseFiles(dockerComposeAuthOAuth2YAML)
		require.NoError(t, err)
		f, err := os.OpenFile(filepath.Join(dir, "docker-compose.yaml"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
		require.NoError(t, err)
		require.NoError(t, tmpl.ExecuteTemplate(f, "docker-compose_auth-oauth2.yaml.tmpl", td))

		require.NoError(t, filepath.Walk("./components/auth-oauth2", func(path string, info fs.FileInfo, err error) error {
			if info.IsDir() {
				return nil
			}
			tmpl, err := template.New("").ParseFiles(path)
			require.NoError(t, err)
			path = strings.TrimSuffix(path, ".tmpl")
			require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(dir, path)), 0o755))
			f, err := os.OpenFile(filepath.Join(dir, path), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
			require.NoError(t, err)
			require.NoError(t, tmpl.ExecuteTemplate(f, filepath.Base(path)+".tmpl", td))
			return nil
		}))

		suite.Run(t, &pulsarSuite{
			oauth2CAPEM:       oauth2CA,
			authType:          "oauth2",
			dockerComposeYAML: filepath.Join(dir, "docker-compose.yaml"),
			componentsPath:    filepath.Join(dir, "components/auth-oauth2"),
			services:          []string{"zookeeper", "pulsar-init", "bookie", "broker"},
		})
	})
}

func subscriberApplication(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
	return func(ctx flow.Context, s common.Service) error {
		// Simulate periodic errors.
		sim := simulate.PeriodicError(ctx, 100)
		// Setup the /orders event handler.
		return multierr.Combine(
			s.AddTopicEventHandler(&common.Subscription{
				PubsubName: pubsubName,
				Topic:      topicName,
				Route:      "/orders",
			}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
				if err := sim(); err != nil {
					return true, err
				}

				// Track/Observe the data of the event.
				messagesWatcher.Observe(e.Data)
				ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
				return false, nil
			}),
		)
	}
}

func subscriberApplicationWithoutError(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
	return func(ctx flow.Context, s common.Service) error {
		// Setup the /orders event handler.
		return multierr.Combine(
			s.AddTopicEventHandler(&common.Subscription{
				PubsubName: pubsubName,
				Topic:      topicName,
				Route:      "/orders",
				Metadata: map[string]string{
					subscribeTypeKey: subscribeTypeKeyShared,
					processModeKey:   processModeSync,
				},
			}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
				// Track/Observe the data of the event.
				messagesWatcher.Observe(e.Data)
				ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
				return false, nil
			}),
		)
	}
}

func subscriberSchemaApplication(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
	return func(ctx flow.Context, s common.Service) error {
		// Setup the /orders event handler.
		return multierr.Combine(
			s.AddTopicEventHandler(&common.Subscription{
				PubsubName: pubsubName,
				Topic:      topicName,
				Route:      "/orders",
			}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
				// Track/Observe the data of the event.
				messagesWatcher.ObserveJSON(e.Data)
				ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
				return false, nil
			}),
		)
	}
}

func publishMessages(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
	return func(ctx flow.Context) error {
		// prepare the messages
		messages := make([]string, numMessages)
		for i := range messages {
			messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
		}

		for _, messageWatcher := range messageWatchers {
			messageWatcher.ExpectStrings(messages...)
		}

		// get the sidecar (dapr) client
		client := sidecar.GetClient(ctx, sidecarName)

		// publish messages
		ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

		var publishOptions dapr.PublishEventOption

		if metadata != nil {
			publishOptions = dapr.PublishEventWithMetadata(metadata)
		}

		for _, message := range messages {
			ctx.Logf("Publishing: %q", message)
			var err error

			if publishOptions != nil {
				err = client.PublishEvent(ctx, pubsubName, topicName, message, publishOptions)
			} else {
				err = client.PublishEvent(ctx, pubsubName, topicName, message)
			}
			require.NoError(ctx, err, "error publishing message")
		}
		return nil
	}
}

func assertMessages(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
	return func(ctx flow.Context) error {
		// assert for messages
		for _, m := range messageWatchers {
			m.Assert(ctx, 25*timeout)
		}

		return nil
	}
}

func (p *pulsarSuite) TestPulsar() {
	t := p.T()
	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
			}

			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, topicName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, topicName, message)
				}
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	flow.New(t, "pulsar certification basic test").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, p.dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := p.client(t)
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			return err
		})).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_one")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).

		// Run subscriberApplication app2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID2, topicActiveName, consumerGroup2))).

		// Run the Dapr sidecar with the component 2.
		Step(sidecar.Run(sidecarName2,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_two")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
			)...,
		)).
		Step("publish messages to topic1", publishMessages(nil, sidecarName1, topicActiveName, consumerGroup1, consumerGroup2)).
		Step("publish messages to unUsedTopic", publishMessages(nil, sidecarName1, topicPassiveName)).
		Step("verify if app1 has received messages published to active topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("verify if app2 has received messages published to passive topic", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

func (p *pulsarSuite) TestPulsarMultipleSubsSameConsumerIDs() {
	t := p.T()
	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	metadata := map[string]string{
		messageKey: partition0,
	}

	metadata1 := map[string]string{
		messageKey: partition1,
	}

	flow.New(t, "pulsar certification - single publisher and multiple subscribers with same consumer IDs").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, p.dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := p.client(t)
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			return err
		})).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_one")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).

		// Run subscriberApplication app2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID2, topicActiveName, consumerGroup2))).

		// Run the Dapr sidecar with the component 2.
		Step(sidecar.Run(sidecarName2,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_two")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
			)...,
		)).
		Step("publish messages to topic1", publishMessages(metadata, sidecarName1, topicActiveName, consumerGroup2)).
		Step("publish messages to topic1", publishMessages(metadata1, sidecarName2, topicActiveName, consumerGroup2)).
		Step("verify if app1, app2 together have received messages published to topic1", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

func (p *pulsarSuite) TestPulsarMultipleSubsDifferentConsumerIDs() {
	t := p.T()

	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	flow.New(t, "pulsar certification - single publisher and multiple subscribers with different consumer IDs").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, p.dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := p.client(t)
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			// Ensure the brokers are ready by attempting to consume
			// a topic partition.
			return err
		})).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_one")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).

		// Run subscriberApplication app2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID2, topicActiveName, consumerGroup2))).

		// Run the Dapr sidecar with the component 2.
		Step(sidecar.Run(sidecarName2,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_two")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
			)...,
		)).
		Step("publish messages to topic1", publishMessages(metadata, sidecarName1, topicActiveName, consumerGroup1)).
		Step("verify if app1, app2 together have received messages published to topic1", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

func (p *pulsarSuite) TestPulsarMultiplePubSubsDifferentConsumerIDs() {
	t := p.T()
	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	metadata1 := map[string]string{
		messageKey: partition1,
	}

	flow.New(t, "pulsar certification - multiple publishers and multiple subscribers with different consumer IDs").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, p.dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := p.client(t)
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			// Ensure the brokers are ready by attempting to consume
			// a topic partition.
			return err
		})).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_one")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).

		// Run subscriberApplication app2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID2, topicActiveName, consumerGroup2))).

		// Run the Dapr sidecar with the component 2.
		Step(sidecar.Run(sidecarName2,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_two")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
			)...,
		)).
		Step("publish messages to topic1", publishMessages(metadata, sidecarName1, topicActiveName, consumerGroup1)).
		Step("publish messages to topic1", publishMessages(metadata1, sidecarName2, topicActiveName, consumerGroup2)).
		Step("verify if app1, app2 together have received messages published to topic1", assertMessages(10*time.Second, consumerGroup1)).
		Step("verify if app1, app2 together have received messages published to topic1", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

func (p *pulsarSuite) TestPulsarNonexistingTopic() {
	t := p.T()
	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	flow.New(t, "pulsar certification - non-existing topic").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset*3),
			subscriberApplication(appID1, topicToBeCreated, consumerGroup1))).
		Step(dockercompose.Run(clusterName, p.dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := p.client(t)
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			// Ensure the brokers are ready by attempting to consume
			// a topic partition.
			return err
		})).
		// Run the Dapr sidecar with the component entitymanagement
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_one")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset*3)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset*3)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset*3)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset*3)),
			)...,
		)).
		Step(fmt.Sprintf("publish messages to topicToBeCreated: %s", topicToBeCreated), publishMessages(metadata, sidecarName1, topicToBeCreated, consumerGroup1)).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("verify if app1 has received messages published to newly created topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

func (p *pulsarSuite) TestPulsarNetworkInterruption() {
	t := p.T()
	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	flow.New(t, "pulsar certification - network interruption").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, p.dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := p.client(t)
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			// Ensure the brokers are ready by attempting to consume
			// a topic partition.
			return err
		})).
		// Run the Dapr sidecar with the component entitymanagement
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_one")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
			)...,
		)).
		Step(fmt.Sprintf("publish messages to topicToBeCreated: %s", topicActiveName), publishMessages(metadata, sidecarName1, topicActiveName, consumerGroup1)).
		Step("interrupt network", network.InterruptNetwork(30*time.Second, nil, nil, "6650")).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("verify if app1 has received messages published to newly created topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

func (p *pulsarSuite) TestPulsarPersitant() {
	t := p.T()
	consumerGroup1 := watcher.NewUnordered()

	flow.New(t, "pulsar certification persistant test").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, p.dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := p.client(t)
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			// Ensure the brokers are ready by attempting to consume
			// a topic partition.
			return err
		})).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_one")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
				embedded.WithGracefulShutdownDuration(time.Second*20),
			)...,
		)).
		Step("publish messages to topic1", publishMessages(nil, sidecarName1, topicActiveName, consumerGroup1)).
		Step("stop pulsar server", dockercompose.Stop(clusterName, p.dockerComposeYAML, p.services...)).
		Step("wait", flow.Sleep(5*time.Second)).
		Step("start pulsar server", dockercompose.Start(clusterName, p.dockerComposeYAML, p.services...)).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("verify if app1 has received messages published to active topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}

func (p *pulsarSuite) TestPulsarDelay() {
	t := p.T()
	consumerGroup1 := watcher.NewUnordered()

	date := time.Now()
	deliverTime := date.Add(time.Second * 60)

	metadataAfter := map[string]string{
		"deliverAfter": "30s",
	}

	metadataAt := map[string]string{
		"deliverAt": deliverTime.Format(time.RFC3339Nano),
	}

	assertMessagesNot := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.AssertNotDelivered(ctx, 5*timeout)
			}

			return nil
		}
	}

	flow.New(t, "pulsar certification delay test").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, p.dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := p.client(t)
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			// Ensure the brokers are ready by attempting to consume
			// a topic partition.
			return err
		})).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_three")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish messages to topic1", publishMessages(metadataAfter, sidecarName1, topicActiveName, consumerGroup1)).
		// receive no messages due to deliverAfter delay
		Step("verify if app1 has received no messages published to topic", assertMessagesNot(1*time.Second, consumerGroup1)).
		// delay has passed, messages should be received
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		// publish messages using deliverAt property
		Step("publish messages to topic1", publishMessages(metadataAt, sidecarName1, topicActiveName, consumerGroup1)).
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

type schemaTest struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func (p *pulsarSuite) TestPulsarSchema() {
	t := p.T()
	consumerGroup1 := watcher.NewUnordered()

	publishMessages := func(sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				test := &schemaTest{
					ID:   i,
					Name: uuid.New().String(),
				}

				b, _ := json.Marshal(test)
				messages[i] = string(b)
			}

			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)

				err := client.PublishEvent(ctx, pubsubName, topicName, message)
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	flow.New(t, "pulsar certification schema test").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberSchemaApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, p.dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := p.client(t)
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			return err
		})).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_four")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish messages to topic1", publishMessages(sidecarName1, topicActiveName, consumerGroup1)).
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	pubsubRegistry := pubsub_loader.NewRegistry()
	pubsubRegistry.Logger = log
	pubsubRegistry.RegisterComponent(pubsub_pulsar.NewPulsar, "pulsar")

	return []embedded.Option{
		embedded.WithPubSubs(pubsubRegistry),
	}
}

func (p *pulsarSuite) createMultiPartitionTopic(tenant, namespace, topic string, partition int) flow.Runnable {
	return func(ctx flow.Context) error {
		reqURL := fmt.Sprintf("http://localhost:8080/admin/v2/persistent/%s/%s/%s/partitions",
			tenant, namespace, topic)

		reqBody, err := json.Marshal(partition)

		if err != nil {
			return fmt.Errorf("createMultiPartitionTopic json.Marshal(%d) err: %s", partition, err.Error())
		}

		req, err := http.NewRequest(http.MethodPut, reqURL, bytes.NewBuffer(reqBody))

		if err != nil {
			return fmt.Errorf("createMultiPartitionTopic NewRequest(url: %s, body: %s) err:%s",
				reqURL, reqBody, err.Error())
		}

		req.Header.Set("Content-Type", "application/json")

		if p.authType == "oauth2" {
			cc, err := p.oauth2ClientCredentials()
			if err != nil {
				return err
			}
			token, err := cc.Token()
			if err != nil {
				return err
			}

			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		}

		rsp, err := http.DefaultClient.Do(req)

		if err != nil {
			return fmt.Errorf("createMultiPartitionTopic(url: %s, body: %s) err:%s",
				reqURL, reqBody, err.Error())
		}

		defer rsp.Body.Close()

		if rsp.StatusCode >= http.StatusOK && rsp.StatusCode <= http.StatusMultipleChoices {
			return nil
		}

		rspBody, _ := ioutil.ReadAll(rsp.Body)

		return fmt.Errorf("createMultiPartitionTopic(url: %s, body: %s) statusCode: %d, resBody: %s",
			reqURL, reqBody, rsp.StatusCode, string(rspBody))
	}
}

func (p *pulsarSuite) TestPulsarPartitionedOrderingProcess() {
	t := p.T()
	consumerGroup1 := watcher.NewOrdered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	flow.New(t, "pulsar certification -  process message in order with partitioned-topic").
		Step(dockercompose.Run(clusterName, p.dockerComposeYAML)).

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplicationWithoutError(appID1, topicMultiPartitionName, consumerGroup1))).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := p.client(t)
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}

			defer consumer.Close()

			// Ensure the brokers are ready by attempting to consume
			// a topic partition.
			return err
		})).
		Step("create multi-partition topic explicitly", retry.Do(10*time.Second, 30,
			p.createMultiPartitionTopic("public", "default", topicMultiPartitionName, 4))).
		// Run the Dapr sidecar with the component entitymanagement
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_one")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
			)...,
		)).
		// Run subscriberApplication app2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset*3),
			subscriberApplicationWithoutError(appID2, topicActiveName, consumerGroup1))).

		// Run the Dapr sidecar with the component 2.
		Step(sidecar.Run(sidecarName2,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_two")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset*3)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset*3)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset*3)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset*3)),
			)...,
		)).
		Step(fmt.Sprintf("publish messages to topicToBeCreated: %s", topicMultiPartitionName), publishMessages(metadata, sidecarName1, topicMultiPartitionName, consumerGroup1)).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("verify if app1 has received messages published to newly created topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}

func (p *pulsarSuite) TestPulsarEncryptionFromFile() {
	t := p.T()
	consumerGroup1 := watcher.NewUnordered()

	publishMessages := func(sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				test := &schemaTest{
					ID:   i,
					Name: uuid.New().String(),
				}

				b, _ := json.Marshal(test)
				messages[i] = string(b)
			}

			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)

				err := client.PublishEvent(ctx, pubsubName, topicName, message)
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	flow.New(t, "pulsar encryption test with file path").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberSchemaApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, p.dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := p.client(t)
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			return err
		})).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_five")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish messages to topic1", publishMessages(sidecarName1, topicActiveName, consumerGroup1)).
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}

func (p *pulsarSuite) TestPulsarEncryptionFromData() {
	t := p.T()
	consumerGroup1 := watcher.NewUnordered()

	publishMessages := func(sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				test := &schemaTest{
					ID:   i,
					Name: uuid.New().String(),
				}

				b, _ := json.Marshal(test)
				messages[i] = string(b)
			}

			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)

				err := client.PublishEvent(ctx, pubsubName, topicName, message)
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	flow.New(t, "pulsar encryption test with data").

		// Run subscriberApplication app2
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberSchemaApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, p.dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := p.client(t)
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			return err
		})).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_six")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish messages to topic1", publishMessages(sidecarName1, topicActiveName, consumerGroup1)).
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}

func (p *pulsarSuite) client(t *testing.T) (pulsar.Client, error) {
	t.Helper()

	opts := pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	}
	switch p.authType {
	case "oauth2":
		cc, err := p.oauth2ClientCredentials()
		require.NoError(t, err)
		opts.Authentication = pulsar.NewAuthenticationTokenFromSupplier(cc.Token)
	default:
	}

	return pulsar.NewClient(opts)
}

func (p *pulsarSuite) oauth2ClientCredentials() (*oauth2.ClientCredentials, error) {
	cc, err := oauth2.NewClientCredentials(context.Background(), oauth2.ClientCredentialsOptions{
		Logger:       logger.NewLogger("dapr.test.readiness"),
		TokenURL:     "https://localhost:8085/issuer1/token",
		ClientID:     "foo",
		ClientSecret: "bar",
		Scopes:       []string{"openid"},
		Audiences:    []string{"pulsar"},
		CAPEM:        p.oauth2CAPEM,
	})
	if err != nil {
		return nil, err
	}

	return cc, nil
}

func peerCertificate(t *testing.T, hostport string) []byte {
	conf := &tls.Config{InsecureSkipVerify: true}

	for {
		time.Sleep(1 * time.Second)

		conn, err := tls.Dial("tcp", hostport, conf)
		if err != nil {
			t.Log(err)
			continue
		}

		defer conn.Close()

		certs := conn.ConnectionState().PeerCertificates
		require.Len(t, certs, 1, "expected 1 peer certificate")
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certs[0].Raw})
	}
}
