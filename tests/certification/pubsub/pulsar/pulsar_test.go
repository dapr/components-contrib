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
	"net"
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
	topicSchemaName             = "certification-pubsub-topic-schema"
	topicAvroCEName             = "certification-pubsub-topic-avro-ce"
	topicAvroRawName            = "certification-pubsub-topic-avro-raw"
	topicJSONCEName             = "certification-pubsub-topic-json-ce"
	topicJSONRawName            = "certification-pubsub-topic-json-raw"
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
	pulsarReadinessTimeout      = 5 * time.Minute
	deliveryDelay               = 15 * time.Second

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
	topicSuffix       string
}

func (p *pulsarSuite) SetupSuite() {
	flow.New(p.T(), "pulsar cluster startup").
		Step("remove leftover pulsar cluster", dockercompose.Down(clusterName, p.dockerComposeYAML)).
		Step("start pulsar cluster", dockercompose.Up(clusterName, p.dockerComposeYAML)).
		Step("wait for pulsar readiness", p.waitForPulsar()).
		Run()
	if p.T().Failed() {
		// Fail the whole suite instead of letting every test time out.
		p.TearDownSuite()
		p.T().FailNow()
	}
}

func (p *pulsarSuite) TearDownSuite() {
	flow.New(p.T(), "pulsar cluster shutdown").
		Step("stop pulsar cluster", dockercompose.Down(clusterName, p.dockerComposeYAML)).
		Run()
}

func (p *pulsarSuite) SetupTest() {
	p.topicSuffix = strings.ToLower(filepath.Base(p.T().Name()))
}

func (p *pulsarSuite) topic(base string) string {
	return base + "-" + p.topicSuffix
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

		// Create credentials files for testing oauth2ClientSecretPath
		plainTextCredsFile := filepath.Join(dir, "credentials-plain.txt")
		require.NoError(t, os.WriteFile(plainTextCredsFile, []byte("bar"), 0o644))

		jsonCredsFile := filepath.Join(dir, "credentials.json")
		jsonCreds := map[string]string{
			"client_id":     "foo",
			"client_secret": "bar",
			"issuer_url":    "https://localhost:8085/issuer1/token",
		}
		jsonCredsBytes, err := json.Marshal(jsonCreds)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(jsonCredsFile, jsonCredsBytes, 0o644))

		td := struct {
			TmpDir                  string
			OAuth2CAPEM             string
			CredentialsFilePath     string
			CredentialsJSONFilePath string
		}{
			TmpDir:                  dir,
			OAuth2CAPEM:             strings.ReplaceAll(string(oauth2CA), "\n", "\\n"),
			CredentialsFilePath:     plainTextCredsFile,
			CredentialsJSONFilePath: jsonCredsFile,
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
	topicActive := p.topic(topicActiveName)
	topicPassive := p.topic(topicPassiveName)
	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	flow.New(t, "pulsar certification basic test").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActive, consumerGroup1))).
		Step("wait for pulsar readiness", p.waitForPulsar()).
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
			subscriberApplication(appID2, topicActive, consumerGroup2))).

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
		Step(fmt.Sprintf("publish messages to topic: %s", topicActive), publishMessages(nil, sidecarName1, topicActive, consumerGroup1, consumerGroup2)).
		Step(fmt.Sprintf("publish messages to unused topic: %s", topicPassive), publishMessages(nil, sidecarName1, topicPassive)).
		Step("verify if app1 has received messages published to active topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("verify if app2 has received messages published to passive topic", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

func (p *pulsarSuite) TestPulsarMultipleSubsSameConsumerIDs() {
	t := p.T()
	topicActive := p.topic(topicActiveName)
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
			subscriberApplication(appID1, topicActive, consumerGroup1))).
		Step("wait for pulsar readiness", p.waitForPulsar()).
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
			subscriberApplication(appID2, topicActive, consumerGroup2))).

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
		Step(fmt.Sprintf("publish messages to topic: %s", topicActive), publishMessages(metadata, sidecarName1, topicActive, consumerGroup2)).
		Step(fmt.Sprintf("publish messages to topic: %s", topicActive), publishMessages(metadata1, sidecarName2, topicActive, consumerGroup2)).
		Step("verify if app1, app2 together have received messages published to topic1", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

func (p *pulsarSuite) TestPulsarMultipleSubsDifferentConsumerIDs() {
	t := p.T()
	topicActive := p.topic(topicActiveName)

	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	flow.New(t, "pulsar certification - single publisher and multiple subscribers with different consumer IDs").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActive, consumerGroup1))).
		Step("wait for pulsar readiness", p.waitForPulsar()).
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
			subscriberApplication(appID2, topicActive, consumerGroup2))).

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
		Step(fmt.Sprintf("publish messages to topic: %s", topicActive), publishMessages(metadata, sidecarName1, topicActive, consumerGroup1)).
		Step("verify if app1, app2 together have received messages published to topic1", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

func (p *pulsarSuite) TestPulsarMultiplePubSubsDifferentConsumerIDs() {
	t := p.T()
	topicActive := p.topic(topicActiveName)
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
			subscriberApplication(appID1, topicActive, consumerGroup1))).
		Step("wait for pulsar readiness", p.waitForPulsar()).
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
			subscriberApplication(appID2, topicActive, consumerGroup2))).

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
		Step(fmt.Sprintf("publish messages to topic: %s", topicActive), publishMessages(metadata, sidecarName1, topicActive, consumerGroup1)).
		Step(fmt.Sprintf("publish messages to topic: %s", topicActive), publishMessages(metadata1, sidecarName2, topicActive, consumerGroup2)).
		Step("verify if app1, app2 together have received messages published to topic1", assertMessages(10*time.Second, consumerGroup1)).
		Step("verify if app1, app2 together have received messages published to topic1", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

func (p *pulsarSuite) TestPulsarNonexistingTopic() {
	t := p.T()
	topicToCreate := p.topic(topicToBeCreated)
	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	flow.New(t, "pulsar certification - non-existing topic").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset*3),
			subscriberApplication(appID1, topicToCreate, consumerGroup1))).
		Step("wait for pulsar readiness", p.waitForPulsar()).
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
		Step(fmt.Sprintf("publish messages to newly created topic: %s", topicToCreate), publishMessages(metadata, sidecarName1, topicToCreate, consumerGroup1)).
		Step("verify if app1 has received messages published to newly created topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

func (p *pulsarSuite) TestPulsarNetworkInterruption() {
	t := p.T()
	topicActive := p.topic(topicActiveName)
	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	flow.New(t, "pulsar certification - network interruption").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID1, topicActive, consumerGroup1))).
		Step("wait for pulsar readiness", p.waitForPulsar()).
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
		Step(fmt.Sprintf("publish messages to topic: %s", topicActive), publishMessages(metadata, sidecarName1, topicActive, consumerGroup1)).
		Step("interrupt network", network.InterruptNetwork(30*time.Second, nil, nil, "6650")).
		Step("verify if app1 has received messages published to newly created topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

func (p *pulsarSuite) TestPulsarPersitant() {
	t := p.T()
	topicActive := p.topic(topicActiveName)
	consumerGroup1 := watcher.NewUnordered()

	flow.New(t, "pulsar certification persistant test").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActive, consumerGroup1))).
		Step("wait for pulsar readiness", p.waitForPulsar()).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_one")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
				embedded.WithGracefulShutdownDuration(time.Second*20),
			)...,
		)).
		Step(fmt.Sprintf("publish messages to topic: %s", topicActive), publishMessages(nil, sidecarName1, topicActive, consumerGroup1)).
		Step("stop pulsar server", dockercompose.Stop(clusterName, p.dockerComposeYAML, p.services...)).
		Step("start pulsar server", dockercompose.Start(clusterName, p.dockerComposeYAML, p.services...)).
		Step("wait for pulsar readiness after restart", p.waitForPulsar()).
		Step("verify if app1 has received messages published to active topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}

func (p *pulsarSuite) TestPulsarDelay() {
	t := p.T()
	topicActive := p.topic(topicActiveName)
	consumerGroup1 := watcher.NewUnordered()

	metadataAfter := map[string]string{"deliverAfter": deliveryDelay.String()}

	assertMessagesNot := func(window time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			for _, m := range messageWatchers {
				_, _, observed := m.Partial(ctx, window)
				require.Empty(ctx, observed, "messages were delivered before the delivery delay elapsed")
			}

			return nil
		}
	}

	flow.New(t, "pulsar certification delay test").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActive, consumerGroup1))).
		Step("wait for pulsar readiness", p.waitForPulsar()).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_three")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step(fmt.Sprintf("publish messages with deliverAfter to topic: %s", topicActive), publishMessages(metadataAfter, sidecarName1, topicActive, consumerGroup1)).
		// receive no messages due to deliverAfter delay
		Step("verify if app1 has received no messages published to topic", assertMessagesNot(5*time.Second, consumerGroup1)).
		// delay has passed, messages should be received
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		// publish messages using deliverAt property
		Step(fmt.Sprintf("publish messages with deliverAt to topic: %s", topicActive), func(ctx flow.Context) error {
			metadataAt := map[string]string{"deliverAt": time.Now().Add(deliveryDelay).Format(time.RFC3339Nano)}
			return publishMessages(metadataAt, sidecarName1, topicActive, consumerGroup1)(ctx)
		}).
		Step("verify if app1 has received no messages published to topic", assertMessagesNot(5*time.Second, consumerGroup1)).
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

type schemaTest struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type avroSchemaTest struct {
	TestID   int    `json:"testId"`
	TestName string `json:"testName"`
}

func subscriberRawSchemaApplication(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
	return func(ctx flow.Context, s common.Service) error {
		return multierr.Combine(
			s.AddTopicEventHandler(&common.Subscription{
				PubsubName: pubsubName,
				Topic:      topicName,
				Route:      "/orders",
				Metadata:   map[string]string{"rawPayload": "true"},
			}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
				// With rawPayload, e.Data arrives as []uint8.
				// Unmarshal into the typed struct to normalize JSON field order
				// so that it matches the published strings.
				dataStr := fmt.Sprintf("%s", e.Data)
				var obj avroSchemaTest
				if err := json.Unmarshal([]byte(dataStr), &obj); err != nil {
					ctx.Logf("failed to unmarshal raw schema payload in subscriber (appID=%s, topic=%s, id=%s): %v", appID, e.Topic, e.ID, err)
					// Non-retryable error so the test fails clearly on bad payloads.
					return false, fmt.Errorf("subscriberRawSchemaApplication: unmarshal payload: %w", err)
				}
				normalized, err := json.Marshal(obj)
				if err != nil {
					ctx.Logf("failed to marshal normalized raw schema payload in subscriber (appID=%s, topic=%s, id=%s): %v", appID, e.Topic, e.ID, err)
					// Non-retryable error so the test fails clearly on bad normalization.
					return false, fmt.Errorf("subscriberRawSchemaApplication: marshal normalized payload: %w", err)
				}
				messagesWatcher.Observe(string(normalized))
				ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
				return false, nil
			}),
		)
	}
}

func publishSchemaMessages(sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
	return func(ctx flow.Context) error {
		// prepare the messages
		messages := make([]string, numMessages)
		for i := range messages {
			test := &schemaTest{
				ID:   i,
				Name: uuid.New().String(),
			}

			b, err := json.Marshal(test)
			require.NoError(ctx, err, "error marshaling schemaTest")
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

// publishSchemaMessagesCE publishes schema-validated messages without rawPayload,
// allowing Dapr to wrap them in a CloudEvents envelope. Works for both Avro and
// JSON schema topics since the payload shape (avroSchemaTest) is the same.
func publishSchemaMessagesCE(sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
	return func(ctx flow.Context) error {
		messages := make([]string, numMessages)
		for i := range messages {
			test := &avroSchemaTest{
				TestID:   i,
				TestName: uuid.New().String(),
			}

			b, err := json.Marshal(test)
			require.NoError(ctx, err, "error marshaling avroSchemaTest")
			messages[i] = string(b)
		}

		for _, messageWatcher := range messageWatchers {
			messageWatcher.ExpectStrings(messages...)
		}

		client := sidecar.GetClient(ctx, sidecarName)

		ctx.Logf("Publishing messages (CE wrapped). sidecarName: %s, topicName: %s", sidecarName, topicName)

		for _, message := range messages {
			ctx.Logf("Publishing: %q", message)

			err := client.PublishEvent(ctx, pubsubName, topicName, message)
			require.NoError(ctx, err, "error publishing message")
		}
		return nil
	}
}

// publishSchemaMessagesRaw publishes schema-validated messages with rawPayload=true,
// bypassing CloudEvents wrapping. Used with rawSchema=true topics for both Avro
// and JSON schema types.
func publishSchemaMessagesRaw(sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
	return func(ctx flow.Context) error {
		messages := make([]string, numMessages)
		for i := range messages {
			test := &avroSchemaTest{
				TestID:   i,
				TestName: uuid.New().String(),
			}

			b, err := json.Marshal(test)
			require.NoError(ctx, err, "error marshaling avroSchemaTest")
			messages[i] = string(b)
		}

		for _, messageWatcher := range messageWatchers {
			messageWatcher.ExpectStrings(messages...)
		}

		client := sidecar.GetClient(ctx, sidecarName)

		ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

		for _, message := range messages {
			ctx.Logf("Publishing: %q", message)

			err := client.PublishEvent(ctx, pubsubName, topicName, message, dapr.PublishEventWithRawPayload())
			require.NoError(ctx, err, "error publishing message")
		}
		return nil
	}
}

func (p *pulsarSuite) TestPulsarSchema() {
	t := p.T()
	consumerGroup1 := watcher.NewUnordered()

	flow.New(t, "pulsar certification schema test").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberSchemaApplication(appID1, topicSchemaName, consumerGroup1))).
		Step("wait for pulsar readiness", p.waitForPulsar()).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_four")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step(fmt.Sprintf("publish messages to topic: %s", topicSchemaName), publishSchemaMessages(sidecarName1, topicSchemaName, consumerGroup1)).
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

// TestPulsarAvroSchema tests Avro schema with CloudEvents envelope wrapping.
// The sidecar registers the CE-wrapped schema on subscribe; no pre-registration needed.
func (p *pulsarSuite) TestPulsarAvroSchema() {
	t := p.T()
	consumerGroup1 := watcher.NewUnordered()

	flow.New(t, "pulsar certification avro schema test").

		// subscriberSchemaApplication subscribes without rawPayload, so Dapr
		// unwraps the CloudEvents envelope and delivers the inner data field.
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberSchemaApplication(appID1, topicAvroCEName, consumerGroup1))).
		Step("wait for pulsar readiness", p.waitForPulsar()).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_nine")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step(fmt.Sprintf("publish messages to topic: %s", topicAvroCEName), publishSchemaMessagesCE(sidecarName1, topicAvroCEName, consumerGroup1)).
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

// TestPulsarAvroSchemaRaw tests Avro schema with rawSchema=true, bypassing
// CloudEvents envelope wrapping. The raw user schema is pre-registered on the
// topic and the component uses it directly without CE wrapping.
func (p *pulsarSuite) TestPulsarAvroSchemaRaw() {
	t := p.T()
	consumerGroup1 := watcher.NewUnordered()

	avroSchema := `{"type":"record","name":"Example","namespace":"test","fields":[{"name":"testId","type":"int"},{"name":"testName","type":"string"}]}`

	flow.New(t, "pulsar certification avro schema raw test").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberRawSchemaApplication(appID1, topicAvroRawName, consumerGroup1))).
		Step("wait for pulsar readiness", p.waitForPulsar()).
		// Pre-register the raw Avro schema on the topic. Because consumer_ten
		// uses rawschema=true, the sidecar subscribes with the same raw schema
		// (no CloudEvents wrapping), so Pulsar accepts the consumer.
		Step("register avro schema on topic", func(ctx flow.Context) error {
			client, err := p.client(t)
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}
			defer client.Close()

			producer, err := client.CreateProducer(pulsar.ProducerOptions{
				Topic:  "persistent://public/default/" + topicAvroRawName,
				Schema: pulsar.NewAvroSchema(avroSchema, nil),
			})
			if err != nil {
				return fmt.Errorf("could not create producer to register avro schema: %v", err)
			}
			producer.Close()

			return nil
		}).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_ten")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish messages to topic1", publishSchemaMessagesRaw(sidecarName1, topicAvroRawName, consumerGroup1)).
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

// TestPulsarJSONSchema tests JSON schema with CloudEvents envelope wrapping.
// The sidecar registers the CE-wrapped schema on subscribe; no pre-registration needed.
func (p *pulsarSuite) TestPulsarJSONSchema() {
	t := p.T()
	consumerGroup1 := watcher.NewUnordered()

	flow.New(t, "pulsar certification json schema CE test").

		// subscriberSchemaApplication subscribes without rawPayload, so Dapr
		// unwraps the CloudEvents envelope and delivers the inner data field.
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberSchemaApplication(appID1, topicJSONCEName, consumerGroup1))).
		Step("wait for pulsar readiness", p.waitForPulsar()).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_eleven")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish messages to topic1", publishSchemaMessagesCE(sidecarName1, topicJSONCEName, consumerGroup1)).
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

// TestPulsarJSONSchemaRaw tests JSON schema with rawSchema=true, bypassing
// CloudEvents envelope wrapping. The raw user schema is pre-registered on the
// topic and the component uses it directly without CE wrapping.
func (p *pulsarSuite) TestPulsarJSONSchemaRaw() {
	t := p.T()
	consumerGroup1 := watcher.NewUnordered()

	// Pulsar JSON schema uses Avro-compatible record definitions, not JSON Schema Draft.
	jsonSchema := `{"type":"record","name":"Example","namespace":"test","fields":[{"name":"testId","type":"int"},{"name":"testName","type":"string"}]}`

	flow.New(t, "pulsar certification json schema raw test").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberRawSchemaApplication(appID1, topicJSONRawName, consumerGroup1))).
		Step("wait for pulsar readiness", p.waitForPulsar()).
		// Pre-register the raw JSON schema on the topic. Because consumer_twelve
		// uses rawschema=true, the sidecar subscribes with the same raw schema
		// (no CloudEvents wrapping), so Pulsar accepts the consumer.
		Step("register json schema on topic", func(ctx flow.Context) error {
			client, err := p.client(t)
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}
			defer client.Close()

			producer, err := client.CreateProducer(pulsar.ProducerOptions{
				Topic:  "persistent://public/default/" + topicJSONRawName,
				Schema: pulsar.NewJSONSchema(jsonSchema, nil),
			})
			if err != nil {
				return fmt.Errorf("could not create producer to register json schema: %v", err)
			}
			producer.Close()

			return nil
		}).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_twelve")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish messages to topic1", publishSchemaMessagesRaw(sidecarName1, topicJSONRawName, consumerGroup1)).
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

// TestOAuth2WithPlainTextCredentialsFile tests OAuth2 authentication using oauth2ClientSecretPath
// with a plain text credentials file (backward compatibility).
func (p *pulsarSuite) TestOAuth2WithPlainTextCredentialsFile() {
	t := p.T()
	topicActive := p.topic(topicActiveName)
	consumerGroup1 := watcher.NewUnordered()

	if p.authType != "oauth2" {
		t.Skip("Skipping OAuth2 credentials file test for non-OAuth2 auth type")
		return
	}

	flow.New(t, "pulsar certification oauth2 plain text credentials file test").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActive, consumerGroup1))).
		Step("wait for pulsar readiness", p.waitForPulsar()).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_seven")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step(fmt.Sprintf("publish messages to topic: %s", topicActive), publishMessages(nil, sidecarName1, topicActive, consumerGroup1)).
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

// TestOAuth2WithJSONCredentialsFile tests OAuth2 authentication using oauth2CredentialsFile
// with a JSON credentials file containing both client_id and client_secret.
func (p *pulsarSuite) TestOAuth2WithJSONCredentialsFile() {
	t := p.T()
	topicActive := p.topic(topicActiveName)
	consumerGroup1 := watcher.NewUnordered()

	if p.authType != "oauth2" {
		t.Skip("Skipping OAuth2 credentials file test for non-OAuth2 auth type")
		return
	}

	flow.New(t, "pulsar certification oauth2 json credentials file test").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActive, consumerGroup1))).
		Step("wait for pulsar readiness", p.waitForPulsar()).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_eight")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step(fmt.Sprintf("publish messages to topic: %s", topicActive), publishMessages(nil, sidecarName1, topicActive, consumerGroup1)).
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

		req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, bytes.NewBuffer(reqBody))

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

		client := &http.Client{Timeout: 10 * time.Second}
		rsp, err := client.Do(req)

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
	topicMultiPartition := p.topic(topicMultiPartitionName)
	topicActive := p.topic(topicActiveName)
	consumerGroup1 := watcher.NewOrdered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	flow.New(t, "pulsar certification -  process message in order with partitioned-topic").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplicationWithoutError(appID1, topicMultiPartition, consumerGroup1))).
		Step("wait for pulsar readiness", p.waitForPulsar()).
		Step(fmt.Sprintf("create multi-partition topic explicitly: %s", topicMultiPartition), retry.Do(time.Second, 30,
			p.createMultiPartitionTopic("public", "default", topicMultiPartition, 4))).
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
			subscriberApplicationWithoutError(appID2, topicActive, consumerGroup1))).

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
		Step(fmt.Sprintf("publish messages to multi-partition topic: %s", topicMultiPartition), publishMessages(metadata, sidecarName1, topicMultiPartition, consumerGroup1)).
		Step("verify if app1 has received messages published to newly created topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}

func (p *pulsarSuite) TestPulsarEncryptionFromFile() {
	t := p.T()
	topicActive := p.topic(topicActiveName)
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
			subscriberSchemaApplication(appID1, topicActive, consumerGroup1))).
		Step("wait for pulsar readiness", p.waitForPulsar()).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_five")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step(fmt.Sprintf("publish messages to topic: %s", topicActive), publishMessages(sidecarName1, topicActive, consumerGroup1)).
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}

func (p *pulsarSuite) TestPulsarEncryptionFromData() {
	t := p.T()
	topicActive := p.topic(topicActiveName)
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
			subscriberSchemaApplication(appID1, topicActive, consumerGroup1))).
		Step("wait for pulsar readiness", p.waitForPulsar()).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath(filepath.Join(p.componentsPath, "consumer_six")),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step(fmt.Sprintf("publish messages to topic: %s", topicActive), publishMessages(sidecarName1, topicActive, consumerGroup1)).
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}

func (p *pulsarSuite) waitForPulsar() flow.Runnable {
	return func(ctx flow.Context) error {
		readinessCtx, cancel := context.WithTimeout(ctx, pulsarReadinessTimeout)
		defer cancel()

		probe := func() error {
			client, err := p.newClient(ctx.T, pulsar.ClientOptions{
				ConnectionTimeout: 3 * time.Second,
				OperationTimeout:  5 * time.Second,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %w", err)
			}
			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not subscribe to pulsar readiness topic: %w", err)
			}
			defer consumer.Close()
			return nil
		}

		for {
			if err := readinessCtx.Err(); err != nil {
				return err
			}

			result := make(chan error, 1)
			// Subscribe has no context parameter, so bound the probe separately.
			go func() { result <- probe() }()
			select {
			case <-readinessCtx.Done():
				return readinessCtx.Err()
			case err := <-result:
				if err == nil {
					return nil
				}
				ctx.Logf("Pulsar is not ready: %v", err)
			}

			select {
			case <-readinessCtx.Done():
				return readinessCtx.Err()
			case <-time.After(time.Second):
			}
		}
	}
}

func (p *pulsarSuite) client(t *testing.T) (pulsar.Client, error) {
	t.Helper()
	return p.newClient(t, pulsar.ClientOptions{})
}

func (p *pulsarSuite) newClient(t *testing.T, opts pulsar.ClientOptions) (pulsar.Client, error) {
	t.Helper()

	opts.URL = "pulsar://" + pulsarURL
	switch p.authType {
	case "oauth2":
		cc, err := p.oauth2ClientCredentials()
		if err != nil {
			return nil, err
		}
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
	deadline := time.Now().Add(2 * time.Minute)
	dialer := &net.Dialer{Timeout: 3 * time.Second, Deadline: deadline}

	for time.Now().Before(deadline) {
		conn, err := tls.DialWithDialer(dialer, "tcp", hostport, conf)
		if err != nil {
			t.Log(err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		defer conn.Close()

		certs := conn.ConnectionState().PeerCertificates
		require.Len(t, certs, 1, "expected 1 peer certificate")
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certs[0].Raw})
	}

	require.Fail(t, "timed out waiting for OAuth2 server certificate", "server: %s", hostport)
	return nil
}
