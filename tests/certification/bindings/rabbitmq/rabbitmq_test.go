/*
Copyright 2022 The Dapr Authors
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
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dapr/components-contrib/tests/certification/flow/network"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	"github.com/dapr/components-contrib/bindings"
	binding_rabbitmq "github.com/dapr/components-contrib/bindings/rabbitmq"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	"github.com/dapr/dapr/pkg/config/protocol"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	daprClient "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/simulate"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
)

const (
	rabbitMQURL              = "amqp://test:test@localhost:5672"
	rabbitMQURLExtAuth       = "amqps://localhost:5671"
	clusterName              = "rabbitmqcertification"
	dockerComposeYAML        = "docker-compose.yml"
	extSaslDockerComposeYAML = "mtls_sasl_external/docker-compose.yml"
	numOfMessages            = 10
	sidecarName1             = "dapr-1"
	sidecarName2             = "dapr-2"
)

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

func TestRabbitMQ(t *testing.T) {
	messages := watcher.NewUnordered()

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	test := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		// Declare the expected data.
		msgs := make([]string, numOfMessages)

		for i := 0; i < numOfMessages; i++ {
			msgs[i] = fmt.Sprintf("standard-binding: Message %03d", i)
		}

		messages.ExpectStrings(msgs...)

		metadata := make(map[string]string)

		ctx.Log("Invoking binding!")
		for _, msg := range msgs {
			ctx.Logf("Sending: %q", msg)

			req := &daprClient.InvokeBindingRequest{Name: "standard-binding", Operation: "create", Data: []byte(msg), Metadata: metadata}
			err := client.InvokeOutputBinding(ctx, req)
			require.NoError(ctx, err, "error publishing message")
		}

		// Assertion on the data.
		messages.Assert(ctx, time.Minute)

		return nil
	}

	application := func(ctx flow.Context, s common.Service) (err error) {
		// Setup the input binding endpoints.
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("standard-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				messages.Observe(string(in.Data))
				ctx.Logf("Got message: %s", string(in.Data))
				return []byte("{}"), nil
			}))
		return err
	}

	flow.New(t, "rabbitmq certification").
		// Run the application logic above.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for rabbitmq readiness",
			retry.Do(time.Second, 30, amqpReady(rabbitMQURL))).
		Step(app.Run("standardApp", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("standardSidecar",
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithComponentsPath("./components/standard"),
			)...,
		)).
		Step("send and wait", test).
		Run()
}

func TestRabbitMQForOptions(t *testing.T) {
	messages := watcher.NewUnordered()

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	test := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		// Declare the expected data.
		msgs := make([]string, numOfMessages)

		for i := 0; i < numOfMessages; i++ {
			msgs[i] = fmt.Sprintf("options-binding: Message %03d", i)
		}

		messages.ExpectStrings(msgs...)

		metadata := make(map[string]string)

		ctx.Log("Invoking binding!")
		for _, msg := range msgs {
			ctx.Logf("Sending: %q", msg)

			req := &daprClient.InvokeBindingRequest{Name: "options-binding", Operation: "create", Data: []byte(msg), Metadata: metadata}
			err := client.InvokeOutputBinding(ctx, req)
			require.NoError(ctx, err, "error publishing message")
		}

		// Assertion on the data.
		messages.Assert(ctx, time.Minute)

		return nil
	}

	application := func(ctx flow.Context, s common.Service) (err error) {
		// Setup the input binding endpoints.
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("options-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				messages.Observe(string(in.Data))
				ctx.Logf("Got message: %s", string(in.Data))
				return []byte("{}"), nil
			}))
		return err
	}

	flow.New(t, "rabbitmq options certification").
		// Run the application logic above.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for rabbitmq readiness",
			retry.Do(time.Second, 30, amqpReady(rabbitMQURL))).
		Step(app.Run("optionsApp", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("optionsSidecar",
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithComponentsPath("./components/options"),
			)...,
		)).
		Step("send and wait", test).
		Run()
}

func TestRabbitMQTTLs(t *testing.T) {
	ttlMessages := watcher.NewUnordered()

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	ttlTest := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		ctx.Logf("Sending messages for expiration.")
		for i := 0; i < numOfMessages; i++ {
			msg := fmt.Sprintf("Expiring message %d", i)

			metadata := make(map[string]string)

			// Send to the queue with TTL.
			queueTTLReq := &daprClient.InvokeBindingRequest{Name: "queue-ttl-binding", Operation: "create", Data: []byte(msg), Metadata: metadata}
			err := client.InvokeOutputBinding(ctx, queueTTLReq)
			require.NoError(ctx, err, "error publishing message")

			// Send message with TTL set in yaml file
			messageTTLReq := &daprClient.InvokeBindingRequest{Name: "msg-ttl-binding", Operation: "create", Data: []byte(msg), Metadata: metadata}
			messageTTLReq.Metadata["ttlInSeconds"] = "20"
			err = client.InvokeOutputBinding(ctx, messageTTLReq)
			require.NoError(ctx, err, "error publishing message")

			// Send message with TTL to ensure it overwrites Queue TTL.
			mixedTTLReq := &daprClient.InvokeBindingRequest{Name: "overwrite-ttl-binding", Operation: "create", Data: []byte(msg), Metadata: metadata}
			mixedTTLReq.Metadata["ttlInSeconds"] = "10"
			err = client.InvokeOutputBinding(ctx, mixedTTLReq)
			require.NoError(ctx, err, "error publishing message")
		}

		// Wait for double the TTL after sending the last message.
		time.Sleep(time.Second * 20)
		return nil
	}

	ttlApplication := func(ctx flow.Context, s common.Service) (err error) {
		// Setup the input binding endpoints.
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("queue-ttl-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				ctx.Logf("Got message: %s", string(in.Data))
				ttlMessages.FailIfNotExpected(t, string(in.Data))
				return []byte("{}"), nil
			}),
			s.AddBindingInvocationHandler("msg-ttl-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				ctx.Logf("Got message: %s", string(in.Data))
				ttlMessages.FailIfNotExpected(t, string(in.Data))
				return []byte("{}"), nil
			}),
			s.AddBindingInvocationHandler("overwrite-ttl-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				ctx.Logf("Got message: %s", string(in.Data))
				ttlMessages.FailIfNotExpected(t, string(in.Data))
				return []byte("{}"), nil
			}))

		return err
	}

	freshPorts, _ := dapr_testing.GetFreePorts(2)

	flow.New(t, "rabbitmq ttl certification").
		// Run the application logic above.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for rabbitmq readiness",
			retry.Do(time.Second, 30, amqpReady(rabbitMQURL))).
		Step(app.Run("ttlApp", fmt.Sprintf(":%d", appPort), ttlApplication)).
		Step(sidecar.Run("ttlSidecar",
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithComponentsPath("./components/ttl"),
			)...,
		)).
		Step("send ttl messages", ttlTest).
		Step("stop initial sidecar", sidecar.Stop("ttlSidecar")).
		Step(app.Run("ttlApp", fmt.Sprintf(":%d", appPort), ttlApplication)).
		Step(sidecar.Run("appSidecar",
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(freshPorts[0])),
				embedded.WithDaprHTTPPort(strconv.Itoa(freshPorts[1])),
			)...,
		)).
		Step("verify no messages", func(ctx flow.Context) error {
			// Assertion on the data.
			ttlMessages.Assert(t, time.Minute)
			return nil
		}) //.
	// Run()
}

func TestRabbitMQRetriesOnError(t *testing.T) {
	messages := watcher.NewUnordered()

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	testRetry := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		// Declare the expected data.
		msgs := make([]string, numOfMessages)
		for i := 0; i < numOfMessages; i++ {
			msgs[i] = fmt.Sprintf("Message %03d", i)
		}

		messages.ExpectStrings(msgs...)

		metadata := make(map[string]string)

		// Send events that the application above will observe.
		ctx.Log("Invoking binding!")
		for _, msg := range msgs {
			ctx.Logf("Sending: %q", msg)

			req := &daprClient.InvokeBindingRequest{Name: "retry-binding", Operation: "create", Data: []byte(msg), Metadata: metadata}
			err := client.InvokeOutputBinding(ctx, req)
			require.NoError(ctx, err, "error publishing message")
		}

		// Assertion on the data.
		messages.Assert(ctx, time.Minute)

		return nil
	}
	// Application logic that tracks messages from a topic.
	retryApplication := func(ctx flow.Context, s common.Service) (err error) {
		// Simulate periodic errors.
		sim := simulate.PeriodicError(ctx, 10)

		// Setup the input binding endpoint.
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("retry-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				if err := sim(); err != nil {
					ctx.Logf("Failing message: %s", string(in.Data))
					return nil, err
				}
				messages.Observe(string(in.Data))
				ctx.Logf("Got message: %s", string(in.Data))
				return []byte("{}"), nil
			}))

		return err
	}

	flow.New(t, "rabbitmq retry certification").
		// Run the application logic above.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for rabbitmq readiness",
			retry.Do(time.Second, 30, amqpReady(rabbitMQURL))).
		Step(app.Run("retryApp", fmt.Sprintf(":%d", appPort), retryApplication)).
		Step(sidecar.Run("retrySidecar",
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithComponentsPath("./components/retry"),
			)...,
		)).
		Step("send and wait", testRetry).
		Run()
}

func TestRabbitMQNetworkError(t *testing.T) {
	messages := watcher.NewUnordered()

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	test := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		// Declare the expected data.
		msgs := make([]string, numOfMessages)

		for i := 0; i < numOfMessages; i++ {
			msgs[i] = fmt.Sprintf("standard-binding: Message %03d", i)
		}

		messages.ExpectStrings(msgs...)

		metadata := make(map[string]string)

		ctx.Log("Invoking binding!")
		for _, msg := range msgs {
			ctx.Logf("Sending: %q", msg)

			req := &daprClient.InvokeBindingRequest{Name: "standard-binding", Operation: "create", Data: []byte(msg), Metadata: metadata}
			err := client.InvokeOutputBinding(ctx, req)
			require.NoError(ctx, err, "error publishing message")
		}

		// Assertion on the data.
		messages.Assert(ctx, time.Minute)

		return nil
	}

	application := func(ctx flow.Context, s common.Service) (err error) {
		// Setup the input binding endpoints.
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("standard-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				messages.Observe(string(in.Data))
				ctx.Logf("Got message: %s", string(in.Data))
				return []byte("{}"), nil
			}))
		return err
	}

	flow.New(t, "rabbitmq certification").
		// Run the application logic above.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for rabbitmq readiness",
			retry.Do(time.Second, 30, amqpReady(rabbitMQURL))).
		Step(app.Run("standardApp", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("standardSidecar",
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithComponentsPath("./components/standard"),
			)...,
		)).
		Step("send and wait", test).
		Step("interrupt network", network.InterruptNetwork(30*time.Second, nil, nil, "5672")).
		Run()
}

func TestRabbitMQExclusive(t *testing.T) {
	messages := watcher.NewUnordered()

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	test := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		// Declare the expected data.
		msgs := make([]string, numOfMessages)

		for i := 0; i < numOfMessages; i++ {
			msgs[i] = fmt.Sprintf("exclusive-binding: Message %03d", i)
		}

		messages.ExpectStrings(msgs...)

		metadata := make(map[string]string)

		ctx.Log("Invoking binding!")
		for _, msg := range msgs {
			ctx.Logf("Sending: %q", msg)

			req := &daprClient.InvokeBindingRequest{Name: "exclusive-binding", Operation: "create", Data: []byte(msg), Metadata: metadata}
			err := client.InvokeOutputBinding(ctx, req)
			require.Error(ctx, err, "error publishing message")
		}
		return nil
	}

	application := func(ctx flow.Context, s common.Service) (err error) {
		// Setup the input binding endpoints.
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("exclusive-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				messages.Observe(string(in.Data))
				ctx.Logf("Got message: %s", string(in.Data))
				return []byte("{}"), nil
			}))
		return err
	}

	flow.New(t, "rabbitmq certification").
		// Run the application logic above.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for rabbitmq readiness",
			retry.Do(time.Second, 30, amqpReady(rabbitMQURL))).
		Step(app.Run("standardApp", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("standardSidecar",
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithComponentsPath("./components/exclusive"),
			)...,
		)).
		// TODO: The following test function will always fail as expected because the sidecar didn't initialize the component (expected). This should be updated to look for a much more specific error signature however by reading the sidecar's stderr.
		Step("send and wait", test).
		Run()
}

func TestRabbitMQExtAuth(t *testing.T) {
	messages := watcher.NewUnordered()

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	test := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		// Declare the expected data.
		msgs := make([]string, numOfMessages)

		for i := 0; i < numOfMessages; i++ {
			msgs[i] = fmt.Sprintf("standard-binding: Message %03d", i)
		}

		messages.ExpectStrings(msgs...)

		metadata := make(map[string]string)

		ctx.Log("Invoking binding!")
		for _, msg := range msgs {
			ctx.Logf("Sending: %q", msg)

			req := &daprClient.InvokeBindingRequest{Name: "mq-mtls", Operation: "create", Data: []byte(msg), Metadata: metadata}
			err := client.InvokeOutputBinding(ctx, req)
			require.NoError(ctx, err, "error publishing message")
		}

		// Assertion on the data.
		messages.Assert(ctx, time.Minute)

		return nil
	}

	application := func(ctx flow.Context, s common.Service) (err error) {
		// Setup the input binding endpoints.
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("mq-mtls", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				messages.Observe(string(in.Data))
				ctx.Logf("Got message: %s", string(in.Data))
				return []byte("{}"), nil
			}))
		return err
	}

	flow.New(t, "rabbitmq mtls certification").
		// Run the application logic above.
		Step(dockercompose.Run(clusterName, extSaslDockerComposeYAML)).
		Step("wait for rabbitmq readiness",
			retry.Do(time.Second, 30, amqpMtlsExternalAuthReady(rabbitMQURLExtAuth))).
		Step(app.Run("standardApp", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("standardSidecar",
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithComponentsPath("./mtls_sasl_external/components/mtls_external"),
			)...,
		)).
		Step("send and wait", test)
	// Disabling this test because certs have expired and need to be regenerated
	// Run()
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

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	bindingsRegistry := bindings_loader.NewRegistry()
	bindingsRegistry.Logger = log
	bindingsRegistry.RegisterInputBinding(func(l logger.Logger) bindings.InputBinding {
		return binding_rabbitmq.NewRabbitMQ(l)
	}, "rabbitmq")
	bindingsRegistry.RegisterOutputBinding(func(l logger.Logger) bindings.OutputBinding {
		return binding_rabbitmq.NewRabbitMQ(l)
	}, "rabbitmq")

	return []embedded.Option{
		embedded.WithBindings(bindingsRegistry),
	}
}
