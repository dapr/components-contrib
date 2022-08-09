package mqtt

import (
	"context"
	"os"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	// Environment variable containing the host name for MQTT integration tests
	// To run using docker:
	//   Create mosquitto.conf with content:
	//     listener 1883
	//     allow_anonymous true
	//   And run:
	// nolint:misspell
	//     docker run -d -v mosquitto.conf:/mosquitto/config/mosquitto.conf --name test-mqtt -p 1883:1883 eclipse-mosquitto:2
	// In that case the connection string will be: tcp://127.0.0.1:1883
	testMQTTConnectionStringEnvKey = "DAPR_TEST_MQTT_URL"
)

func getConnectionString() string {
	return os.Getenv(testMQTTConnectionStringEnvKey)
}

func TestInvokeWithTopic(t *testing.T) {
	t.Parallel()

	url := getConnectionString()
	if url == "" {
		t.Skipf("MQTT connection string configuration must be set in environment variable '%s' (example 'tcp://localhost:1883')", testMQTTConnectionStringEnvKey)
	}

	topicDefault := "/app/default"
	const msgDefault = "hello from default"
	dataDefault := []byte(msgDefault)

	topicCustomized := "/app/customized"
	const msgCustomized = "hello from customized"
	dataCustomized := []byte(msgCustomized)

	metadata := bindings.Metadata{
		Name: "testQueue",
		Properties: map[string]string{
			"consumerID":        uuid.NewString(),
			"url":               url,
			"topic":             topicDefault,
			"qos":               "1",
			"retain":            "false",
			"cleanSession":      "true",
			"backOffMaxRetries": "0",
		},
	}

	logger := logger.NewLogger("test")

	r := NewMQTT(logger)
	err := r.Init(metadata)
	assert.Nil(t, err)

	conn, err := r.connect(uuid.NewString())
	assert.Nil(t, err)
	defer conn.Disconnect(1)

	msgCh := make(chan interface{})
	defer close(msgCh)

	token := conn.Subscribe("/app/#", 1, func(client mqtt.Client, mqttMsg mqtt.Message) {
		msgCh <- mqttMsg
	})
	ok := token.WaitTimeout(2 * time.Second)
	assert.True(t, ok, "subscribe to /app/# timeout")
	err = token.Error()
	assert.Nil(t, err, "error subscribe to test topic")

	// Timeout in case message transfer error.
	go func() {
		time.Sleep(5 * time.Second)
		msgCh <- "timeout"
	}()

	// Test invoke with default topic configured for component.
	_, err = r.Invoke(context.Background(), &bindings.InvokeRequest{Data: dataDefault})
	assert.Nil(t, err)

	m := <-msgCh
	mqttMessage, ok := m.(mqtt.Message)
	assert.True(t, ok)
	assert.Equal(t, dataDefault, mqttMessage.Payload())
	assert.Equal(t, topicDefault, mqttMessage.Topic())

	// Test invoke with customized topic.
	_, err = r.Invoke(context.Background(), &bindings.InvokeRequest{
		Data: dataCustomized,
		Metadata: map[string]string{
			mqttTopic: topicCustomized,
		},
	})
	assert.Nil(t, err)

	m = <-msgCh
	mqttMessage, ok = m.(mqtt.Message)
	assert.True(t, ok)
	assert.Equal(t, dataCustomized, mqttMessage.Payload())
	assert.Equal(t, topicCustomized, mqttMessage.Topic())
}
