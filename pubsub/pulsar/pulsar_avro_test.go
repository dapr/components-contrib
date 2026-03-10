package pulsar

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

// MockPulsarMessage implements a subset of pulsar.Message needed for handleMessage
type MockPulsarMessage struct {
	pulsar.Message
	payload    []byte
	properties map[string]string
	topic      string
	key        string
}

func (m *MockPulsarMessage) Payload() []byte {
	return m.payload
}

func (m *MockPulsarMessage) Properties() map[string]string {
	return m.properties
}

func (m *MockPulsarMessage) Topic() string {
	return m.topic
}

func (m *MockPulsarMessage) Key() string {
	return m.key
}

func (m *MockPulsarMessage) ID() pulsar.MessageID {
	return nil
}

// MockConsumerForHandleMessage implements a subset of pulsar.Consumer needed for handleMessage
type MockConsumerForHandleMessage struct {
	pulsar.Consumer
	Acked  bool
	Nacked bool
}

func (m *MockConsumerForHandleMessage) Ack(msg pulsar.Message) error {
	m.Acked = true
	return nil
}

func (m *MockConsumerForHandleMessage) Nack(msg pulsar.Message) {
	m.Nacked = true
}

func (m *MockConsumerForHandleMessage) Chan() <-chan pulsar.ConsumerMessage {
	return nil
}

func (m *MockConsumerForHandleMessage) Close() {}

func TestHandleMessage_AvroDecoding(t *testing.T) {
	// 1. Setup Avro Schema
	avroSchemaJSON := `{
		"type": "record",
		"name": "Person",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age", "type": "int"}
		]
	}`

	// Use the helper from pulsar_test.go to create schema metadata
	sm := newAvroSchemaMetadata(t, avroSchemaJSON)

	// 2. Setup Pulsar Component
	p := &Pulsar{
		logger: logger.NewLogger("test"),
		metadata: pulsarMetadata{
			internalTopicSchemas: map[string]schemaMetadata{
				"my-topic": sm,
			},
		},
	}

	// 3. Create Avro Binary Payload
	// We can use the codec from sm to generate binary data
	nativeData := map[string]interface{}{
		"name": "John Doe",
		"age":  30,
	}
	binaryData, err := sm.codec.BinaryFromNative(nil, nativeData)
	require.NoError(t, err)

	// 4. Create Mock Message and Consumer
	mockMsg := &MockPulsarMessage{
		payload: binaryData,
		topic:   "my-topic",
	}
	mockConsumer := &MockConsumerForHandleMessage{}

	// Create the ConsumerMessage struct
	consumerMsg := pulsar.ConsumerMessage{
		Consumer: mockConsumer,
		Message:  mockMsg,
	}

	// 5. Call handleMessage
	var receivedMsg *pubsub.NewMessage
	handler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		receivedMsg = msg
		return nil
	}

	err = p.handleMessage(context.Background(), "my-topic", consumerMsg, handler)
	require.NoError(t, err)

	// 6. Verify Results
	require.NotNil(t, receivedMsg)

	// Since we wrap the JSON in a CloudEvent, we expect a CloudEvent structure
	var cloudEvent map[string]interface{}
	err = json.Unmarshal(receivedMsg.Data, &cloudEvent)
	require.NoError(t, err)

	assert.Equal(t, "1.0", cloudEvent["specversion"])
	assert.Equal(t, "application/json", cloudEvent["datacontenttype"])

	// Verify data field
	dataField, ok := cloudEvent["data"].(map[string]interface{})
	require.True(t, ok, "data field should be a map")

	assert.Equal(t, float64(30), dataField["age"])
	assert.Equal(t, "John Doe", dataField["name"])

	assert.True(t, mockConsumer.Acked)
	assert.False(t, mockConsumer.Nacked)
}
