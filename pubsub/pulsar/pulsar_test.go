package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
)

func TestParsePulsarMetadata(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"host": "a", "subscriptionName": "b", "enableTLS": "false"}
	meta, err := parsePulsarMetadata(m)

	assert.Nil(t, err)
	assert.Equal(t, "b", meta.SubscriptionName)
	assert.Equal(t, "a", meta.Host)
	assert.Equal(t, false, meta.EnableTLS)
}

func TestMissingHost(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"host": ""}
	meta, err := parsePulsarMetadata(m)

	assert.Error(t, err)
	assert.Nil(t, meta)
	assert.Equal(t, "Pulsar error: missing pulsar Host", err.Error())
}

func TestInvalidTLSInput(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"host": "a", "subscriptionName": "b", "enableTLS": "honk"}
	meta, err := parsePulsarMetadata(m)

	assert.Error(t, err)
	assert.Nil(t, meta)
	assert.Equal(t, "Pulsar Error: invalid value for enableTLS", err.Error())

}
