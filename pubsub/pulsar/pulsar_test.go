package pulsar

import (
	"testing"
	"time"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/stretchr/testify/assert"
)

func TestParsePulsarMetadata(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"host": "a", "enableTLS": "false"}
	meta, err := parsePulsarMetadata(m)

	assert.Nil(t, err)
	assert.Equal(t, "a", meta.Host)
	assert.Equal(t, false, meta.EnableTLS)
}

func TestParsePublishMetadata(t *testing.T) {
	m := &pubsub.PublishRequest{}
	m.Metadata = map[string]string{
		"deliverAt":    "2021-08-31 11:45:02",
		"deliverAfter": "60s",
	}
	msg := parsePublishMetadata(m)

	val, _ := time.ParseDuration("60s")
	assert.Equal(t, val, msg.DeliverAfter)
	assert.Equal(t, "2021-08-31 11:45:02",
		msg.DeliverAt.Format("2006-01-02 15:04:05"))
}

func TestMissingHost(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"host": ""}
	meta, err := parsePulsarMetadata(m)

	assert.Error(t, err)
	assert.Nil(t, meta)
	assert.Equal(t, "pulsar error: missing pulsar host", err.Error())
}

func TestInvalidTLSInput(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"host": "a", "enableTLS": "honk"}
	meta, err := parsePulsarMetadata(m)

	assert.Error(t, err)
	assert.Nil(t, meta)
	assert.Equal(t, "pulsar error: invalid value for enableTLS", err.Error())
}
