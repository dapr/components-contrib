package pulsar

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
)

func TestParsePulsarMetadata(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"host": "a", "enableTLS": "false"}
	meta, err := parsePulsarMetadata(m)

	assert.Nil(t, err)
	assert.Equal(t, "a", meta.Host)
	assert.Equal(t, false, meta.EnableTLS)
	assert.Equal(t, defaultTenant, meta.Tenant)
	assert.Equal(t, defaultNamespace, meta.Namespace)
}

func TestParsePublishMetadata(t *testing.T) {
	m := &pubsub.PublishRequest{}
	m.Metadata = map[string]string{
		"deliverAt":    "2021-08-31T11:45:02Z",
		"deliverAfter": "60s",
	}
	msg, err := parsePublishMetadata(m)
	assert.Nil(t, err)

	val, _ := time.ParseDuration("60s")
	assert.Equal(t, val, msg.DeliverAfter)
	assert.Equal(t, "2021-08-31T11:45:02Z",
		msg.DeliverAt.Format(time.RFC3339))
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

func TestValidTenantAndNS(t *testing.T) {
	var (
		testTenant                = "testTenant"
		testNamespace             = "testNamespace"
		testTopic                 = "testTopic"
		expectPersistentResult    = "persistent://testTenant/testNamespace/testTopic"
		expectNonPersistentResult = "non-persistent://testTenant/testNamespace/testTopic"
	)
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"host": "a", tenant: testTenant, namespace: testNamespace}

	t.Run("test vaild tenant and namespace", func(t *testing.T) {
		meta, err := parsePulsarMetadata(m)

		assert.Nil(t, err)
		assert.Equal(t, testTenant, meta.Tenant)
		assert.Equal(t, testNamespace, meta.Namespace)
	})

	t.Run("test persistent format topic", func(t *testing.T) {
		meta, err := parsePulsarMetadata(m)
		p := Pulsar{metadata: *meta}
		res := p.formatTopic(testTopic)

		assert.Nil(t, err)
		assert.Equal(t, true, meta.Persistent)
		assert.Equal(t, expectPersistentResult, res)
	})

	t.Run("test non-persistent format topic", func(t *testing.T) {
		m.Properties[persistent] = "false"
		meta, err := parsePulsarMetadata(m)
		p := Pulsar{metadata: *meta}
		res := p.formatTopic(testTopic)

		assert.Nil(t, err)
		assert.Equal(t, false, meta.Persistent)
		assert.Equal(t, expectNonPersistentResult, res)
	})
}
