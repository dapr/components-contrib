package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
)

func TestParsePulsarMetadata(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"host": "asdasd"}
	meta, _ := parsePulsarMetadata(m)
	assert.Equal(t, "asdasd", meta.Host)
}