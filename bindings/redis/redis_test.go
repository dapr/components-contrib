package redis

import (
	"testing"

	"github.com/actionscore/components-contrib/bindings"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"redisHost": "a", "redisPassword": "a"}
	r := Redis{}
	redisM, err := r.parseMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "a", redisM.Host)
	assert.Equal(t, "a", redisM.Password)
}
