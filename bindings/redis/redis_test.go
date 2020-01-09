// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"redisHost": "host", "redisPassword": "password"}
	r := Redis{}
	redisM, err := r.parseMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "host", redisM.Host)
	assert.Equal(t, "password", redisM.Password)
}
