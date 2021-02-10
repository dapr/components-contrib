// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"redisHost": "host", "redisPassword": "password", "enableTLS": "true", "maxRetries": "3", "maxRetryBackoff": "10000"}
	r := Redis{logger: logger.NewLogger("test")}
	redisM, err := r.parseMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "host", redisM.host)
	assert.Equal(t, "password", redisM.password)
	assert.Equal(t, true, redisM.enableTLS)
	assert.Equal(t, 3, redisM.maxRetries)
	assert.Equal(t, time.Duration(10000), redisM.maxRetryBackoff)
}
