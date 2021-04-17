// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package zeebe

import (
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"gatewayAddr":            "172.0.0.1:1234",
		"gatewayKeepAlive":       "5s",
		"caCertificatePath":      "/cert/path",
		"usePlaintextConnection": "true"}
	client := ClientFactoryImpl{logger: logger.NewLogger("test")}
	meta, err := client.parseMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "172.0.0.1:1234", meta.GatewayAddr)
	assert.Equal(t, 5*time.Second, meta.GatewayKeepAlive.Duration)
	assert.Equal(t, "/cert/path", meta.CaCertificatePath)
	assert.Equal(t, true, meta.UsePlaintextConnection)
}

func TestGatewayAddrMetadataIsMandatory(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{}
	client := ClientFactoryImpl{logger: logger.NewLogger("test")}
	meta, err := client.parseMetadata(m)
	assert.Nil(t, meta)
	assert.Error(t, err)
	assert.Equal(t, err, ErrMissingGatewayAddr)
}

func TestParseMetadataDefaultValues(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"gatewayAddr": "172.0.0.1:1234"}
	client := ClientFactoryImpl{logger: logger.NewLogger("test")}
	meta, err := client.parseMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, time.Duration(0), meta.GatewayKeepAlive.Duration)
	assert.Equal(t, "", meta.CaCertificatePath)
	assert.Equal(t, false, meta.UsePlaintextConnection)
}
