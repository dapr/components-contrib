/*
Copyright 2022 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package zeebe

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{Properties: map[string]string{
		"gatewayAddr":            "172.0.0.1:1234",
		"gatewayKeepAlive":       "5s",
		"caCertificatePath":      "/cert/path",
		"usePlaintextConnection": "true",
	}}
	client := ClientFactoryImpl{logger: logger.NewLogger("test")}
	meta, err := client.parseMetadata(m)
	assert.NoError(t, err)
	assert.Equal(t, "172.0.0.1:1234", meta.GatewayAddr)
	assert.Equal(t, 5*time.Second, meta.GatewayKeepAlive.Duration)
	assert.Equal(t, "/cert/path", meta.CaCertificatePath)
	assert.Equal(t, true, meta.UsePlaintextConnection)
}

func TestGatewayAddrMetadataIsMandatory(t *testing.T) {
	m := bindings.Metadata{}
	client := ClientFactoryImpl{logger: logger.NewLogger("test")}
	meta, err := client.parseMetadata(m)
	assert.Nil(t, meta)
	assert.Error(t, err)
	assert.Equal(t, err, ErrMissingGatewayAddr)
}

func TestParseMetadataDefaultValues(t *testing.T) {
	m := bindings.Metadata{Properties: map[string]string{"gatewayAddr": "172.0.0.1:1234"}}
	client := ClientFactoryImpl{logger: logger.NewLogger("test")}
	meta, err := client.parseMetadata(m)
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(0), meta.GatewayKeepAlive.Duration)
	assert.Equal(t, "", meta.CaCertificatePath)
	assert.Equal(t, false, meta.UsePlaintextConnection)
}
