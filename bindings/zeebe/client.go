/*
Copyright 2021 The Dapr Authors
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
	"errors"
	"time"

	"github.com/camunda/zeebe/clients/go/v8/pkg/zbc"

	"github.com/dapr/components-contrib/bindings"
	metadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

var ErrMissingGatewayAddr = errors.New("gatewayAddr is a required attribute")

// ClientFactory enables injection for testing.
type ClientFactory interface {
	Get(metadata bindings.Metadata) (zbc.Client, error)
}

type ClientFactoryImpl struct {
	logger logger.Logger
}

// https://docs.zeebe.io/operations/authentication.html
type clientMetadata struct {
	GatewayAddr            string        `json:"gatewayAddr" mapstructure:"gatewayAddr"`
	GatewayKeepAlive       time.Duration `json:"gatewayKeepAlive" mapstructure:"gatewayKeepAlive"`
	CaCertificatePath      string        `json:"caCertificatePath" mapstructure:"caCertificatePath"`
	UsePlaintextConnection bool          `json:"usePlainTextConnection,string" mapstructure:"usePlainTextConnection"`
}

// NewClientFactoryImpl returns a new ClientFactory instance.
func NewClientFactoryImpl(logger logger.Logger) *ClientFactoryImpl {
	return &ClientFactoryImpl{logger: logger}
}

func (c *ClientFactoryImpl) Get(metadata bindings.Metadata) (zbc.Client, error) {
	meta, err := c.parseMetadata(metadata)
	if err != nil {
		return nil, err
	}

	client, err := zbc.NewClient(&zbc.ClientConfig{
		GatewayAddress:         meta.GatewayAddr,
		UsePlaintextConnection: meta.UsePlaintextConnection,
		CaCertificatePath:      meta.CaCertificatePath,
		KeepAlive:              meta.GatewayKeepAlive,
	})
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (c *ClientFactoryImpl) parseMetadata(meta bindings.Metadata) (*clientMetadata, error) {
	var m clientMetadata
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}

	if m.GatewayAddr == "" {
		return nil, ErrMissingGatewayAddr
	}

	return &m, nil
}
