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

package dubbo

import (
	"context"
	"fmt"

	"dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/config"
	"dubbo.apache.org/dubbo-go/v3/config/generic"
	hessian "github.com/apache/dubbo-go-hessian2"
	perrors "github.com/pkg/errors"
)

const (
	metadataRPCGroup            = "group"
	metadataRPCVersion          = "version"
	metadataRPCInterface        = "interfaceName"
	metadataRPCMethodName       = "methodName"
	metadataRPCProviderHostname = "providerHostname"
	metadataRPCProviderPort     = "providerPort"
)

type dubboContext struct {
	group         string
	version       string
	interfaceName string
	hostname      string
	port          string
	method        string

	inited bool
	client *generic.GenericService
}

func newDubboContext(metadata map[string]string) *dubboContext {
	dubboMetadata := &dubboContext{}
	dubboMetadata.group = metadata[metadataRPCGroup]
	dubboMetadata.interfaceName = metadata[metadataRPCInterface]
	dubboMetadata.version = metadata[metadataRPCVersion]
	dubboMetadata.method = metadata[metadataRPCMethodName]
	dubboMetadata.hostname = metadata[metadataRPCProviderHostname]
	dubboMetadata.port = metadata[metadataRPCProviderPort]
	dubboMetadata.inited = false
	return dubboMetadata
}

func (d *dubboContext) Init() error {
	if d.inited {
		return nil
	}
	consumerConfig := config.NewConsumerConfigBuilder().Build()
	consumerConfig.ProxyFactory = constant.PassThroughProxyFactoryKey
	rootConfig := config.NewRootConfigBuilder().
		SetConsumer(consumerConfig).
		Build()
	referenceConfig := config.NewReferenceConfigBuilder().
		SetInterface(d.interfaceName).
		SetProtocol(constant.Dubbo).
		Build()
	referenceConfig.URL = fmt.Sprintf("%s://%s:%s", constant.Dubbo, d.hostname, d.port)
	referenceConfig.Group = d.group
	referenceConfig.Version = d.version

	if err := referenceConfig.Init(rootConfig); err != nil {
		return err
	}
	rootConfig.Start()
	referenceConfig.GenericLoad(d.interfaceName)
	genericService, ok := referenceConfig.GetRPCService().(*generic.GenericService)
	if !ok {
		return perrors.Errorf("Get gerneric service of dubbo failed")
	}
	d.client = genericService
	d.inited = true
	return nil
}

func (d *dubboContext) Invoke(ctx context.Context, body []byte) (interface{}, error) {
	return d.client.Invoke(ctx, d.method, []string{}, []hessian.Object{body})
}

func (d *dubboContext) String() string {
	return fmt.Sprintf("%s.%s.%s.%s.%s.%s", d.group, d.version, d.interfaceName, d.hostname, d.port, d.method)
}
