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
	"errors"
	"fmt"

	"dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/config"
	"dubbo.apache.org/dubbo-go/v3/config/generic"
	hessian "github.com/apache/dubbo-go-hessian2"
)

// TODO delete this and leverage json tags instead.
// Note: leaving for now since this is already a big PR.
const (
	metadataRPCGroup            = "group"
	metadataRPCVersion          = "version"
	metadataRPCInterface        = "interfaceName"
	metadataRPCMethodName       = "methodName"
	metadataRPCProviderHostname = "providerHostname"
	metadataRPCProviderPort     = "providerPort"
)

func newDubboContext(metadata map[string]string) *dubboMetadata {
	dubboMetadata := &dubboMetadata{}
	dubboMetadata.Group = metadata[metadataRPCGroup]
	dubboMetadata.InterfaceName = metadata[metadataRPCInterface]
	dubboMetadata.Version = metadata[metadataRPCVersion]
	dubboMetadata.MethodName = metadata[metadataRPCMethodName]
	dubboMetadata.ProviderHostname = metadata[metadataRPCProviderHostname]
	dubboMetadata.ProviderPort = metadata[metadataRPCProviderPort]
	dubboMetadata.inited = false
	return dubboMetadata
}

func (d *dubboMetadata) Init() error {
	// TODO: eventually remove this inited field
	if d.inited {
		return nil
	}
	consumerConfig := config.NewConsumerConfigBuilder().Build()
	consumerConfig.ProxyFactory = constant.PassThroughProxyFactoryKey
	rootConfig := config.NewRootConfigBuilder().
		SetConsumer(consumerConfig).
		Build()
	referenceConfig := config.NewReferenceConfigBuilder().
		SetInterface(d.InterfaceName).
		SetProtocol(constant.Dubbo).
		Build()
	referenceConfig.URL = fmt.Sprintf("%s://%s:%s", constant.Dubbo, d.ProviderHostname, d.ProviderPort)
	referenceConfig.Group = d.Group
	referenceConfig.Version = d.Version

	if err := referenceConfig.Init(rootConfig); err != nil {
		return err
	}
	rootConfig.Start()
	referenceConfig.GenericLoad(d.InterfaceName)
	genericService, ok := referenceConfig.GetRPCService().(*generic.GenericService)
	if !ok {
		return errors.New("get gerneric service of dubbo failed")
	}
	d.client = genericService
	d.inited = true
	return nil
}

func (d *dubboMetadata) Invoke(ctx context.Context, body []byte) (interface{}, error) {
	return d.client.Invoke(ctx, d.MethodName, []string{}, []hessian.Object{body})
}

func (d *dubboMetadata) String() string {
	return fmt.Sprintf("%s.%s.%s.%s.%s.%s", d.Group, d.Version, d.InterfaceName, d.ProviderHostname, d.ProviderPort, d.MethodName)
}
