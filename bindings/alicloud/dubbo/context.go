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
	metadataRpcGroup            = "group"
	metadataRpcVersion          = "version"
	metadataRpcInterface        = "interfaceName"
	metadataRpcMethodName       = "methodName"
	metadataRpcProviderHostname = "providerHostname"
	metadataRpcProviderPort     = "providerPort"
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
	dubboMetadata.group = metadata[metadataRpcGroup]
	dubboMetadata.interfaceName = metadata[metadataRpcInterface]
	dubboMetadata.version = metadata[metadataRpcVersion]
	dubboMetadata.method = metadata[metadataRpcMethodName]
	dubboMetadata.hostname = metadata[metadataRpcProviderHostname]
	dubboMetadata.port = metadata[metadataRpcProviderPort]
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

func (d *dubboContext) Invoke(body []byte) (interface{}, error) {
	return d.client.Invoke(context.Background(), d.method, []string{}, []hessian.Object{body})
}

func (d *dubboContext) String() string {
	return fmt.Sprintf("%s.%s.%s.%s.%s.%s", d.group, d.version, d.interfaceName, d.hostname, d.port, d.method)
}
