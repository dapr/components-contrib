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

	_ "dubbo.apache.org/dubbo-go/v3/cluster/cluster/failover"
	_ "dubbo.apache.org/dubbo-go/v3/cluster/loadbalance/random"
	"dubbo.apache.org/dubbo-go/v3/common/constant"
	dubboLogger "dubbo.apache.org/dubbo-go/v3/common/logger"
	_ "dubbo.apache.org/dubbo-go/v3/common/proxy/proxy_factory"
	_ "dubbo.apache.org/dubbo-go/v3/filter/filter_impl"
	_ "dubbo.apache.org/dubbo-go/v3/protocol/dubbo"
	dubboImpl "dubbo.apache.org/dubbo-go/v3/protocol/dubbo/impl"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

type DUBBOOutputBinding struct {
	ctxCache map[string]*dubboContext
}

var dubboBinding *DUBBOOutputBinding

func NewDUBBOOutput(logger logger.Logger) *DUBBOOutputBinding {
	if dubboBinding == nil {
		dubboBinding = &DUBBOOutputBinding{
			ctxCache: make(map[string]*dubboContext),
		}
	}
	dubboLogger.SetLogger(logger)
	return dubboBinding
}

func (out *DUBBOOutputBinding) Init(_ bindings.Metadata) error {
	dubboImpl.SetSerializer(constant.Hessian2Serialization, HessianSerializer{})
	return nil
}

func (out *DUBBOOutputBinding) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var (
		cachedDubboCtx = &dubboContext{}
		ok             bool
		finalResult    = &bindings.InvokeResponse{}
	)

	dubboCtx := newDUBBOContext(req.Metadata)
	dubboCtxKey := dubboCtx.String()

	if cachedDubboCtx, ok = out.ctxCache[dubboCtxKey]; !ok {
		// not found cached dubbo context, init ctx and store into cache
		if err := dubboCtx.Init(); err != nil {
			return finalResult, err
		}
		out.ctxCache[dubboCtxKey] = dubboCtx
		cachedDubboCtx = dubboCtx
	}

	rsp, err := cachedDubboCtx.Invoke(req.Data)
	if data, ok := rsp.([]byte); ok {
		finalResult.Data = data
	}
	return finalResult, err
}

func (out *DUBBOOutputBinding) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

type ProxyInvoker struct {
	Service func(ctx context.Context, methodName string, argsTypes []string, args [][]byte) ([]byte, error)
}
