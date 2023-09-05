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

package dubbobinding_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/config"
	dubboImpl "dubbo.apache.org/dubbo-go/v3/protocol/dubbo/impl"
	hessian "github.com/apache/dubbo-go-hessian2"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/dubbo"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	"github.com/dapr/dapr/pkg/runtime"
	daprsdk "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
)

const (
	localhostIP           = "127.0.0.1"
	dubboPort             = "20000"
	providerInterfaceName = "org.apache.dubbo.samples.UserProvider"
	paramInterfaceName    = "org.apache.dubbo.samples.User"
	providerTypeName      = "UserProvider"
	methodName            = "SayHello"
	helloPrefix           = "hello "
	testName              = "dubbo-certification"
	sidecarName           = "dubbo-sidecar"
	bindingName           = "alicloud-dubbo-binding"
)

func TestDubboBinding(t *testing.T) {
	testDubboInvocation := func(ctx flow.Context) error {
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(runtime.DefaultDaprAPIGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		// 1. create req/rsp value
		reqUser := &User{Name: testName}
		rspUser := &User{}

		// 2. get req bytes
		enc := hessian.NewEncoder()
		hessian.RegisterPOJO(reqUser)
		argTypeList, _ := dubboImpl.GetArgsTypeList([]interface{}{reqUser})
		err := enc.Encode(argTypeList)
		assert.Nil(t, err)
		err = enc.Encode(reqUser)
		assert.Nil(t, err)
		reqData := enc.Buffer()

		metadata := map[string]string{
			"providerPort":     dubboPort,
			"providerHostname": localhostIP,
			"methodName":       methodName,
			"interfaceName":    providerInterfaceName,
		}

		invokeRequest := &daprsdk.InvokeBindingRequest{
			Name:      bindingName,
			Operation: string(bindings.GetOperation),
			Metadata:  metadata,
			Data:      reqData,
		}

		rsp, err := client.InvokeBinding(ctx, invokeRequest)
		assert.Nil(t, err)

		// 4. get rsp value
		decoder := hessian.NewDecoder(rsp.Data)
		_, err = decoder.Decode() // decode type
		assert.Nil(t, err)
		rspDecodedValue, err := decoder.Decode() // decode value
		assert.Nil(t, err)
		err = hessian.ReflectResponse(rspDecodedValue, rspUser)
		assert.Nil(t, err)
		assert.Equal(t, helloPrefix+reqUser.Name, rspUser.Name)
		return nil
	}
	stopCh := make(chan struct{})
	defer close(stopCh)
	go func() {
		assert.Nil(t, runDubboServer(stopCh))
	}()
	time.Sleep(time.Second * 3)

	flow.New(t, "test dubbo binding config").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			embedded.WithBindings(newBindingsRegistry()))).
		Step("verify dubbo invocation", testDubboInvocation).
		Run()
}

func newBindingsRegistry() *bindings_loader.Registry {
	log := logger.NewLogger("dapr.components")

	r := bindings_loader.NewRegistry()
	r.Logger = log
	r.RegisterOutputBinding(dubbo.NewDubboOutput, "alicloud.dubbo")
	return r
}

func runDubboServer(stop chan struct{}) error {
	hessian.RegisterPOJO(&User{})
	config.SetProviderService(&UserProvider{})

	rootConfig := config.NewRootConfigBuilder().
		SetProvider(config.NewProviderConfigBuilder().
			AddService(providerTypeName,
				config.NewServiceConfigBuilder().
					SetProtocolIDs(constant.Dubbo).
					SetInterface(providerInterfaceName).
					Build()).
			Build()).
		AddProtocol(constant.Dubbo, config.NewProtocolConfigBuilder().
			SetName(constant.Dubbo).
			SetPort(dubboPort).
			Build()).
		Build()

	if err := config.Load(config.WithRootConfig(rootConfig)); err != nil {
		return err
	}

	after := time.After(time.Second * 10)
	select {
	case <-stop:
	case <-after:
	}
	return nil
}

type User struct {
	ID   string `hessian:"id"`
	Name string
	Age  int32
	Time time.Time
}

func (u *User) JavaClassName() string {
	return paramInterfaceName
}

type UserProvider struct{}

func (u *UserProvider) SayHello(_ context.Context, user *User) (*User, error) {
	user.Name = helloPrefix + user.Name
	return user, nil
}
