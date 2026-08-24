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

	"dubbo.apache.org/dubbo-go/v3/global"
	"dubbo.apache.org/dubbo-go/v3/graceful_shutdown"
	"dubbo.apache.org/dubbo-go/v3/protocol"
	dubboImpl "dubbo.apache.org/dubbo-go/v3/protocol/dubbo/impl"
	"dubbo.apache.org/dubbo-go/v3/server"
	hessian "github.com/apache/dubbo-go-hessian2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
		require.NoError(t, err)
		err = enc.Encode(reqUser)
		require.NoError(t, err)
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
		require.NoError(t, err)

		// 4. get rsp value
		decoder := hessian.NewDecoder(rsp.Data)
		_, err = decoder.Decode() // decode type
		require.NoError(t, err)
		rspDecodedValue, err := decoder.Decode() // decode value
		require.NoError(t, err)
		err = hessian.ReflectResponse(rspDecodedValue, rspUser)
		require.NoError(t, err)
		assert.Equal(t, helloPrefix+reqUser.Name, rspUser.Name)
		return nil
	}
	stopCh := make(chan struct{})
	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- runDubboServer(stopCh)
	}()
	t.Cleanup(func() {
		close(stopCh)
		// dubbo-go's ServeContext returns the context error after a
		// cancellation-triggered graceful shutdown.
		require.ErrorIs(t, <-serverErrCh, context.Canceled)
		// Wait for dubbo-go's process-global graceful shutdown to finish.
		// Getty may report an error while asynchronously closing its session;
		// retain that as a diagnostic instead of failing the invocation.
		if err := graceful_shutdown.Shutdown(context.Background()); err != nil {
			t.Logf("Dubbo graceful shutdown returned an error: %v", err)
		}
	})
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

	// Use immediate graceful-shutdown steps so cleanup does not spend the
	// default multi-second windows notifying and accepting requests.
	internalSignal := false
	shutdownCfg := global.DefaultShutdownConfig()
	shutdownCfg.InternalSignal = &internalSignal
	shutdownCfg.ConsumerUpdateWaitTime = "0s"
	shutdownCfg.StepTimeout = "0s"
	shutdownCfg.OfflineRequestWindowTimeout = "0s"

	srv, err := server.NewServer(
		server.WithServerProtocol(
			protocol.WithDubbo(),
			protocol.WithPort(20000),
		),
		server.SetServerShutdown(shutdownCfg),
	)
	if err != nil {
		return err
	}

	if err := srv.RegisterService(&UserProvider{}, server.WithInterface(providerInterfaceName)); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-stop
		cancel()
	}()
	return srv.ServeContext(ctx)
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
