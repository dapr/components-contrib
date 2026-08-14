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
	"testing"
	"time"

	"dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/global"
	"dubbo.apache.org/dubbo-go/v3/graceful_shutdown"
	dubboLogger "dubbo.apache.org/dubbo-go/v3/logger"
	"dubbo.apache.org/dubbo-go/v3/protocol"
	dubboImpl "dubbo.apache.org/dubbo-go/v3/protocol/dubbo/impl"
	"dubbo.apache.org/dubbo-go/v3/server"
	getty "github.com/apache/dubbo-getty"
	hessian "github.com/apache/dubbo-go-hessian2"
	gostLogger "github.com/dubbogo/gost/log/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	localhostIP           = "127.0.0.1"
	dubboPort             = "20000"
	providerInterfaceName = "org.apache.dubbo.samples.UserProvider"
	paramInterfaceName    = "org.apache.dubbo.samples.User"
	methodName            = "SayHello"
	helloPrefix           = "hello "
	testName              = "dubbo-test"
)

type User struct {
	ID   string `hessian:"id"`
	Name string
	Age  int32
	Time time.Time
}

func (u *User) JavaClassName() string {
	return paramInterfaceName
}

// TestNewDubboOutputSetsDubboLoggers is intentionally not parallel: it mutates
// dubbo-go's process-global logger variables and restores them on cleanup.
func TestNewDubboOutputSetsDubboLoggers(t *testing.T) {
	prevGost := gostLogger.GetLogger()
	prevDubbo := dubboLogger.GetLogger()
	prevGetty := getty.GetLogger()
	t.Cleanup(func() {
		gostLogger.SetLogger(prevGost)
		dubboLogger.SetLogger(prevDubbo)
		getty.SetLogger(prevGetty)
	})

	l := logger.NewLogger("dubbo-logger-test")
	NewDubboOutput(l)

	require.Same(t, l, gostLogger.GetLogger())
	require.Same(t, l, dubboLogger.GetLogger())
	require.Same(t, l, getty.GetLogger())
}

func TestInvoke(t *testing.T) {
	// 0. init dapr provided and dubbo server
	stopCh := make(chan struct{})
	serverErrCh := make(chan error, 1)
	// Create output and set serializer before go routine to prevent data race.
	output := NewDubboOutput(logger.NewLogger("test"))
	dubboImpl.SetSerializer(constant.Hessian2Serialization, HessianSerializer{})
	go func() {
		serverErrCh <- runDubboServer(stopCh)
	}()
	t.Cleanup(func() {
		close(stopCh)
		// dubbo-go's ServeContext returns the context error after a
		// cancellation-triggered graceful shutdown.
		require.ErrorIs(t, <-serverErrCh, context.Canceled)
		// Wait for dubbo-go's process-global graceful shutdown to fully
		// complete so its goroutines don't outlive the test, and surface its
		// result (the second Shutdown call waits on the existing shutdown).
		require.NoError(t, graceful_shutdown.Shutdown(context.Background()))
	})
	time.Sleep(time.Second * 3)

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

	// 3. invoke dapr dubbo output binding, get rsp bytes
	rsp, err := output.Invoke(t.Context(), &bindings.InvokeRequest{
		Metadata: map[string]string{
			metadataRPCProviderPort:     dubboPort,
			metadataRPCProviderHostname: localhostIP,
			metadataRPCMethodName:       methodName,
			metadataRPCInterface:        providerInterfaceName,
		},
		Data:      reqData,
		Operation: bindings.GetOperation,
	})
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
}

func runDubboServer(stop chan struct{}) error {
	hessian.RegisterPOJO(&User{})

	// Use immediate graceful-shutdown steps (mirroring dubbo-go's own
	// ServeContext cancellation tests): the default multi-second waits keep
	// the server notifying/accepting while the cached consumer reconnects,
	// which races with the final protocol destroy under -race.
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

type UserProvider struct{}

func (u *UserProvider) SayHello(_ context.Context, user *User) (*User, error) {
	user.Name = helloPrefix + user.Name
	return user, nil
}
