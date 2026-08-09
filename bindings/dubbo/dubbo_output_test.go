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
	dubboImpl "dubbo.apache.org/dubbo-go/v3/protocol/dubbo/impl"
	"dubbo.apache.org/dubbo-go/v3/protocol"
	"dubbo.apache.org/dubbo-go/v3/server"
	hessian "github.com/apache/dubbo-go-hessian2"
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

func TestInvoke(t *testing.T) {
	// 0. init dapr provided and dubbo server
	stopCh := make(chan struct{})
	defer close(stopCh)
	// Create output and set serializer before go routine to prevent data race.
	output := NewDubboOutput(logger.NewLogger("test"))
	dubboImpl.SetSerializer(constant.Hessian2Serialization, HessianSerializer{})
	go func() {
		require.NoError(t, runDubboServer(stopCh))
	}()
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

	srv, err := server.NewServer(
		server.WithServerProtocol(
			protocol.WithDubbo(),
			protocol.WithPort(20000),
		),
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
