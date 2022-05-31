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
	"dubbo.apache.org/dubbo-go/v3/config"
	dubboImpl "dubbo.apache.org/dubbo-go/v3/protocol/dubbo/impl"
	hessian "github.com/apache/dubbo-go-hessian2"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
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
	testName              = "laurence"
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
	go func() {
		assert.Nil(t, runDubboServer(stopCh))
	}()
	time.Sleep(time.Second * 3)
	dubboImpl.SetSerializer(constant.Hessian2Serialization, HessianSerializer{})
	output := NewDubboOutput(logger.NewLogger("test"))

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

	// 3. invoke dapr dubbo output binding, get rsp bytes
	rsp, err := output.Invoke(context.Background(), &bindings.InvokeRequest{
		Metadata: map[string]string{
			metadataRpcProviderPort:     dubboPort,
			metadataRpcProviderHostname: localhostIP,
			metadataRpcMethodName:       methodName,
			metadataRpcInterface:        providerInterfaceName,
		},
		Data: reqData,
	})
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

type UserProvider struct{}

func (u *UserProvider) SayHello(_ context.Context, user *User) (*User, error) {
	user.Name = helloPrefix + user.Name
	return user, nil
}
