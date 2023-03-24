/*
Copyright 2023 The Dapr Authors
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

package kitex

import (
	"context"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/cloudwego/kitex"
	"github.com/cloudwego/kitex-examples/kitex_gen/api"
	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	kitex_tests "github.com/dapr/components-contrib/tests/e2e/bindings/kitex"
	"github.com/dapr/kit/logger"
)

const (
	hostports   = "127.0.0.1:8888"
	destService = "echo"
	MethodName  = "echo"
)

func TestInvoke(t *testing.T) {
	// 0. init  Kitex server
	go func() {
		err := kitex_tests.EchoKitexServer()
		require.NoError(t, err)
	}()
	time.Sleep(time.Second * 3)

	// 1 create KitexOutput
	output := NewKitexOutput(logger.NewLogger("test"))

	// 2 create req bytes
	codec := utils.NewThriftMessageCodec()
	req := &api.EchoEchoArgs{Req: &api.Request{Message: "hello dapr"}}
	reqData, err := codec.Encode(MethodName, thrift.CALL, 0, req)
	require.NoError(t, err)

	// 3. Invoke dapr kitex output binding, get rsp bytes
	metadata := map[string]string{
		metadataRPCVersion:     kitex.Version,
		metadataRPCHostports:   hostports,
		metadataRPCDestService: destService,
		metadataRPCMethodName:  MethodName,
	}

	resp, err := output.Invoke(context.Background(), &bindings.InvokeRequest{
		Metadata:  metadata,
		Data:      reqData,
		Operation: bindings.GetOperation,
	})
	require.NoError(t, err)

	// 4. get resp value
	result := &api.EchoEchoResult{}
	_, _, err = codec.Decode(resp.Data, result)
	require.NoError(t, err)
	assert.Equal(t, "hello dapr,hi Kitex", result.Success.Message)
}
