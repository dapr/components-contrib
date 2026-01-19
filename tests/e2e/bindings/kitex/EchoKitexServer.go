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
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/cloudwego/kitex-examples/kitex_gen/api"
	"github.com/cloudwego/kitex-examples/kitex_gen/api/echo"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/utils"
)

func GenerateEchoReq(message string) (reqData []byte, err error) {
	codec := utils.NewThriftMessageCodec()

	req := &api.EchoEchoArgs{Req: &api.Request{Message: message}}

	reqData, err = codec.Encode("echo", thrift.CALL, 0, req)
	if err != nil {
		return nil, err
	}
	return reqData, nil
}

func EchoKitexServer() error {
	svr := echo.NewServer(new(EchoImpl))
	time.AfterFunc(10*time.Second, func() {
		svr.Stop()
		klog.Info("server stopped")
	})

	if err := svr.Run(); err != nil {
		klog.Errorf("server stopped with error:", err)
		return err
	} else {
		klog.Info("server stopped")
	}
	return nil
}

var _ api.Echo = &EchoImpl{}

type EchoImpl struct{}

// Echo implements the Echo interface.
func (s *EchoImpl) Echo(ctx context.Context, req *api.Request) (resp *api.Response, err error) {
	klog.Info("echo called")
	return &api.Response{Message: req.Message + ",hi Kitex"}, nil
}
