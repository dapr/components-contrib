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
	"strconv"
	"time"

	"dubbo.apache.org/dubbo-go/v3/common/logger"
	dubboImpl "dubbo.apache.org/dubbo-go/v3/protocol/dubbo/impl"
	hessian "github.com/apache/dubbo-go-hessian2"
	perrors "github.com/pkg/errors"
)

type HessianSerializer struct{}

func (h HessianSerializer) Marshal(p dubboImpl.DubboPackage) ([]byte, error) {
	encoder := hessian.NewEncoder()
	if p.IsRequest() {
		return marshalRequest(encoder, p)
	}
	originHessianSerializer := dubboImpl.HessianSerializer{}
	return originHessianSerializer.Marshal(p)
}

func (h HessianSerializer) Unmarshal(input []byte, p *dubboImpl.DubboPackage) error {
	if p.IsHeartBeat() {
		return nil
	}
	if p.IsResponse() {
		return unmarshalResponseBody(input, p)
	}
	originHessianSerializer := dubboImpl.HessianSerializer{}
	return originHessianSerializer.Unmarshal(input, p)
}

func marshalRequest(encoder *hessian.Encoder, p dubboImpl.DubboPackage) ([]byte, error) {
	service := p.Service
	request := dubboImpl.EnsureRequestPayload(p.Body)
	_ = encoder.Encode(dubboImpl.DEFAULT_DUBBO_PROTOCOL_VERSION)
	_ = encoder.Encode(service.Path)
	_ = encoder.Encode(service.Version)
	args, ok := request.Params.([]interface{})
	if !ok {
		logger.Infof("request args are: %+v", request.Params)
		return nil, perrors.Errorf("@params is not of type: []interface{}")
	}
	_ = encoder.Encode(args[0].(string))

	request.Attachments[dubboImpl.PATH_KEY] = service.Path
	request.Attachments[dubboImpl.VERSION_KEY] = service.Version
	if len(service.Group) > 0 {
		request.Attachments[dubboImpl.GROUP_KEY] = service.Group
	}
	if len(service.Interface) > 0 {
		request.Attachments[dubboImpl.INTERFACE_KEY] = service.Interface
	}
	if service.Timeout != 0 {
		request.Attachments[dubboImpl.TIMEOUT_KEY] = strconv.Itoa(int(service.Timeout / time.Millisecond))
	}

	encoder.Append(args[2].([]hessian.Object)[0].([]byte))

	err := encoder.Encode(request.Attachments)
	return encoder.Buffer(), err
}

func unmarshalResponseBody(body []byte, p *dubboImpl.DubboPackage) error {
	if p.Body == nil {
		p.SetBody(&dubboImpl.ResponsePayload{})
	}
	response := dubboImpl.EnsureResponsePayload(p.Body)
	return perrors.WithStack(hessian.ReflectResponse(body, response.RspObj))
}
