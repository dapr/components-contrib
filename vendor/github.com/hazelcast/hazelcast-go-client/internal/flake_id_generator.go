// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"github.com/hazelcast/hazelcast-go-client/internal/flakeid"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

type flakeIDGeneratorProxy struct {
	*proxy
	batcher *flakeid.AutoBatcher
}

func newFlakeIDGenerator(client *HazelcastClient, serviceName string, name string) *flakeIDGeneratorProxy {
	config := client.Config.GetFlakeIDGeneratorConfig(name)
	flakeIDGenerator := &flakeIDGeneratorProxy{}
	flakeIDGenerator.proxy = &proxy{client: client, serviceName: serviceName, name: name}
	flakeIDGenerator.batcher = flakeid.NewAutoBatcher(config.PrefetchCount(), config.PrefetchValidityMillis(), flakeIDGenerator)
	return flakeIDGenerator
}

func (fp *flakeIDGeneratorProxy) NewID() (id int64, err error) {
	return fp.batcher.NewID()
}

func (fp *flakeIDGeneratorProxy) NewIDBatch(batchSize int32) (*flakeid.IDBatch, error) {
	request := proto.FlakeIDGeneratorNewIDBatchEncodeRequest(fp.name, batchSize)
	responseMessage, err := fp.invokeOnRandomTarget(request)
	if err != nil {
		return nil, err
	}
	base, increment, newBatchSize := proto.FlakeIDGeneratorNewIDBatchDecodeResponse(responseMessage)()
	return flakeid.NewIDBatch(base, increment, newBatchSize), nil
}
