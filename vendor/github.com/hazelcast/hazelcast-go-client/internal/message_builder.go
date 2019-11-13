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
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
)

type clientMessageBuilder struct {
	incompleteMessages map[int64]*proto.ClientMessage
	handleResponse     func(interface{})
}

func (mb *clientMessageBuilder) onMessage(msg *proto.ClientMessage) {
	if msg.HasFlags(bufutil.BeginEndFlag) > 0 {
		mb.handleResponse(msg)
	} else if msg.HasFlags(bufutil.BeginFlag) > 0 {
		mb.incompleteMessages[msg.CorrelationID()] = msg
	} else {
		message, found := mb.incompleteMessages[msg.CorrelationID()]
		if !found {
			return
		}
		message.Accumulate(msg)
		if msg.HasFlags(bufutil.EndFlag) > 0 {
			message.AddFlags(bufutil.BeginEndFlag)
			mb.handleResponse(message)
			delete(mb.incompleteMessages, msg.CorrelationID())
		}
	}
}
