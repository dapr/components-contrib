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
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type topicProxy struct {
	*partitionSpecificProxy
}

func newTopicProxy(client *HazelcastClient, serviceName string, name string) *topicProxy {
	parSpecProxy := newPartitionSpecificProxy(client, serviceName, name)
	return &topicProxy{parSpecProxy}
}

func (tp *topicProxy) AddMessageListener(messageListener core.MessageListener) (registrationID string, err error) {
	request := proto.TopicAddMessageListenerEncodeRequest(tp.name, false)
	eventHandler := tp.createEventHandler(messageListener)

	return tp.client.ListenerService.registerListener(request, eventHandler,
		func(registrationID string) *proto.ClientMessage {
			return proto.TopicRemoveMessageListenerEncodeRequest(tp.name, registrationID)
		}, func(clientMessage *proto.ClientMessage) string {
			return proto.TopicAddMessageListenerDecodeResponse(clientMessage)()
		})

}

func (tp *topicProxy) RemoveMessageListener(registrationID string) (removed bool, err error) {
	return tp.client.ListenerService.deregisterListener(registrationID, func(registrationID string) *proto.ClientMessage {
		return proto.TopicRemoveMessageListenerEncodeRequest(tp.name, registrationID)
	})
}

func (tp *topicProxy) Publish(message interface{}) (err error) {
	messageData, err := tp.validateAndSerialize(message)
	if err != nil {
		return err
	}
	request := proto.TopicPublishEncodeRequest(tp.name, messageData)
	_, err = tp.invoke(request)
	return
}

func (tp *topicProxy) createEventHandler(messageListener core.MessageListener) func(clientMessage *proto.ClientMessage) {
	return func(message *proto.ClientMessage) {
		proto.TopicAddMessageListenerHandle(message, func(itemData serialization.Data, publishTime int64, uuid string) {
			member := tp.client.ClusterService.GetMemberByUUID(uuid)
			item, _ := tp.toObject(itemData)
			itemEvent := proto.NewTopicMessage(item, publishTime, member)
			err := messageListener.OnMessage(itemEvent)
			if err != nil {
				tp.client.logger.Warn("Error while handling the message in MessageListener OnMessage: ", err)
			}
		})
	}
}
