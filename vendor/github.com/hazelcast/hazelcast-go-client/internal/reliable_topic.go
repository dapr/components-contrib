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
	"time"

	"sync/atomic"

	"sync"

	"strconv"

	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/internal/reliabletopic"
	"github.com/hazelcast/hazelcast-go-client/internal/util/iputil"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
)

const (
	topicRBPrefix  = "_hz_rb_"
	maxBackoff     = 2000 * time.Millisecond
	initialBackoff = 100 * time.Millisecond
)

type ReliableTopicProxy struct {
	*proxy
	ringBuffer           core.Ringbuffer
	serializationService spi.SerializationService
	topicOverLoadPolicy  core.TopicOverloadPolicy
	config               *config.ReliableTopicConfig
	msgProcessors        *sync.Map
}

func newReliableTopicProxy(client *HazelcastClient, serviceName string, name string) (*ReliableTopicProxy, error) {
	proxy := &ReliableTopicProxy{
		msgProcessors: new(sync.Map),
		proxy: &proxy{
			client:      client,
			serviceName: serviceName,
			name:        name,
		},
	}
	proxy.serializationService = client.SerializationService
	proxy.config = client.Config.GetReliableTopicConfig(name)
	proxy.topicOverLoadPolicy = proxy.config.TopicOverloadPolicy()
	var err error
	proxy.ringBuffer, err = client.GetRingbuffer(topicRBPrefix + name)
	return proxy, err
}

func (r *ReliableTopicProxy) AddMessageListener(messageListener core.MessageListener) (registrationID string, err error) {
	if messageListener == nil {
		return "", core.NewHazelcastNilPointerError(bufutil.NilListenerIsNotAllowed, nil)
	}
	uuid, _ := iputil.NewUUID()
	reliableMsgListener := r.toReliableMessageListener(messageListener)
	msgProcessor := newMessageProcessor(uuid, reliableMsgListener, r)
	r.msgProcessors.Store(uuid, msgProcessor)
	go msgProcessor.next()
	return uuid, nil
}

func (r *ReliableTopicProxy) toReliableMessageListener(messageListener core.MessageListener) core.ReliableMessageListener {
	if listener, ok := messageListener.(core.ReliableMessageListener); ok {
		return listener
	}
	return newReliableMessageListenerAdapter(messageListener)
}

func (r *ReliableTopicProxy) RemoveMessageListener(registrationID string) (removed bool, err error) {
	if msgProcessor, ok := r.msgProcessors.Load(registrationID); ok {
		msgProcessor := msgProcessor.(*messageProcessor)
		msgProcessor.cancel()
		return true, nil
	}
	return false, core.NewHazelcastIllegalArgumentError("no listener is found with the given id : "+
		registrationID, nil)
}

func (r *ReliableTopicProxy) Publish(message interface{}) (err error) {
	messageData, err := r.validateAndSerialize(message)
	if err != nil {
		return err
	}
	reliableMsg := reliabletopic.NewMessage(messageData, nil)
	switch r.topicOverLoadPolicy {
	case core.TopicOverLoadPolicyError:
		err = r.addOrFail(reliableMsg)
	case core.TopicOverLoadPolicyBlock:
		err = r.addWithBackoff(reliableMsg)
	case core.TopicOverLoadPolicyDiscardNewest:
		_, err = r.ringBuffer.Add(reliableMsg, core.OverflowPolicyFail)
	case core.TopicOverLoadPolicyDiscardOldest:
		err = r.addOrOverwrite(reliableMsg)
	}
	return err
}

func (r *ReliableTopicProxy) Destroy() (bool, error) {
	_, err := r.proxy.Destroy()
	if err != nil {
		return false, err
	}
	return r.ringBuffer.Destroy()
}

func (r *ReliableTopicProxy) addOrFail(message *reliabletopic.Message) (err error) {
	seqID, err := r.ringBuffer.Add(message, core.OverflowPolicyFail)
	if err != nil {
		return err
	}
	if seqID == -1 {
		// TODO :: add message to error string
		errorMsg := "failed to publish message to topic: " + r.name
		return core.NewHazelcastTopicOverflowError(errorMsg, nil)
	}
	return
}

func (r *ReliableTopicProxy) addWithBackoff(message *reliabletopic.Message) (err error) {
	sleepTime := initialBackoff
	for {
		result, err := r.ringBuffer.Add(message, core.OverflowPolicyFail)
		if err != nil {
			return err
		}
		if result != -1 {
			return nil
		}
		time.Sleep(sleepTime)
		sleepTime *= 2
		if sleepTime > maxBackoff {
			sleepTime = maxBackoff
		}
	}
}

func (r *ReliableTopicProxy) addOrOverwrite(message *reliabletopic.Message) (err error) {
	_, err = r.ringBuffer.Add(message, core.OverflowPolicyOverwrite)
	return
}
func (r *ReliableTopicProxy) Ringbuffer() core.Ringbuffer {
	return r.ringBuffer
}

type messageProcessor struct {
	id        string
	sequence  int64
	cancelled atomic.Value
	listener  core.ReliableMessageListener
	proxy     *ReliableTopicProxy
}

func newMessageProcessor(id string, listener core.ReliableMessageListener, proxy *ReliableTopicProxy) *messageProcessor {
	msgProcessor := &messageProcessor{
		id:       id,
		listener: listener,
		proxy:    proxy,
	}
	msgProcessor.cancelled.Store(false)
	initialSeq := listener.RetrieveInitialSequence()
	if initialSeq == -1 {
		// set initial seq as tail sequence + 1 so that we wont be reading the items that were added before the listener
		// is added.
		tailSeq, _ := msgProcessor.proxy.ringBuffer.TailSequence()
		initialSeq = tailSeq + 1
	}
	msgProcessor.sequence = initialSeq
	return msgProcessor
}

func (m *messageProcessor) next() {
	if m.cancelled.Load() == true {
		return
	}
	readResults, err := m.proxy.ringBuffer.ReadMany(m.sequence, 1,
		m.proxy.config.ReadBatchSize(), nil)
	if err == nil {
		m.onResponse(readResults)
	} else {
		m.onFailure(err)
	}
}

func (m *messageProcessor) onFailure(err error) {
	if m.cancelled.Load() == true {
		return
	}
	baseMsg := "Terminating Message Listener: " + m.id + " on topic: " + m.proxy.name + ". Reason: "
	if _, ok := err.(*core.HazelcastOperationTimeoutError); ok {
		m.handleOperationTimeoutError()
		return
	} else if _, ok := err.(*core.HazelcastIllegalArgumentError); ok && m.listener.IsLossTolerant() {
		m.handleIllegalArgumentError(err)
		return
	} else if hzErr, ok := err.(core.HazelcastError); ok &&
		hzErr.ServerError() != nil && hzErr.ServerError().ErrorCode() == int32(bufutil.ErrorCodeStaleSequence) {
		if m.handleStaleSequenceError() {
			return
		}

	} else if _, ok := err.(*core.HazelcastInstanceNotActiveError); ok {
		m.proxy.client.logger.Trace(baseMsg + "HazelcastInstance is shutting down.")
	} else if _, ok := err.(*core.HazelcastClientNotActiveError); ok {
		m.proxy.client.logger.Trace(baseMsg + "HazelcastClient is shutting down.")
	} else if hzErr, ok := err.(core.HazelcastError); ok && hzErr.ServerError() != nil &&
		hzErr.ServerError().ErrorCode() == int32(bufutil.ErrorCodeDistributedObjectDestroyed) {
		m.proxy.client.logger.Trace(baseMsg + "Topic is destroyed.")
	} else {
		m.proxy.client.logger.Warn(baseMsg + "Unhandled error, message:  " + err.Error())
	}

	m.cancel()
}

func (m *messageProcessor) handleIllegalArgumentError(err error) {
	headSeq, _ := m.proxy.ringBuffer.HeadSequence()
	m.proxy.client.logger.Warn("MessageListener ", m.id, " on topic ", m.proxy.name,
		" requested a too large sequence: ", err, ". Jumping from old sequence: ", m.sequence, " to sequence: ", headSeq)
	m.sequence = headSeq
	go m.next()
}

func (m *messageProcessor) handleOperationTimeoutError() {
	m.proxy.client.logger.Trace("Message Listener ", m.id, "on topic: ", m.proxy.name, " timed out. "+
		"Continuing from the last known sequence ", m.sequence)
	go m.next()
}

func (m *messageProcessor) handleStaleSequenceError() bool {
	headSeq, _ := m.proxy.ringBuffer.HeadSequence()
	if m.listener.IsLossTolerant() {
		msg := "Topic " + m.proxy.name + " ran into a stale sequence. Jumping from old sequence " +
			strconv.Itoa(int(m.sequence)) + " " +
			" to new sequence " + strconv.Itoa(int(headSeq))
		m.proxy.client.logger.Warn(msg)
		m.sequence = headSeq
		go m.next()
		return true
	}
	m.proxy.client.logger.Warn("Terminating Message Listener: "+m.id+" on topic: "+m.proxy.name+". Reason: "+
		"The listener was too slow or the retention period of the message has been violated. ",
		"Head: ", headSeq, " sequence: ", m.sequence)
	return false
}

func (m *messageProcessor) terminate(err error) bool {
	if m.cancelled.Load() == true {
		return true
	}
	baseMsg := "Terminating Message Listener: " + m.id + " on topic: " + m.proxy.name + ". Reason: "
	terminate, terminalErr := m.listener.IsTerminal(err)
	if terminalErr != nil {
		m.proxy.client.logger.Warn(baseMsg+"Unhandled error while calling ReliableMessageListener.isTerminal() method:", terminalErr)
		return true
	}
	if terminate {
		m.proxy.client.logger.Warn(baseMsg+"Unhandled error:", err)
	} else {
		m.proxy.client.logger.Trace("MessageListener ", m.id, " on topic:", m.proxy.name, " ran into an error:", err)
	}
	return terminate

}

func (m *messageProcessor) onResponse(readResults core.ReadResultSet) {
	// We process all messages in batch. So we don't release the thread and reschedule ourselves;
	// but we'll process whatever was received in 1 go.
	var i int32
	for ; i < readResults.Size(); i++ {
		item, err := readResults.Get(i)
		if m.cancelled.Load() == true {
			return
		}
		if msg, ok := item.(*reliabletopic.Message); ok && err == nil {
			m.listener.StoreSequence(m.sequence)
			err := m.process(msg)
			if err != nil && m.terminate(err) {
				m.cancel()
			}
		}
		m.sequence++
	}
	go m.next()
}

func (m *messageProcessor) process(message *reliabletopic.Message) error {
	return m.listener.OnMessage(m.toMessage(message))
}

func (m *messageProcessor) toMessage(message *reliabletopic.Message) core.Message {
	payload, _ := m.proxy.serializationService.ToObject(message.Payload())
	member := m.proxy.client.ClusterService.GetMember(message.PublisherAddress())
	return proto.NewTopicMessage(payload, message.PublishTime(), member)
}

func (m *messageProcessor) cancel() {
	m.cancelled.Store(true)
	m.proxy.msgProcessors.Delete(m.id)
}

type reliableMessageListenerAdapter struct {
	messageListener core.MessageListener
}

func newReliableMessageListenerAdapter(msgListener core.MessageListener) *reliableMessageListenerAdapter {
	return &reliableMessageListenerAdapter{
		messageListener: msgListener,
	}
}

func (d *reliableMessageListenerAdapter) OnMessage(message core.Message) error {
	return d.messageListener.OnMessage(message)
}

func (d *reliableMessageListenerAdapter) RetrieveInitialSequence() int64 {
	return -1
}

func (d *reliableMessageListenerAdapter) StoreSequence(sequence int64) {
	// no op
}

func (d *reliableMessageListenerAdapter) IsLossTolerant() bool {
	return false
}

func (d *reliableMessageListenerAdapter) IsTerminal(err error) (bool, error) {
	return false, nil
}
