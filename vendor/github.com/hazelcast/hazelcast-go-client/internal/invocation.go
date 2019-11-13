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
	"fmt"
	"sync/atomic"
	"time"

	"sync"

	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/core/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type invocation struct {
	request         atomic.Value
	response        chan interface{}
	isComplete      int32
	boundConnection *Connection
	address         core.Address
	partitionID     int32
	sentConnection  atomic.Value
	eventHandler    func(clientMessage *proto.ClientMessage)
	deadline        time.Time
}

type invocationResult interface {
	Result() (*proto.ClientMessage, error)
	ResultWithTimeout(duration time.Duration) (*proto.ClientMessage, error)
}

func newInvocation(request *proto.ClientMessage, partitionID int32, address core.Address,
	connection *Connection, client *HazelcastClient) *invocation {
	invocation := &invocation{
		partitionID:     partitionID,
		address:         address,
		boundConnection: connection,
		response:        make(chan interface{}, 1),
		isComplete:      0,
		deadline:        time.Now().Add(client.InvocationService.InvocationTimeout()),
	}
	invocation.request.Store(request)
	return invocation
}

func (i *invocation) isBoundToSingleConnection() bool {
	return i.boundConnection != nil
}

func (i *invocation) Result() (*proto.ClientMessage, error) {
	response := <-i.response
	return i.unwrapResponse(response)
}

func (i *invocation) complete(response interface{}) {
	if atomic.CompareAndSwapInt32(&i.isComplete, 0, 1) {
		i.response <- response
	}

}

func (i *invocation) unwrapResponse(response interface{}) (*proto.ClientMessage, error) {
	switch res := response.(type) {
	case *proto.ClientMessage:
		return res, nil
	case error:
		return nil, res
	default:
		panic("Unexpected response in invocation ")
	}
}

func (i *invocation) ResultWithTimeout(duration time.Duration) (*proto.ClientMessage, error) {
	select {
	case response := <-i.response:
		return i.unwrapResponse(response)
	case <-time.After(duration):
		return nil, core.NewHazelcastOperationTimeoutError("invocation timed out after "+duration.String(), nil)
	}
}

type invocationService interface {
	invokeOnPartitionOwner(message *proto.ClientMessage, partitionID int32) invocationResult
	invokeOnRandomTarget(message *proto.ClientMessage) invocationResult
	invokeOnKeyOwner(message *proto.ClientMessage, data serialization.Data) invocationResult
	invokeOnTarget(message *proto.ClientMessage, address core.Address) invocationResult
	invokeOnConnection(message *proto.ClientMessage, connection *Connection) invocationResult
	cleanupConnection(connection *Connection, e error)
	removeEventHandler(correlationID int64)
	sendInvocation(invocation *invocation) invocationResult
	InvocationTimeout() time.Duration
	handleResponse(response interface{})
	shutdown()
}

func (is *invocationServiceImpl) invokeOnPartitionOwner(request *proto.ClientMessage, partitionID int32) invocationResult {
	invocation := newInvocation(request, partitionID, nil, nil, is.client)
	return is.sendInvocation(invocation)
}

func (is *invocationServiceImpl) invokeOnRandomTarget(request *proto.ClientMessage) invocationResult {
	invocation := newInvocation(request, -1, nil, nil, is.client)
	return is.sendInvocation(invocation)
}

func (is *invocationServiceImpl) invokeOnKeyOwner(request *proto.ClientMessage, keyData serialization.Data) invocationResult {
	partitionID := is.client.PartitionService.GetPartitionID(keyData)
	return is.invokeOnPartitionOwner(request, partitionID)
}

func (is *invocationServiceImpl) invokeOnTarget(request *proto.ClientMessage, target core.Address) invocationResult {
	invocation := newInvocation(request, -1, target, nil, is.client)
	return is.sendInvocation(invocation)
}

func (is *invocationServiceImpl) invokeOnConnection(request *proto.ClientMessage, connection *Connection) invocationResult {
	invocation := newInvocation(request, -1, nil, connection, is.client)
	return is.sendInvocation(invocation)
}

func (is *invocationServiceImpl) cleanupConnection(connection *Connection, cause error) {
	is.invocationsLock.Lock()
	defer is.invocationsLock.Unlock()
	for _, invocation := range is.invocations {
		sentConnection, ok := invocation.sentConnection.Load().(*Connection)
		if ok && sentConnection == connection {
			is.unRegisterInvocationWithoutLock(invocation.request.Load().(*proto.ClientMessage).CorrelationID())
			is.handleError(invocation, cause)
		}
	}
}

func (is *invocationServiceImpl) removeEventHandler(correlationID int64) {
	is.eventHandlersLock.Lock()
	defer is.eventHandlersLock.Unlock()
	if _, ok := is.eventHandlers[correlationID]; ok {
		delete(is.eventHandlers, correlationID)
	}
}

func (is *invocationServiceImpl) sendInvocation(invocation *invocation) invocationResult {
	if is.isShutdown.Load() == true {
		invocation.complete(core.NewHazelcastClientNotActiveError("client is shut down", nil))
	}
	is.registerInvocation(invocation)
	is.invoke(invocation)

	return invocation
}

func (is *invocationServiceImpl) retryInvocation(invocation *invocation, cause error) {
	if is.isShutdown.Load() == true {
		invocation.complete(core.NewHazelcastClientNotActiveError("client is shut down", cause))
	}
	// retryInvocation modifies the client message and should not reuse the client message.
	// It could be the case that it is in write queue of the connection.
	invocation.request.Store(invocation.request.Load().(*proto.ClientMessage).CloneMessage())
	is.registerInvocation(invocation)
	is.invoke(invocation)
}

func (is *invocationServiceImpl) shutdown() {
	is.responseChannel <- struct{}{}
	is.isShutdown.Store(true)

	is.invocationsLock.Lock()
	defer is.invocationsLock.Unlock()
	for correlationID, invocation := range is.invocations {
		delete(is.invocations, correlationID)
		invocation.complete(core.NewHazelcastClientNotActiveError("client is shutting down", nil))
	}

}

func (is *invocationServiceImpl) onConnectionClosed(connection *Connection, cause error) {
	is.cleanupConnection(connection, cause)
}

func (is *invocationServiceImpl) onConnectionOpened(connection *Connection) {
}

func (is *invocationServiceImpl) handleResponse(response interface{}) {
	if is.isShutdown.Load() == true {
		return
	}
	is.responseChannel <- response
}

//internal definitions and methods called inside service process

type invocationServiceImpl struct {
	client            *HazelcastClient
	nextCorrelation   int64
	invocationsLock   sync.RWMutex
	invocations       map[int64]*invocation
	invocationTimeout time.Duration
	retryPause        time.Duration
	eventHandlersLock sync.RWMutex
	eventHandlers     map[int64]*invocation
	responseChannel   chan interface{}
	invoke            func(*invocation)
	isShutdown        atomic.Value
	logger            logger.Logger
}

func newInvocationService(client *HazelcastClient) *invocationServiceImpl {
	service := &invocationServiceImpl{
		client:          client,
		invocations:     make(map[int64]*invocation),
		eventHandlers:   make(map[int64]*invocation),
		responseChannel: make(chan interface{}, 1),
		logger:          client.logger,
	}
	service.initInvocationTimeout()
	service.initRetryPause()
	service.isShutdown.Store(false)
	if client.Config.NetworkConfig().IsSmartRouting() {
		service.invoke = service.invokeSmart
	} else {
		service.invoke = service.invokeNonSmart
	}
	go service.process()
	service.client.ConnectionManager.addListener(service)
	return service
}

func (is *invocationServiceImpl) InvocationTimeout() time.Duration {
	return is.invocationTimeout
}

func (is *invocationServiceImpl) initInvocationTimeout() {
	is.invocationTimeout = is.client.properties.GetPositiveDurationOrDef(property.InvocationTimeoutSeconds)
}

func (is *invocationServiceImpl) initRetryPause() {
	is.retryPause = is.client.properties.GetPositiveDurationOrDef(property.InvocationRetryPause)
}

func (is *invocationServiceImpl) process() {
	for command := range is.responseChannel {
		switch resp := command.(type) {
		case *proto.ClientMessage:
			is.handleClientMessage(resp)
		case int64:
			is.handleNotSentInvocation(resp, core.NewHazelcastIOError("packet is not sent", nil))
		case struct{}:
			return
		default:
			panic(fmt.Sprintf("Unexpected command from response channel %s", command))
		}
	}
}

func (is *invocationServiceImpl) nextCorrelationID() int64 {
	return atomic.AddInt64(&is.nextCorrelation, 1)
}

func (is *invocationServiceImpl) getNextAddress() core.Address {
	member := is.client.LoadBalancer.Next()
	if member != nil {
		return member.Address()
	}
	return nil
}

func (is *invocationServiceImpl) sendToRandomAddress(invocation *invocation) {
	var target = is.getNextAddress()
	if target == nil {
		is.handleNotSentInvocation(invocation.request.Load().(*proto.ClientMessage).CorrelationID(),
			core.NewHazelcastIOError("no address found to invoke", nil))
		return
	}
	is.sendToAddress(invocation, target)
}

func (is *invocationServiceImpl) invokeSmart(invocation *invocation) {
	if invocation.boundConnection != nil {
		is.sendToConnection(invocation, invocation.boundConnection)
	} else if invocation.partitionID != -1 {
		if target, ok := is.client.PartitionService.partitionOwner(invocation.partitionID); ok {
			is.sendToAddress(invocation, target)
		} else {
			is.handleNotSentInvocation(invocation.request.Load().(*proto.ClientMessage).CorrelationID(),
				core.NewHazelcastIOError(fmt.Sprintf("partition does not have an owner. partitionID: %d", invocation.partitionID), nil))
		}
	} else if invocation.address != nil {
		is.sendToAddress(invocation, invocation.address)
	} else {
		is.sendToRandomAddress(invocation)
	}
}

func (is *invocationServiceImpl) invokeNonSmart(invocation *invocation) {
	if invocation.boundConnection != nil {
		is.sendToConnection(invocation, invocation.boundConnection)
	} else {
		address := is.client.ClusterService.getOwnerConnectionAddress()
		if address == nil {
			is.handleNotSentInvocation(invocation.request.Load().(*proto.ClientMessage).CorrelationID(),
				core.NewHazelcastIOError("no address found to invoke", nil))
			return
		}
		is.sendToAddress(invocation, address)
	}
}

func (is *invocationServiceImpl) sendToConnection(invocation *invocation, connection *Connection) {
	sent := connection.send(invocation.request.Load().(*proto.ClientMessage))
	if !sent {
		is.handleNotSentInvocation(invocation.request.Load().(*proto.ClientMessage).CorrelationID(),
			core.NewHazelcastIOError("packet is not sent", nil))
	} else {
		invocation.sentConnection.Store(connection)
	}

}

func (is *invocationServiceImpl) sendToAddress(invocation *invocation, address core.Address) {
	connection, err := is.client.ConnectionManager.getOrTriggerConnect(address)
	if err != nil {
		is.logger.Trace("Sending invocation to ", address, " failed, err: ", err)
		is.handleNotSentInvocation(invocation.request.Load().(*proto.ClientMessage).CorrelationID(), err)
		return
	}
	is.sendToConnection(invocation, connection)
}

func (is *invocationServiceImpl) registerInvocation(invocation *invocation) {
	message := invocation.request.Load().(*proto.ClientMessage)
	correlationID := is.nextCorrelationID()
	message.SetCorrelationID(correlationID)
	message.SetPartitionID(invocation.partitionID)
	message.SetFlags(bufutil.BeginEndFlag)
	if invocation.eventHandler != nil {
		is.eventHandlersLock.Lock()
		is.eventHandlers[correlationID] = invocation
		is.eventHandlersLock.Unlock()
	}
	is.invocationsLock.Lock()
	is.invocations[correlationID] = invocation
	is.invocationsLock.Unlock()
}

func (is *invocationServiceImpl) unRegisterInvocation(correlationID int64) (*invocation, bool) {
	is.invocationsLock.Lock()
	defer is.invocationsLock.Unlock()
	return is.unRegisterInvocationWithoutLock(correlationID)
}

func (is *invocationServiceImpl) unRegisterInvocationWithoutLock(correlationID int64) (*invocation, bool) {
	if invocation, ok := is.invocations[correlationID]; ok {
		delete(is.invocations, correlationID)
		return invocation, ok
	}
	return nil, false
}

func (is *invocationServiceImpl) handleNotSentInvocation(correlationID int64, cause error) {
	if invocation, ok := is.unRegisterInvocation(correlationID); ok {
		is.handleError(invocation, cause)
	} else {
		is.logger.Trace("No invocation has been found with the correlation id: ", correlationID)
	}
}

func (is *invocationServiceImpl) handleClientMessage(response *proto.ClientMessage) {
	correlationID := response.CorrelationID()
	if response.HasFlags(bufutil.ListenerFlag) > 0 {
		is.eventHandlersLock.RLock()
		invocation, found := is.eventHandlers[correlationID]
		is.eventHandlersLock.RUnlock()
		if !found {
			is.logger.Trace("Got an event message with unknown correlation id: ", correlationID)
		} else {
			invocation.eventHandler(response)
		}
		return
	}

	if invocation, ok := is.unRegisterInvocation(correlationID); ok {
		if response.MessageType() == bufutil.MessageTypeException {
			err := createHazelcastError(convertToError(response))
			is.handleError(invocation, err)
		} else {
			invocation.complete(response)
		}
	} else {
		is.logger.Trace("No invocation has been found with the correlation id: ", correlationID)
	}
}

func convertToError(clientMessage *proto.ClientMessage) *proto.ServerError {
	return proto.ErrorCodecDecode(clientMessage)
}

func (is *invocationServiceImpl) logError(invocation *invocation, err error) {
	correlationID := invocation.request.Load().(*proto.ClientMessage).CorrelationID()
	is.logger.Trace("Invocation with correlation id: ", correlationID, " got an error: ", err)
}

func (is *invocationServiceImpl) handleError(invocation *invocation, err error) {
	is.logError(invocation, err)
	if !is.client.lifecycleService.isLive.Load().(bool) {
		invocation.complete(core.NewHazelcastClientNotActiveError("client is shutdown", err))
		return
	}
	if is.isNotAllowedToRetryOnConnection(invocation, err) {
		invocation.complete(err)
		return
	}

	if time.Now().After(invocation.deadline) {
		timeSinceDeadline := time.Since(invocation.deadline)
		is.logger.Trace("Invocation will not be retried because it timed out by ", timeSinceDeadline.String())
		invocation.complete(core.NewHazelcastOperationTimeoutError("invocation timed out by "+timeSinceDeadline.String(), err))
		return
	}
	if is.shouldRetryInvocation(invocation, err) {
		time.AfterFunc(is.retryPause, func() {
			is.retryInvocation(invocation, err)
		})
		return
	}
	invocation.complete(err)
}

func (is *invocationServiceImpl) isRedoOperation() bool {
	return is.client.Config.NetworkConfig().IsRedoOperation()
}

func (is *invocationServiceImpl) shouldRetryInvocation(invocation *invocation, err error) bool {
	_, isTargetDisconnectedError := err.(*core.HazelcastTargetDisconnectedError)
	if (isTargetDisconnectedError && invocation.request.Load().(*proto.ClientMessage).IsRetryable) ||
		is.isRedoOperation() || isRetrySafeError(err) {
		return true
	}
	return false
}

func isRetrySafeError(err error) bool {
	var isRetrySafe = false
	_, ok := err.(*core.HazelcastInstanceNotActiveError)
	isRetrySafe = isRetrySafe || ok
	_, ok = err.(*core.HazelcastTargetNotMemberError)
	isRetrySafe = isRetrySafe || ok
	_, ok = err.(*core.HazelcastIOError)
	isRetrySafe = isRetrySafe || ok
	return isRetrySafe
}

func (is *invocationServiceImpl) isNotAllowedToRetryOnConnection(invocation *invocation, err error) bool {
	_, isIOError := err.(*core.HazelcastIOError)
	if invocation.isBoundToSingleConnection() && isIOError {
		return true
	}
	_, isTargetNotMemberError := err.(*core.HazelcastTargetNotMemberError)
	if invocation.address != nil && isTargetNotMemberError && is.client.ClusterService.GetMember(invocation.address) == nil {
		return true
	}
	return false
}
