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
	"sync"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/core/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/security"
)

const (
	authenticated = iota
	credentialsFailed
	serializationVersionMismatch
)

const serializationVersion = 1

var ClientVersion = "0.5-SNAPSHOT" //TODO This should be replace with a build time version variable, BuildInfo etc.

type connectionManager interface {
	//getActiveConnections returns a snapshot of active connections
	getActiveConnections() map[string]*Connection

	//getActiveConnection returns connection if available, nil otherwise
	getActiveConnection(address core.Address) *Connection

	//ConnectionCount returns number of active connections
	ConnectionCount() int32

	//getOrConnect returns associated connection if available, creates new connection otherwise
	getOrConnect(address core.Address, asOwner bool) (*Connection, error)

	//getOrTriggerConnect returns associated connection if available, returns error and triggers new connection creation otherwise
	getOrTriggerConnect(address core.Address) (*Connection, error)

	getOwnerConnection() *Connection

	addListener(listener connectionListener)
	onConnectionClose(connection *Connection, cause error)
	NextConnectionID() int64
	IsAlive() bool
	shutdown()
}

func (cm *connectionManagerImpl) addListener(listener connectionListener) {
	cm.listenerMutex.Lock()
	defer cm.listenerMutex.Unlock()
	if listener != nil {
		listeners := cm.connectionListeners.Load().([]connectionListener)
		size := len(listeners) + 1
		copyListeners := make([]connectionListener, size)
		copy(copyListeners, listeners)
		copyListeners[size-1] = listener
		cm.connectionListeners.Store(copyListeners)
	}
}

func (cm *connectionManagerImpl) getActiveConnection(address core.Address) *Connection {
	return cm.getConnection(address, false)
}

func (cm *connectionManagerImpl) ConnectionCount() int32 {
	cm.connectionsMutex.RLock()
	defer cm.connectionsMutex.RUnlock()
	return int32(len(cm.connections))
}

func (cm *connectionManagerImpl) getActiveConnections() map[string]*Connection {
	connections := make(map[string]*Connection)
	cm.connectionsMutex.RLock()
	defer cm.connectionsMutex.RUnlock()
	for k, v := range cm.connections {
		connections[k] = v
	}
	return connections
}

func (cm *connectionManagerImpl) onConnectionClose(connection *Connection, cause error) {
	//If Connection was authenticated fire event
	if address, ok := connection.endpoint.Load().(core.Address); ok {
		cm.connectionsMutex.Lock()
		delete(cm.connections, address.String())
		cm.connectionsMutex.Unlock()
		cm.notifyListenersForClose(connection, cause)
	} else {
		//Clean up unauthenticated Connection
		cm.client.InvocationService.cleanupConnection(connection, cause)
	}
}

func (cm *connectionManagerImpl) getOrTriggerConnect(address core.Address) (*Connection, error) {
	if cm.isAlive.Load() == false {
		return nil, core.NewHazelcastClientNotActiveError("Connection Manager is not active", nil)
	}
	connection := cm.getConnection(address, false)
	if connection != nil {
		return connection, nil
	}
	go cm.getOrCreateConnectionInternal(address, false)
	return nil, core.NewHazelcastIOError("No available connection to address "+address.String(), nil)
}

func (cm *connectionManagerImpl) getOrConnect(address core.Address, asOwner bool) (*Connection, error) {
	connection := cm.getConnection(address, asOwner)
	if connection != nil {
		return connection, nil
	}
	return cm.getOrCreateConnectionInternal(address, asOwner)
}

func (cm *connectionManagerImpl) getOwnerConnection() *Connection {
	ownerConnectionAddress := cm.client.ClusterService.getOwnerConnectionAddress()
	if ownerConnectionAddress == nil {
		return nil
	}
	return cm.getActiveConnection(ownerConnectionAddress)
}

func (cm *connectionManagerImpl) IsAlive() bool {
	return cm.isAlive.Load().(bool)
}

func (cm *connectionManagerImpl) shutdown() {
	if cm.isAlive.Load() == false {
		return
	}
	cm.isAlive.Store(false)
	activeCons := cm.getActiveConnections()
	for _, con := range activeCons {

		con.close(core.NewHazelcastClientNotActiveError("client is shutting down", nil))
	}
}

//internal definitions and methods called inside connection manager process

type connectionManagerImpl struct {
	client              *HazelcastClient
	connectionsMutex    sync.RWMutex
	connections         map[string]*Connection
	nextConnectionID    int64
	listenerMutex       sync.Mutex
	connectionListeners atomic.Value
	addressTranslator   AddressTranslator
	isAlive             atomic.Value
	credentials         security.Credentials
	logger              logger.Logger
}

func newConnectionManager(client *HazelcastClient, addressTranslator AddressTranslator) connectionManager {
	cm := connectionManagerImpl{
		client:            client,
		connections:       make(map[string]*Connection),
		addressTranslator: addressTranslator,
		credentials:       client.credentials,
		logger:            client.logger,
	}
	cm.connectionListeners.Store(make([]connectionListener, 0))
	cm.isAlive.Store(true)
	return &cm
}

func (cm *connectionManagerImpl) notifyListenersForClose(connection *Connection, cause error) {
	listeners := cm.connectionListeners.Load().([]connectionListener)
	for _, listener := range listeners {
		if _, ok := listener.(connectionListener); ok {
			listener.(connectionListener).onConnectionClosed(connection, cause)
		}
	}
}

func (cm *connectionManagerImpl) getConnection(address core.Address, asOwner bool) *Connection {
	cm.connectionsMutex.RLock()
	conn, found := cm.connections[address.String()]
	cm.connectionsMutex.RUnlock()

	if !found {
		return nil
	}
	if cm.doesMeetOwnerRequirement(asOwner, conn) {
		return nil
	}
	return conn
}

// following methods are called under same connectionsMutex writeLock
// only entry point is getOrCreateConnectionInternal
func (cm *connectionManagerImpl) getOrCreateConnectionInternal(address core.Address, asOwner bool) (*Connection, error) {
	cm.connectionsMutex.Lock()
	defer cm.connectionsMutex.Unlock()

	conn, found := cm.connections[address.String()]

	if !found {
		addr := cm.addressTranslator.Translate(address)
		if addr == nil {
			return nil, core.NewHazelcastNilPointerError("address translator could not translate address:"+
				address.String(), nil)
		}
		return cm.createConnection(addr, asOwner)
	}
	if cm.doesMeetOwnerRequirement(asOwner, conn) {
		err := cm.authenticate(conn, asOwner)
		return conn, err
	}
	return conn, nil
}

func (cm *connectionManagerImpl) doesMeetOwnerRequirement(asOwner bool, conn *Connection) bool {
	return asOwner && !conn.isOwnerConnection
}

func (cm *connectionManagerImpl) NextConnectionID() int64 {
	return atomic.AddInt64(&cm.nextConnectionID, 1)
}

func (cm *connectionManagerImpl) encodeAuthenticationRequest(asOwner bool) *proto.ClientMessage {
	if creds, ok := cm.credentials.(*security.UsernamePasswordCredentials); ok {
		return cm.createAuthenticationRequest(asOwner, creds)
	}
	return cm.createCustomAuthenticationRequest(asOwner)

}

func (cm *connectionManagerImpl) createAuthenticationRequest(asOwner bool,
	creds *security.UsernamePasswordCredentials) *proto.ClientMessage {
	uuid := cm.client.ClusterService.uuid.Load().(string)
	ownerUUID := cm.client.ClusterService.ownerUUID.Load().(string)
	return proto.ClientAuthenticationEncodeRequest(
		creds.Username(),
		creds.Password(),
		uuid,
		ownerUUID,
		asOwner,
		proto.ClientType,
		serializationVersion,
		ClientVersion,
	)
}

func (cm *connectionManagerImpl) createCustomAuthenticationRequest(asOwner bool) *proto.ClientMessage {
	uuid := cm.client.ClusterService.uuid.Load().(string)
	ownerUUID := cm.client.ClusterService.ownerUUID.Load().(string)
	credsData, err := cm.client.SerializationService.ToData(cm.credentials)
	if err != nil {
		cm.logger.Error("Credentials cannot be serialized!")
		return nil
	}
	return proto.ClientAuthenticationCustomEncodeRequest(
		credsData,
		uuid,
		ownerUUID,
		asOwner,
		proto.ClientType,
		serializationVersion,
		ClientVersion,
	)
}

func (cm *connectionManagerImpl) getAuthenticationDecoder() func(clientMessage *proto.ClientMessage) func() (
	status uint8, address *proto.Address,
	uuid string, ownerUuid string, serializationVersion uint8, serverHazelcastVersion string,
	clientUnregisteredMembers []*proto.Member) {
	var authenticationDecoder func(clientMessage *proto.ClientMessage) func() (status uint8, address *proto.Address,
		uuid string, ownerUuid string, serializationVersion uint8, serverHazelcastVersion string,
		clientUnregisteredMembers []*proto.Member)
	if _, ok := cm.credentials.(*security.UsernamePasswordCredentials); ok {
		authenticationDecoder = proto.ClientAuthenticationDecodeResponse
	} else {
		authenticationDecoder = proto.ClientAuthenticationCustomDecodeResponse
	}
	return authenticationDecoder
}

func (cm *connectionManagerImpl) authenticate(connection *Connection, asOwner bool) error {
	cm.credentials.SetEndpoint(connection.socket.LocalAddr().String())
	request := cm.encodeAuthenticationRequest(asOwner)
	invocationResult := cm.client.InvocationService.invokeOnConnection(request, connection)
	result, err := invocationResult.ResultWithTimeout(cm.client.HeartBeatService.heartBeatTimeout)
	if err != nil {
		return err
	}
	return cm.processAuthenticationResult(connection, asOwner, result)
}

func (cm *connectionManagerImpl) processAuthenticationResult(connection *Connection, asOwner bool,
	result *proto.ClientMessage) error {
	authenticationDecoder := cm.getAuthenticationDecoder()
	//status, address, uuid, ownerUUID, serializationVersion, serverHazelcastVersion , clientUnregisteredMembers
	status, address, uuid, ownerUUID, _, serverHazelcastVersion, _ := authenticationDecoder(result)()
	switch status {
	case authenticated:
		connection.setConnectedServerVersion(serverHazelcastVersion)
		connection.endpoint.Store(address)
		connection.isOwnerConnection = asOwner
		cm.connections[address.String()] = connection
		go cm.fireConnectionAddedEvent(connection)
		if asOwner {
			cm.client.ClusterService.ownerConnectionAddress.Store(address)
			cm.client.ClusterService.ownerUUID.Store(ownerUUID)
			cm.client.ClusterService.uuid.Store(uuid)
			cm.logger.Info("Setting ", connection, " as owner.")
		}
	case credentialsFailed:
		return core.NewHazelcastAuthenticationError("invalid credentials!", nil)
	case serializationVersionMismatch:
		return core.NewHazelcastAuthenticationError("serialization version mismatches with the server!", nil)
	}
	return nil
}

func (cm *connectionManagerImpl) fireConnectionAddedEvent(connection *Connection) {
	listeners := cm.connectionListeners.Load().([]connectionListener)
	for _, listener := range listeners {
		if _, ok := listener.(connectionListener); ok {
			listener.(connectionListener).onConnectionOpened(connection)
		}
	}
}

func (cm *connectionManagerImpl) createConnection(address core.Address, asOwner bool) (*Connection, error) {
	if err := cm.canCreateConnection(asOwner); err != nil {
		return nil, err
	}
	con, err := newConnection(address, cm, cm.client.InvocationService.handleResponse, cm.client.Config.NetworkConfig(),
		cm.logger)
	if err != nil {
		return nil, core.NewHazelcastTargetDisconnectedError(err.Error(), err)
	}
	err = cm.authenticate(con, asOwner)
	return con, err
}

func (cm *connectionManagerImpl) canCreateConnection(asOwner bool) error {
	if !asOwner && cm.client.ClusterService.getOwnerConnectionAddress() == nil {
		return core.NewHazelcastIllegalStateError("ownerConnection is not active", nil)
	}
	if cm.isAlive.Load() == false {
		return core.NewHazelcastClientNotActiveError("Connection Manager is not active", nil)
	}
	return nil
}

type connectionListener interface {
	onConnectionClosed(connection *Connection, cause error)
	onConnectionOpened(connection *Connection)
}
