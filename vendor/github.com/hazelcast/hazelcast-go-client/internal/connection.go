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
	"encoding/binary"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"crypto/tls"

	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/core/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/internal/util/timeutil"
	"github.com/hazelcast/hazelcast-go-client/internal/util/versionutil"
)

const (
	kb              = 1024
	bufferSize      = 128 * kb
	protocolStarter = "CB2"
)

type Connection struct {
	pending                   chan *proto.ClientMessage
	received                  chan *proto.ClientMessage
	socket                    net.Conn
	clientMessageBuilder      *clientMessageBuilder
	closed                    chan struct{}
	endpoint                  atomic.Value
	status                    int32
	isOwnerConnection         bool
	lastRead                  atomic.Value
	lastWrite                 atomic.Value
	closedTime                atomic.Value
	readBuffer                []byte
	connectionID              int64
	connectionManager         connectionManager
	connectedServerVersion    int32
	connectedServerVersionStr string
	startTime                 int64
	logger                    logger.Logger
}

func newConnection(address core.Address, cm connectionManager, handleResponse func(interface{}),
	networkCfg *config.NetworkConfig, logger logger.Logger) (*Connection, error) {
	connection := createDefaultConnection(cm, handleResponse, logger)
	socket, err := connection.createSocket(networkCfg, address)
	if err != nil {
		return nil, err
	}
	connection.socket = socket
	connection.init()
	connection.sendProtocolStarter()
	go connection.writePool()
	go connection.readPool()
	return connection, nil
}

func createDefaultConnection(cm connectionManager, handleResponse func(interface{}), logger logger.Logger) *Connection {

	builder := &clientMessageBuilder{
		handleResponse:     handleResponse,
		incompleteMessages: make(map[int64]*proto.ClientMessage),
	}
	return &Connection{
		pending:              make(chan *proto.ClientMessage, 1),
		received:             make(chan *proto.ClientMessage, 1),
		closed:               make(chan struct{}),
		clientMessageBuilder: builder,
		readBuffer:           make([]byte, 0),
		connectionID:         cm.NextConnectionID(),
		connectionManager:    cm,
		status:               0,
		logger:               logger,
	}
}

func (c *Connection) sendProtocolStarter() {
	c.socket.Write([]byte(protocolStarter))
}

func (c *Connection) createSocket(networkCfg *config.NetworkConfig, address core.Address) (net.Conn, error) {
	conTimeout := timeutil.GetPositiveDurationOrMax(networkCfg.ConnectionTimeout())
	socket, err := c.dialToAddressWithTimeout(address, conTimeout)
	if err != nil {
		return nil, err
	}
	if networkCfg.SSLConfig().Enabled() {
		socket, err = c.openTLSConnection(networkCfg.SSLConfig(), socket)
	}
	return socket, err
}

func (c *Connection) dialToAddressWithTimeout(address core.Address, conTimeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", address.String(), conTimeout)
}

func (c *Connection) init() {
	c.lastWrite.Store(time.Time{})
	c.closedTime.Store(time.Time{})
	c.startTime = timeutil.GetCurrentTimeInMilliSeconds()
	c.lastRead.Store(time.Now())
}

func (c *Connection) openTLSConnection(sslCfg *config.SSLConfig, conn net.Conn) (net.Conn, error) {
	tlsCon := tls.Client(conn, sslCfg.Config)
	err := tlsCon.Handshake()
	return tlsCon, err
}

func (c *Connection) isAlive() bool {
	return atomic.LoadInt32(&c.status) == 0
}

func (c *Connection) writePool() {
	//Writer process
	for {
		select {
		case request := <-c.pending:
			err := c.write(request)
			if err != nil {
				c.clientMessageBuilder.handleResponse(request.CorrelationID())
			} else {
				c.lastWrite.Store(time.Now())
			}
		case <-c.closed:
			return
		}
	}
}

func (c *Connection) send(clientMessage *proto.ClientMessage) bool {
	select {
	case <-c.closed:
		return false
	case c.pending <- clientMessage:
		return true

	}
}

func (c *Connection) write(clientMessage *proto.ClientMessage) error {
	remainingLen := len(clientMessage.Buffer)
	writeIndex := 0
	for remainingLen > 0 {
		writtenLen, err := c.socket.Write(clientMessage.Buffer[writeIndex:])
		if err != nil {
			return err
		}
		remainingLen -= writtenLen
		writeIndex += writtenLen
	}

	return nil
}

func (c *Connection) readPool() {
	buf := make([]byte, bufferSize)
	for {
		c.socket.SetDeadline(time.Now().Add(2 * time.Second))
		n, err := c.socket.Read(buf)
		if !c.isAlive() {
			return
		}
		if err != nil {
			if c.isTimeoutError(err) {
				continue
			}
			c.close(err)
			return
		}
		if n == 0 {
			continue
		}
		c.readBuffer = append(c.readBuffer, buf[:n]...)
		c.receiveMessage()
	}
}

func (c *Connection) isTimeoutError(err error) bool {
	netErr, ok := err.(net.Error)
	return ok && netErr.Timeout()
}

func (c *Connection) StartTime() int64 {
	return c.startTime
}

func (c *Connection) receiveMessage() {
	c.lastRead.Store(time.Now())
	for len(c.readBuffer) > bufutil.Int32SizeInBytes {
		frameLength := binary.LittleEndian.Uint32(c.readBuffer[0:bufutil.Int32SizeInBytes])
		if frameLength > uint32(len(c.readBuffer)) {
			return
		}
		resp := proto.NewClientMessage(c.readBuffer[:frameLength], 0)
		c.readBuffer = c.readBuffer[frameLength:]
		c.clientMessageBuilder.onMessage(resp)
	}
}

func (c *Connection) localAddress() net.Addr {
	return c.socket.LocalAddr()
}

func (c *Connection) setConnectedServerVersion(connectedServerVersion string) {
	c.connectedServerVersionStr = connectedServerVersion
	c.connectedServerVersion = versionutil.CalculateVersion(connectedServerVersion)
}

func (c *Connection) close(err error) {
	if !atomic.CompareAndSwapInt32(&c.status, 0, 1) {
		return
	}
	c.logger.Warn("Connection :", c, " closed, err: ", err)
	close(c.closed)
	c.socket.Close()
	c.closedTime.Store(time.Now())
	c.connectionManager.onConnectionClose(c, core.NewHazelcastTargetDisconnectedError(err.Error(), err))
}

func (c *Connection) String() string {
	return fmt.Sprintf("ClientConnection{"+
		"isAlive=%t"+
		", connectionID=%d"+
		", endpoint=%s"+
		", lastReadTime=%s"+
		", lastWriteTime=%s"+
		", closedTime=%s"+
		", connected server version=%s", c.isAlive(), c.connectionID,
		c.endpoint.Load().(core.Address),
		c.lastRead.Load().(time.Time), c.lastWrite.Load().(time.Time),
		c.closedTime.Load().(time.Time), c.connectedServerVersionStr)
}
