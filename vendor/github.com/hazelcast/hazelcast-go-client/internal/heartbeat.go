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

	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/core/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

type heartBeatService struct {
	client            *HazelcastClient
	heartBeatTimeout  time.Duration
	heartBeatInterval time.Duration
	cancel            chan struct{}
	logger            logger.Logger
}

func newHeartBeatService(client *HazelcastClient) *heartBeatService {
	heartBeat := heartBeatService{client: client,
		heartBeatInterval: client.properties.GetPositiveDurationOrDef(property.HeartbeatInterval),
		heartBeatTimeout:  client.properties.GetPositiveDurationOrDef(property.HeartbeatTimeout),
		cancel:            make(chan struct{}),
		logger:            client.logger,
	}

	return &heartBeat
}

func (hbs *heartBeatService) start() {
	go func() {
		ticker := time.NewTicker(hbs.heartBeatInterval)
		for {
			if !hbs.client.lifecycleService.isLive.Load().(bool) {
				return
			}
			select {
			case <-ticker.C:
				hbs.heartBeat()
			case <-hbs.cancel:
				ticker.Stop()
				return
			}
		}
	}()
}

func (hbs *heartBeatService) heartBeat() {
	for _, connection := range hbs.client.ConnectionManager.getActiveConnections() {
		if !connection.isAlive() {
			continue
		}
		timeSinceLastRead := time.Since(connection.lastRead.Load().(time.Time))
		timeSinceLastWrite := time.Since(connection.lastWrite.Load().(time.Time))
		if timeSinceLastRead > hbs.heartBeatTimeout {
			if connection.isAlive() {
				hbs.logger.Warn("Heartbeat failed over the connection: ", connection)
				hbs.onHeartbeatStopped(connection)
			}
		}
		if timeSinceLastWrite > hbs.heartBeatInterval {
			request := proto.ClientPingEncodeRequest()
			sentInvocation := hbs.client.InvocationService.invokeOnConnection(request, connection)
			go func(con *Connection) {
				_, err := sentInvocation.Result()
				if err != nil {
					hbs.logger.Debug("Error when receiving heartbeat for connection, ", con)
				}
			}(connection)
		}
	}
}

func (hbs *heartBeatService) onHeartbeatStopped(connection *Connection) {
	connection.close(core.NewHazelcastTargetDisconnectedError("heartbeat timed out to connection "+
		connection.String(), nil))
}

func (hbs *heartBeatService) shutdown() {
	close(hbs.cancel)
}
