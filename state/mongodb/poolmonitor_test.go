/*
Copyright 2026 The Dapr Authors
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

package mongodb

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/event"

	"github.com/dapr/kit/logger"
)

func TestPoolEventMessage(t *testing.T) {
	const addr = "mongo-0.example:27017"

	tests := []struct {
		name string
		evt  event.PoolEvent
		want string
	}{
		{
			name: "pool created",
			evt: event.PoolEvent{
				Type:        event.PoolCreated,
				Address:     addr,
				PoolOptions: &event.MonitorPoolOptions{MaxPoolSize: 100, MinPoolSize: 5},
			},
			want: "MongoDB connection pool event: type=ConnectionPoolCreated address=mongo-0.example:27017 maxPoolSize=100 minPoolSize=5",
		},
		{
			name: "pool ready",
			evt:  event.PoolEvent{Type: event.PoolReady, Address: addr},
			want: "MongoDB connection pool event: type=ConnectionPoolReady address=mongo-0.example:27017",
		},
		{
			name: "pool cleared",
			evt: event.PoolEvent{
				Type:         event.PoolCleared,
				Address:      addr,
				Interruption: true,
				Error:        errors.New("connection reset by peer"),
			},
			want: `MongoDB connection pool event: type=ConnectionPoolCleared address=mongo-0.example:27017 interruptInUseConnections=true error="connection reset by peer"`,
		},
		{
			name: "pool closed",
			evt:  event.PoolEvent{Type: event.PoolClosedEvent, Address: addr},
			want: "MongoDB connection pool event: type=ConnectionPoolClosed address=mongo-0.example:27017",
		},
		{
			name: "connection created",
			evt:  event.PoolEvent{Type: event.ConnectionCreated, Address: addr, ConnectionID: 7},
			want: "MongoDB connection pool event: type=ConnectionCreated address=mongo-0.example:27017 connectionID=7",
		},
		{
			name: "connection ready",
			evt: event.PoolEvent{
				Type:         event.ConnectionReady,
				Address:      addr,
				ConnectionID: 7,
				Duration:     185 * time.Millisecond,
			},
			want: "MongoDB connection pool event: type=ConnectionReady address=mongo-0.example:27017 connectionID=7 duration=185ms",
		},
		{
			name: "connection closed on error",
			evt: event.PoolEvent{
				Type:         event.ConnectionClosed,
				Address:      addr,
				ConnectionID: 3,
				Reason:       event.ReasonConnectionErrored,
				Error:        errors.New("i/o timeout"),
			},
			want: `MongoDB connection pool event: type=ConnectionClosed address=mongo-0.example:27017 connectionID=3 reason=connectionError error="i/o timeout"`,
		},
		{
			name: "connection closed idle",
			evt: event.PoolEvent{
				Type:         event.ConnectionClosed,
				Address:      addr,
				ConnectionID: 4,
				Reason:       event.ReasonIdle,
			},
			want: "MongoDB connection pool event: type=ConnectionClosed address=mongo-0.example:27017 connectionID=4 reason=idle",
		},
		{
			name: "check out failed",
			evt: event.PoolEvent{
				Type:     event.GetFailed,
				Address:  addr,
				Reason:   event.ReasonTimedOut,
				Duration: 2 * time.Second,
				Error:    errors.New("context deadline exceeded"),
			},
			want: `MongoDB connection pool event: type=ConnectionCheckOutFailed address=mongo-0.example:27017 reason=timeout duration=2s error="context deadline exceeded"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := poolEventMessage(&tt.evt)
			assert.True(t, ok)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("per-operation events are skipped", func(t *testing.T) {
		for _, typ := range []string{event.GetStarted, event.GetSucceeded, event.ConnectionReturned} {
			_, ok := poolEventMessage(&event.PoolEvent{Type: typ, Address: addr, ConnectionID: 1})
			assert.False(t, ok, typ)
		}
	})
}

func TestPoolMonitorLogLevel(t *testing.T) {
	evt := &event.PoolEvent{Type: event.ConnectionCreated, Address: "localhost:27017", ConnectionID: 1}

	t.Run("debug enabled", func(t *testing.T) {
		var buf bytes.Buffer
		log := logger.NewLogger("mongodb-poolmonitor-test-debug")
		log.SetOutput(&buf)
		log.SetOutputLevel(logger.DebugLevel)

		newPoolMonitor(log).Event(evt)
		assert.Contains(t, buf.String(), "type=ConnectionCreated address=localhost:27017 connectionID=1")
	})

	t.Run("debug disabled", func(t *testing.T) {
		var buf bytes.Buffer
		log := logger.NewLogger("mongodb-poolmonitor-test-info")
		log.SetOutput(&buf)
		log.SetOutputLevel(logger.InfoLevel)

		newPoolMonitor(log).Event(evt)
		assert.Empty(t, buf.String())
	})
}
