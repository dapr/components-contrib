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
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/event"

	"github.com/dapr/kit/logger"
)

// newPoolMonitor returns a driver pool monitor that logs connection pool
// events at debug level.
func newPoolMonitor(log logger.Logger) *event.PoolMonitor {
	return &event.PoolMonitor{
		Event: func(evt *event.PoolEvent) {
			if !log.IsOutputLevelEnabled(logger.DebugLevel) {
				return
			}
			if msg, ok := poolEventMessage(evt); ok {
				log.Debug(msg)
			}
		},
	}
}

// poolEventMessage returns the log message for a pool event, or false if the
// event is not logged. Check-out started, checked-out and checked-in events
// fire on every operation and are skipped.
func poolEventMessage(evt *event.PoolEvent) (string, bool) {
	var b strings.Builder
	fmt.Fprintf(&b, "MongoDB connection pool event: type=%s address=%s", evt.Type, evt.Address)

	switch evt.Type {
	case event.PoolCreated:
		if evt.PoolOptions != nil {
			fmt.Fprintf(&b, " maxPoolSize=%d minPoolSize=%d", evt.PoolOptions.MaxPoolSize, evt.PoolOptions.MinPoolSize)
		}
	case event.PoolReady, event.PoolClosedEvent:
	case event.PoolCleared:
		fmt.Fprintf(&b, " interruptInUseConnections=%t", evt.Interruption)
	case event.ConnectionCreated:
		fmt.Fprintf(&b, " connectionID=%d", evt.ConnectionID)
	case event.ConnectionReady:
		fmt.Fprintf(&b, " connectionID=%d duration=%s", evt.ConnectionID, evt.Duration)
	case event.ConnectionClosed:
		fmt.Fprintf(&b, " connectionID=%d reason=%s", evt.ConnectionID, evt.Reason)
	case event.GetFailed:
		fmt.Fprintf(&b, " reason=%s duration=%s", evt.Reason, evt.Duration)
	default:
		return "", false
	}

	if evt.Error != nil {
		fmt.Fprintf(&b, " error=%q", evt.Error.Error())
	}

	return b.String(), true
}
