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

package core

const (
	// LifecycleStateStarting is fired when the client is starting.
	LifecycleStateStarting = "STARTING"

	// LifecycleStateStarted is fired when the client start is completed.
	LifecycleStateStarted = "STARTED"

	// LifecycleStateConnected is fired when the client is connected to a member.
	LifecycleStateConnected = "CONNECTED"

	// LifecycleStateDisconnected is fired when the client disconnected from a member.
	LifecycleStateDisconnected = "DISCONNECTED"

	// LifecycleStateShuttingDown is fired when the client is shutting down.
	LifecycleStateShuttingDown = "SHUTTING_DOWN"

	// LifecycleStateShutdown is fired when the client shutdown is completed.
	LifecycleStateShutdown = "SHUTDOWN"
)

// LifecycleService allows you to shutdown, terminate, and listen to lifecycle events
// on HazelcastInstance.
type LifecycleService interface {
	// AddLifecycleListener adds a listener object to listen for lifecycle events.
	// AddLifecycleListener returns the registrationID.
	AddLifecycleListener(listener interface{}) string

	// RemoveLifecycleListener removes lifecycle listener with the given registrationID.
	// RemoveLifecycleListener returns true if the listener is removed successfully, false otherwise.
	RemoveLifecycleListener(registrationID string) bool

	// IsRunning checks whether or not the client is running.
	IsRunning() bool
}
