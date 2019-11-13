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

package property

import "time"

var (

	// HeartbeatTimeout is the duration for client sending heartbeat messages to the members.
	// If there is not any message passing between the client and member within the given time via this property,
	// the connection will be closed.
	HeartbeatTimeout = NewHazelcastPropertyInt64WithTimeUnit("hazelcast.client.heartbeat.timeout", 60000,
		time.Millisecond)

	// HeartbeatInterval is the time interval between the heartbeats sent by the client to the nodes.
	HeartbeatInterval = NewHazelcastPropertyInt64WithTimeUnit("hazelcast.client.heartbeat.interval", 5000,
		time.Millisecond)

	// InvocationTimeoutSeconds is used when an invocation gets an error because :
	//  * Member throws an exception.
	//  * Connection between the client and member is closed.
	//  * Client's heartbeat requests are timed out.
	// Time passed since invocation started is compared with this property.
	// If the time is already passed, then the error is delegated to the user. If not, the invocation is retried.
	// Note that, if invocation gets no error and it is a long running one, then it will not get any error,
	// no matter how small this timeout is set.
	InvocationTimeoutSeconds = NewHazelcastPropertyInt64WithTimeUnit("hazelcast.client.invocation.timeout.seconds",
		120, time.Second)

	// InvocationRetryPause time is the pause time between each retry cycle of an invocation in milliseconds.
	InvocationRetryPause = NewHazelcastPropertyInt64WithTimeUnit("hazelcast.client.invocation.retry.pause.millis",
		1000, time.Millisecond)

	// HazelcastCloudDiscoveryToken is a token to use discovering cluster via hazelcast.cloud.
	HazelcastCloudDiscoveryToken = NewHazelcastProperty("hazelcast.client.cloud.discovery.token")

	// StatisticsPeriodSeconds is the period in seconds to collect statistics.
	StatisticsPeriodSeconds = NewHazelcastPropertyInt64WithTimeUnit("hazelcast.client.statistics.period.seconds",
		3, time.Second)

	// StatisticsEnabled is used to enable the client statistics collection.
	StatisticsEnabled = NewHazelcastPropertyBool("hazelcast.client.statistics.enabled", false)

	// LoggingLevel is used to configure logging level in the client.
	// This is used only for default logging, if you use other loggers this has no effect.
	LoggingLevel = NewHazelcastPropertyString("hazelcast.client.logging.level", "info")
)
