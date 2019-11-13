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

package config

import (
	"log"
	"time"
)

// NetworkConfig contains network related configuration parameters.
type NetworkConfig struct {
	// addresses are the candidate addresses slice that client will use to establish initial connection.
	addresses []string

	// connectionAttemptLimit is how many times client will retry to connect to the members in the addresses slice.
	// While client is trying to connect initially to one of the members in the addresses slice, all might not be
	// available. Instead of giving up, returning Error and stopping client, it will attempt to retry as many times as
	// defined by this parameter.
	connectionAttemptLimit int32

	// connectionAttemptPeriod is the period for the next attempt to find a member to connect.
	connectionAttemptPeriod time.Duration

	// connectionTimeout is a duration for connection timeout.
	// Setting a timeout of zero disables the timeout feature and is equivalent to block the socket until it connects.
	connectionTimeout time.Duration

	// redoOperation determines if client will redo the operations that were executing on the server when client
	// recovers the connection after a failure. This can be because of network, or simply because the member died.
	// However it is not clear whether the application is performed or not. For idempotent operations this is harmless,
	// but for non idempotent ones retrying can cause to undesirable effects.
	// Note that the redo can perform on any member.
	redoOperation bool

	// smartRouting determines if client will route the key based operations to owner of the key at the best effort.
	// Note that it uses a cached value of partition count and doesn't guarantee that the operation will always be
	// executed on the owner.
	// The cached table is updated every 10 seconds.
	smartRouting bool

	// cloudConfig is the config for cloud discovery.
	cloudConfig *CloudConfig

	// sslConfig is used for ssl/tls configuration of client.
	sslConfig *SSLConfig
}

// NewNetworkConfig returns a new NetworkConfig with default configuration.
func NewNetworkConfig() *NetworkConfig {
	return &NetworkConfig{
		addresses:               make([]string, 0),
		connectionAttemptLimit:  2,
		connectionAttemptPeriod: 3 * time.Second,
		connectionTimeout:       5 * time.Second,
		redoOperation:           false,
		smartRouting:            true,
		cloudConfig:             NewCloudConfig(),
		sslConfig:               NewSSLConfig(),
	}
}

// Addresses returns the slice of candidate addresses that client will use to establish initial connection.
func (nc *NetworkConfig) Addresses() []string {
	return nc.addresses
}

// ConnectionAttemptLimit returns connection attempt limit.
func (nc *NetworkConfig) ConnectionAttemptLimit() int32 {
	return nc.connectionAttemptLimit
}

// ConnectionAttemptPeriod returns the period for the next attempt to find a member to connect.
func (nc *NetworkConfig) ConnectionAttemptPeriod() time.Duration {
	return nc.connectionAttemptPeriod
}

// ConnectionTimeout returns the timeout value for nodes to accept client connection requests.
func (nc *NetworkConfig) ConnectionTimeout() time.Duration {
	return nc.connectionTimeout
}

// IsRedoOperation returns true if redo operations are enabled.
func (nc *NetworkConfig) IsRedoOperation() bool {
	return nc.redoOperation
}

// IsSmartRouting returns true if client is smart.
func (nc *NetworkConfig) IsSmartRouting() bool {
	return nc.smartRouting
}

// AddAddress adds given addresses to candidate address list that client will use to establish initial connection.
func (nc *NetworkConfig) AddAddress(addresses ...string) {
	nc.addresses = append(nc.addresses, addresses...)
}

// SetAddresses sets given addresses as candidate address list that client will use to establish initial connection.
func (nc *NetworkConfig) SetAddresses(addresses []string) {
	nc.addresses = addresses
}

// SetConnectionAttemptLimit sets the connection attempt limit.
// While client is trying to connect initially to one of the members in the addresses slice, all might not be
// available. Instead of giving up, returning Error and stopping client, it will attempt to retry as much as defined
// by this parameter.
func (nc *NetworkConfig) SetConnectionAttemptLimit(connectionAttemptLimit int32) {
	nc.connectionAttemptLimit = connectionAttemptLimit
}

// SetConnectionAttemptPeriod sets the period for the next attempt to find a member to connect
func (nc *NetworkConfig) SetConnectionAttemptPeriod(connectionAttemptPeriod time.Duration) {
	nc.connectionAttemptPeriod = connectionAttemptPeriod
}

// SetConnectionTimeout sets the connection timeout.
// Setting a timeout of zero disables the timeout feature and is equivalent to block the socket until it connects.
func (nc *NetworkConfig) SetConnectionTimeout(connectionTimeout time.Duration) {
	if connectionTimeout < 0 {
		log.Panicf("connectionTimeout should be non-negative, got %s", connectionTimeout)
	}
	nc.connectionTimeout = connectionTimeout
}

// SetRedoOperation sets redoOperation.
// If true, client will redo the operations that were executing on the server when client recovers the connection after a failure.
// This can be because of network, or simply because the member died. However it is not clear whether the
// application is performed or not. For idempotent operations this is harmless, but for non idempotent ones
// retrying can cause to undesirable effects. Note that the redo can perform on any member.
func (nc *NetworkConfig) SetRedoOperation(redoOperation bool) {
	nc.redoOperation = redoOperation
}

// SetSmartRouting sets smartRouting.
// If true, client will route the key based operations to owner of the key at the best effort.
// Note that it uses a cached version of partitionService and doesn't
// guarantee that the operation will always be executed on the owner. The cached table is updated every 10 seconds.
// Default value is true.
func (nc *NetworkConfig) SetSmartRouting(smartRouting bool) {
	nc.smartRouting = smartRouting
}

// CloudConfig returns the cloud config.
func (nc *NetworkConfig) CloudConfig() *CloudConfig {
	return nc.cloudConfig
}

// SetCloudConfig sets the Cloud Config as the given config.
func (nc *NetworkConfig) SetCloudConfig(cloudConfig *CloudConfig) {
	nc.cloudConfig = cloudConfig
}

// SSLConfig returns SSLConfig for this client.
func (nc *NetworkConfig) SSLConfig() *SSLConfig {
	return nc.sslConfig
}
