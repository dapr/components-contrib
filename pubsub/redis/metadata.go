// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"time"
)

type metadata struct {
	// The Redis host
	host string
	// The Redis password
	password string
	// Database to be selected after connecting to the server.
	db int

	// Maximum number of retries before giving up.
	// Default is to not retry failed commands.
	maxRetries int
	// Minimum backoff between each retry.
	// Default is 8 milliseconds; -1 disables backoff.
	minRetryBackoff time.Duration
	// Maximum backoff between each retry.
	// Default is 512 milliseconds; -1 disables backoff.
	maxRetryBackoff time.Duration

	// Dial timeout for establishing new connections.
	dialTimeout time.Duration
	// Timeout for socket reads. If reached, commands will fail
	// with a timeout instead of blocking. Use value -1 for no timeout and 0 for default.
	readTimeout time.Duration
	// Timeout for socket writes. If reached, commands will fail
	writeTimeout time.Duration
	// Maximum number of socket connections.
	poolSize int
	// Minimum number of idle connections which is useful when establishing
	// new connection is slow.
	minIdleConns int
	// Connection age at which client retires (closes) the connection.
	// Default is to not close aged connections.
	maxConnAge time.Duration
	// Amount of time client waits for connection if all connections
	// are busy before returning an error.
	// Default is ReadTimeout + 1 second.
	poolTimeout time.Duration
	// Amount of time after which client closes idle connections.
	// Should be less than server's timeout.
	// Default is 5 minutes. -1 disables idle timeout check.
	idleTimeout time.Duration
	// Frequency of idle checks made by idle connections reaper.
	// Default is 1 minute. -1 disables idle connections reaper,
	// but idle connections are still discarded by the client
	// if IdleTimeout is set.
	idleCheckFrequency time.Duration

	// The consumer identifier
	consumerID string
	// A flag to enables TLS by setting InsecureSkipVerify to true
	enableTLS bool
	// The interval between checking for pending messages to redelivery (0 disables redelivery)
	redeliverInterval time.Duration
	// The amount time a message must be pending before attempting to redeliver it (0 disables redelivery)
	processingTimeout time.Duration
	// The size of the message queue for processing
	queueDepth uint
	// The number of concurrent workers that are processing messages
	concurrency uint
}
