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
	// Dial timeout for establishing new connections.
	dialTimeoutInSec int
	// Timeout for socket reads. If reached, commands will fail
	// with a timeout instead of blocking. Use value -1 for no timeout and 0 for default.
	readTimeoutInSec int
	// Timeout for socket writes. If reached, commands will fail
	writeTimeoutInSec int
	// Maximum number of socket connections.
	poolSize int
	// Connection age at which client retires (closes) the connection.
	maxConnAgeInSec int
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
