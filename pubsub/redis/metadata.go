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
	// The consumer identifier
	consumerID string
	// A flag to enables TLS by setting InsecureSkipVerify to true
	enableTLS bool
	// The interval between checking for pending messages to redelivery
	redeliverInterval time.Duration
	// The amount time a message must be pending before attempting to redeliver it
	processingTimeout time.Duration
	// The size of the message queue for processing
	queueDepth uint
	// The number of concurrent workers that are processing messages
	concurrency uint
}
