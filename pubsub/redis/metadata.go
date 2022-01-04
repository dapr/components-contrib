// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"time"
)

type metadata struct {
	// The consumer identifier
	consumerID string
	// The interval between checking for pending messages to redelivery (0 disables redelivery)
	redeliverInterval time.Duration
	// The amount time a message must be pending before attempting to redeliver it (0 disables redelivery)
	processingTimeout time.Duration
	// The size of the message queue for processing
	queueDepth uint
	// The number of concurrent workers that are processing messages
	concurrency uint

	// the max len of stream
	maxLenApprox int64
}
