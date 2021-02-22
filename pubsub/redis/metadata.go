// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"time"
)

type metadata struct {
	host              string
	password          string
	consumerID        string
	enableTLS         bool
	processingTimeout time.Duration
	reclaimInterval   time.Duration
	queueDepth        uint
	concurrency       uint
}
