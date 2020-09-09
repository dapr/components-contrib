// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import "time"

type metadata struct {
	host               string
	password           string
	sentinelMasterName string
	maxRetries         int
	maxRetryBackoff    time.Duration
	enableTLS          bool
	failover           bool
}
