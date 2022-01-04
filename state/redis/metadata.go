// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import "time"

type metadata struct {
	maxRetries      int
	maxRetryBackoff time.Duration
	ttlInSeconds    *int
}
