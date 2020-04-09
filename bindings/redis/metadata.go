// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import "time"

type metadata struct {
	host            string
	password        string
	enableTLS       bool
	maxRetries      int
	maxRetryBackoff time.Duration
}
