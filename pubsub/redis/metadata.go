// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

type metadata struct {
	host       string
	password   string
	consumerID string
	enableTLS  bool
}
