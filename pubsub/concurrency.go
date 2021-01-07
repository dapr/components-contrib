// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

// ConcurrencyMode is a pub/sub metadata setting that allows to specify whether messages are delivered in a serial or parallel execution
type ConcurrencyMode string

const (
	// ConcurrencyKey is the metadata key name for ConcurrencyMode
	ConcurrencyKey                 = "concurrencyMode"
	Single         ConcurrencyMode = "single"
	Parallel       ConcurrencyMode = "parallel"
)

// Concurrency takes a metadata object and returns the ConcurrencyMode configured. Default is Parallel
func Concurrency(metadata map[string]string) ConcurrencyMode {
	if val, ok := metadata[ConcurrencyKey]; ok && val != "" {
		return ConcurrencyMode(val)
	}

	return Parallel
}
