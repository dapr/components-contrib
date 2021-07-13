// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import "fmt"

// ConcurrencyMode is a pub/sub metadata setting that allows to specify whether messages are delivered in a serial or parallel execution.
type ConcurrencyMode string

const (
	// ConcurrencyKey is the metadata key name for ConcurrencyMode.
	ConcurrencyKey                 = "concurrencyMode"
	Single         ConcurrencyMode = "single"
	Parallel       ConcurrencyMode = "parallel"
)

// Concurrency takes a metadata object and returns the ConcurrencyMode configured. Default is Parallel.
func Concurrency(metadata map[string]string) (ConcurrencyMode, error) {
	if val, ok := metadata[ConcurrencyKey]; ok && val != "" {
		switch val {
		case string(Single):
			return Single, nil
		case string(Parallel):
			return Parallel, nil
		default:
			return "", fmt.Errorf("invalid %s %s", ConcurrencyKey, val)
		}
	}

	return Parallel, nil
}
