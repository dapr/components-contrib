// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package metadata

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

const (
	// TTLMetadataKey defines the metadata key for setting a time to live (in seconds)
	TTLMetadataKey = "ttlInSeconds"
)

// TryGetTTL tries to get the ttl as a time.Duration value for pubsub, binding and any other building block.
func TryGetTTL(props map[string]string) (time.Duration, bool, error) {
	if val, ok := props[TTLMetadataKey]; ok && val != "" {
		valInt64, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, false, errors.Wrapf(err, "%s value must be a valid integer: actual is '%s'", TTLMetadataKey, val)
		}

		if valInt64 <= 0 {
			return 0, false, fmt.Errorf("%s value must be higher than zero: actual is %d", TTLMetadataKey, valInt64)
		}

		duration := time.Duration(valInt64) * time.Second
		if duration < 0 {
			// Overflow
			duration = math.MaxInt64
		}

		return duration, true, nil
	}

	return 0, false, nil
}
