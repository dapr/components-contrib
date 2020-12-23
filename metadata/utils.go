// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package metadata

import (
	"fmt"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

const (
	// TTLMetadataKey defines the metadata key for setting a time to live (in seconds)
	TTLMetadataKey = "ttlInSeconds"
)

// TryGetTTL tries to get the ttl (in seconds) value for pubsub, binding and any other building block.
func TryGetTTL(props map[string]string) (time.Duration, bool, error) {
	if val, ok := props[TTLMetadataKey]; ok && val != "" {
		valInt, err := strconv.Atoi(val)
		if err != nil {
			return 0, false, errors.Wrapf(err, "%s value must be a valid integer: actual is '%s'", TTLMetadataKey, val)
		}

		if valInt <= 0 {
			return 0, false, fmt.Errorf("%s value must be higher than zero: actual is %d", TTLMetadataKey, valInt)
		}

		return time.Duration(valInt) * time.Second, true, nil
	}

	return 0, false, nil
}
