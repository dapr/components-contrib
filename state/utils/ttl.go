/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"fmt"
	"math"
	"strconv"
)

// Key used for "ttlInSeconds" in metadata.
const MetadataTTLKey = "ttlInSeconds"

// ParseTTL parses the "ttlInSeconds" metadata property.
func ParseTTL(requestMetadata map[string]string) (*int, error) {
	if val := requestMetadata[MetadataTTLKey]; val != "" {
		parsedVal, err := strconv.ParseInt(val, 10, 0)
		if err != nil {
			return nil, fmt.Errorf("incorrect value for metadata '%s': %w", MetadataTTLKey, err)
		}
		if parsedVal < -1 || parsedVal > math.MaxInt32 {
			return nil, fmt.Errorf("incorrect value for metadata '%s': must be -1 or greater", MetadataTTLKey)
		}
		i := int(parsedVal)
		return &i, nil
	}
	return nil, nil
}

// ParseTTL64 parses the "ttlInSeconds" metadata property.
func ParseTTL64(requestMetadata map[string]string) (*int64, error) {
	if val := requestMetadata[MetadataTTLKey]; val != "" {
		parsedVal, err := strconv.ParseInt(val, 10, 0)
		if err != nil {
			return nil, fmt.Errorf("incorrect value for metadata '%s': %w", MetadataTTLKey, err)
		}
		if parsedVal < -1 || parsedVal > math.MaxInt32 {
			return nil, fmt.Errorf("incorrect value for metadata '%s': must be -1 or greater", MetadataTTLKey)
		}
		return &parsedVal, nil
	}
	return nil, nil
}
