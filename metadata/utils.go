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

package metadata

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"

	"github.com/dapr/components-contrib/internal/utils"
	"github.com/dapr/kit/ptr"
)

const (
	// TTLMetadataKey defines the metadata key for setting a time to live (in seconds).
	TTLMetadataKey = "ttlInSeconds"

	// RawPayloadKey defines the metadata key for forcing raw payload in pubsub.
	RawPayloadKey = "rawPayload"

	// PriorityMetadataKey defines the metadata key for setting a priority.
	PriorityMetadataKey = "priority"

	// ContentType defines the metadata key for the content type.
	ContentType = "contentType"

	// QueryIndexName defines the metadata key for the name of query indexing schema (for redis).
	QueryIndexName = "queryIndexName"

	// MaxBulkCountSubKey defines the maximum number of messages to be sent in a single bulk subscribe request.
	MaxBulkSubCountKey string = "maxBulkSubCount"

	// MaxBulkAwaitDurationKey is the key for the max bulk await duration in the metadata.
	MaxBulkSubAwaitDurationMsKey string = "maxBulkSubAwaitDurationMs"

	// MaxBulkPubBytesKey defines the maximum bytes to publish in a bulk publish request metadata.
	MaxBulkPubBytesKey string = "maxBulkPubBytes"
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

// TryGetPriority tries to get the priority for binding and any other building block.
func TryGetPriority(props map[string]string) (uint8, bool, error) {
	if val, ok := props[PriorityMetadataKey]; ok && val != "" {
		intVal, err := strconv.Atoi(val)
		if err != nil {
			return 0, false, errors.Wrapf(err, "%s value must be a valid integer: actual is '%s'", PriorityMetadataKey, val)
		}

		priority := uint8(intVal)
		if intVal < 0 {
			priority = 0
		} else if intVal > 255 {
			priority = math.MaxUint8
		}

		return priority, true, nil
	}

	return 0, false, nil
}

// IsRawPayload determines if payload should be used as-is.
func IsRawPayload(props map[string]string) (bool, error) {
	if val, ok := props[RawPayloadKey]; ok && val != "" {
		boolVal, err := strconv.ParseBool(val)
		if err != nil {
			return false, errors.Wrapf(err, "%s value must be a valid boolean: actual is '%s'", RawPayloadKey, val)
		}

		return boolVal, nil
	}

	return false, nil
}

func TryGetContentType(props map[string]string) (string, bool) {
	if val, ok := props[ContentType]; ok && val != "" {
		return val, true
	}

	return "", false
}

func TryGetQueryIndexName(props map[string]string) (string, bool) {
	if val, ok := props[QueryIndexName]; ok && val != "" {
		return val, true
	}

	return "", false
}

// GetMetadataProperty returns a property from the metadata map, with support for aliases
func GetMetadataProperty(props map[string]string, keys ...string) (val string, ok bool) {
	for _, k := range keys {
		val, ok = props[k]
		if ok {
			return val, true
		}
	}
	return "", false
}

// DecodeMetadata decodes metadata into a struct
// This is an extension of mitchellh/mapstructure which also supports decoding durations
func DecodeMetadata(input interface{}, result interface{}) error {
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			toTimeDurationHookFunc(),
			toTruthyBoolHookFunc(),
			toStringArrayHookFunc(),
		),
		Metadata:         nil,
		Result:           result,
		WeaklyTypedInput: true,
	})
	if err != nil {
		return err
	}
	err = decoder.Decode(input)
	return err
}

func toTruthyBoolHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		if f == reflect.TypeOf("") && t == reflect.TypeOf(true) {
			val := data.(string)
			return utils.IsTruthy(val), nil
		}
		if f == reflect.TypeOf("") && t == reflect.TypeOf(reflect.TypeOf(ptr.Of(true))) {
			val := data.(string)
			return ptr.Of(utils.IsTruthy(val)), nil
		}
		return data, nil
	}
}

func toStringArrayHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		if f == reflect.TypeOf("") && t == reflect.TypeOf([]string{}) {
			val := data.(string)
			return strings.Split(val, ","), nil
		}
		if f == reflect.TypeOf("") && t == reflect.TypeOf(ptr.Of([]string{})) {
			val := data.(string)
			return ptr.Of(strings.Split(val, ",")), nil
		}
		return data, nil
	}
}

// GetMetadataInfoFromStructType converts a struct to a map of field name (or struct tag) to field type.
// This is used to generate metadata documentation for components.
func GetMetadataInfoFromStructType(t reflect.Type, metadataMap *map[string]string) error {
	// Return if not struct or pointer to struct.
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return fmt.Errorf("not a struct: %s", t.Kind().String())
	}

	for i := 0; i < t.NumField(); i++ {
		currentField := t.Field(i)
		mapStructureTag := currentField.Tag.Get("mapstructure")
		tags := strings.Split(mapStructureTag, ",")
		numTags := len(tags)
		if numTags > 1 && tags[numTags-1] == "squash" && currentField.Anonymous {
			// traverse embedded struct
			GetMetadataInfoFromStructType(currentField.Type, metadataMap)
			continue
		}
		var fieldName string
		if numTags > 0 && tags[0] != "" {
			fieldName = tags[0]
		} else {
			fieldName = currentField.Name
		}
		(*metadataMap)[fieldName] = currentField.Type.String()
	}
	return nil
}
