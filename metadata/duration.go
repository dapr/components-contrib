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

// JSON marshaling and unmarshaling methods for time.Duration based on https://stackoverflow.com/a/48051946
// Includes methods to return an ISO-8601 formatted string from a time.Duration.
import (
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"time"

	"github.com/mitchellh/mapstructure"
)

type Duration struct {
	time.Duration
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)

		return nil
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}

		return nil
	default:
		return errors.New("invalid duration")
	}
}

// This helper function is used to decode durations within a map[string]interface{} into a struct.
// It must be used in conjunction with mapstructure's DecodeHook.
// This is used in utils.DecodeMetadata to decode durations in metadata.
//
//	mapstructure.NewDecoder(&mapstructure.DecoderConfig{
//	   DecodeHook: mapstructure.ComposeDecodeHookFunc(
//	     toTimeDurationHookFunc()),
//	   Metadata: nil,
//			Result:   result,
//	})
func toTimeDurationHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		if t != reflect.TypeOf(Duration{}) && t != reflect.TypeOf(time.Duration(0)) {
			return data, nil
		}

		switch f.Kind() {
		case reflect.String:
			val, err := time.ParseDuration(data.(string))
			if err != nil {
				return nil, err
			}
			if t != reflect.TypeOf(Duration{}) {
				return val, nil
			}
			return Duration{Duration: val}, nil
		case reflect.Float64:
			val := time.Duration(data.(float64))
			if t != reflect.TypeOf(Duration{}) {
				return val, nil
			}
			return Duration{Duration: val}, nil
		case reflect.Int64:
			val := time.Duration(data.(int64))
			if t != reflect.TypeOf(Duration{}) {
				return val, nil
			}
			return Duration{Duration: val}, nil
		default:
			return data, nil
		}
	}
}

// ToISOString returns the duration formatted as a ISO-8601 duration string (-ish).
// This methods supports days, hours, minutes, and seconds. It assumes all durations are in UTC time and are not impacted by DST (so all days are 24-hours long).
// This method does not support fractions of seconds, and durations are truncated to seconds.
// See https://en.wikipedia.org/wiki/ISO_8601#Durations for referece.
func (d Duration) ToISOString() string {
	// Truncate to seconds, removing fractional seconds
	trunc := d.Truncate(time.Second)

	seconds := int64(trunc.Seconds())
	if seconds == 0 {
		// Zero value
		return "P0D"
	}

	res := "P"
	if seconds >= 86400 {
		res += strconv.FormatInt(seconds/86400, 10) + "D"
		seconds %= 86400
	}
	if seconds == 0 {
		// Short-circuit if there's nothing left (we had whole days only)
		return res
	}
	res += "T"
	if seconds >= 3600 {
		res += strconv.FormatInt(seconds/3600, 10) + "H"
		seconds %= 3600
	}
	if seconds >= 60 {
		res += strconv.FormatInt(seconds/60, 10) + "M"
		seconds %= 60
	}
	if seconds > 0 {
		res += strconv.FormatInt(seconds, 10) + "S"
	}
	return res
}
