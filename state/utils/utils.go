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
	"encoding/json"
	"strconv"
	"strings"
)

func JSONStringify(value any) ([]byte, error) {
	switch value := value.(type) {
	case []byte:
		return value, nil
	case int, int8, int16, int32, int64:
		return []byte(strconv.FormatInt(value.(int64), 10)), nil
	case uint, uint8, uint16, uint32, uint64:
		return []byte(strconv.FormatUint(value.(uint64), 10)), nil
	case float32, float64:
		return []byte(strconv.FormatFloat(value.(float64), 'f', -1, 64)), nil
	case bool:
		if value {
			return []byte("true"), nil
		}
		return []byte("false"), nil
	case string:
		return []byte(`"` + strings.ReplaceAll(value, `"`, `\"`) + `"`), nil
	default:
		return json.Marshal(value)
	}
}

// Deprecated: use JSONStringify instead.
func Marshal(val interface{}, marshaler func(interface{}) ([]byte, error)) ([]byte, error) {
	var err error = nil
	bt, ok := val.([]byte)
	if !ok {
		bt, err = marshaler(val)
	}

	return bt, err
}
