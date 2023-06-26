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
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

func Marshal(val interface{}, marshaler func(interface{}) ([]byte, error)) ([]byte, error) {
	var err error = nil
	bt, ok := val.([]byte)
	if !ok {
		bt, err = marshaler(val)
	}

	return bt, err
}

func JSONStringify(value any) ([]byte, error) {
	switch value := value.(type) {
	case []byte:
		return value, nil
	case int:
		return []byte(strconv.FormatInt(int64(value), 10)), nil
	case int8:
		return []byte(strconv.FormatInt(int64(value), 10)), nil
	case int16:
		return []byte(strconv.FormatInt(int64(value), 10)), nil
	case int32:
		return []byte(strconv.FormatInt(int64(value), 10)), nil
	case int64:
		return []byte(strconv.FormatInt(value, 10)), nil
	case uint:
		return []byte(strconv.FormatUint(uint64(value), 10)), nil
	case uint16:
		return []byte(strconv.FormatUint(uint64(value), 10)), nil
	case uint32:
		return []byte(strconv.FormatUint(uint64(value), 10)), nil
	case uint64:
		return []byte(strconv.FormatUint(value, 10)), nil
	case float32:
		return []byte(strconv.FormatFloat(float64(value), 'f', -1, 64)), nil
	case float64:
		return []byte(strconv.FormatFloat(value, 'f', -1, 64)), nil
	case bool:
		if value {
			return []byte("true"), nil
		}
		return []byte("false"), nil
	case string:
		return []byte(`"` + strings.ReplaceAll(value, `"`, `\"`) + `"`), nil
	default:
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		err := enc.Encode(value)
		// Trim newline.
		return bytes.TrimSuffix(buf.Bytes(), []byte{0xa}), err
	}
}
