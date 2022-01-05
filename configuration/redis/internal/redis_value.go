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

package internal

import (
	"fmt"
	"strings"
)

const (
	channelPrefix = "__keyspace@0__:"
	separator     = "||"
)

func GetRedisValueAndVersion(redisValue string) (string, string) {
	valueAndRevision := strings.Split(redisValue, separator)
	if len(valueAndRevision) == 0 {
		return "", ""
	}
	if len(valueAndRevision) == 1 {
		return valueAndRevision[0], ""
	}
	return valueAndRevision[0], valueAndRevision[1]
}

func ParseRedisKeyFromEvent(eventChannel string) (string, error) {
	index := strings.Index(eventChannel, channelPrefix)
	if index == -1 {
		return "", fmt.Errorf("wrong format of event channel, it should start with '%s': eventChannel=%s", channelPrefix, eventChannel)
	}

	return eventChannel[len(channelPrefix):], nil
}
