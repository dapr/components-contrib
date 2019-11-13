// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iputil

import (
	"crypto/rand"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var defaultPort = 5701

func GetIPAndPort(addr string) (string, int) {
	var port int
	var err error
	parts := strings.Split(addr, ":")
	if len(parts) == 2 {
		port, err = strconv.Atoi(parts[1])
		if err != nil {
			port = defaultPort // Default port
		}
	} else {
		port = -1
	}
	addr = parts[0]
	return addr, port
}

// NewUUID generates a random uuid according to RFC 4122
func NewUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return "", err
	}
	uuid[8] = uuid[8]&^0xc0 | 0x80
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
}
