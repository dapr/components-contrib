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

package redis

import (
	"time"

	"github.com/dapr/components-contrib/metadata"
)

const (
	defaultMaxRetries      = 3
	defaultMaxRetryBackoff = time.Second * 2
)

type Metadata struct {
	MaxRetries      int           `json:"maxRetries,string,omitempty"`
	MaxRetryBackoff time.Duration `json:"maxRetryBackoff,string,omitempty"`
	TTLInSeconds    *int          `json:"ttlInSeconds,string,omitempty"`
	QueryIndexes    string        `json:"queryIndexes,omitempty"`
	RedisVersion    string        `json:"redisVersion,omitempty"`
}

func ParseRedisMetadata(properties map[string]string) (Metadata, error) {
	m := Metadata{}

	m.MaxRetries = defaultMaxRetries
	m.MaxRetryBackoff = defaultMaxRetryBackoff
	metadata.DecodeMetadata(properties, &m)

	return m, nil
}
