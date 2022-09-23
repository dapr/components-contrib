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

// Nacos is an easy-to-use dynamic service discovery, configuration and service management platform
//
// See https://github.com/nacos-group/nacos-sdk-go/

package nacos

import (
	"errors"
	"fmt"
	"time"

	"github.com/dapr/components-contrib/metadata"
)

type Settings struct {
	NameServer           string        `mapstructure:"nameServer"`
	Endpoint             string        `mapstructure:"endpoint"`
	RegionID             string        `mapstructure:"region"`
	NamespaceID          string        `mapstructure:"namespace"`
	AccessKey            string        `mapstructure:"accessKey"`
	SecretKey            string        `mapstructure:"secretKey"`
	Timeout              time.Duration `mapstructure:"timeout"`
	CacheDir             string        `mapstructure:"cacheDir"`
	UpdateThreadNum      int           `mapstructure:"updateThreadNum"`
	NotLoadCacheAtStart  bool          `mapstructure:"notLoadCacheAtStart"`
	UpdateCacheWhenEmpty bool          `mapstructure:"updateCacheWhenEmpty"`
	Username             string        `mapstructure:"username"`
	Password             string        `mapstructure:"password"`
	LogDir               string        `mapstructure:"logDir"`
	MaxAge               int           `mapstructure:"maxAge"`
	MaxSize              int           `mapstructure:"maxSize"`
	LogLevel             string        `mapstructure:"logLevel"`
	Config               string        `mapstructure:"config"`
	Watches              string        `mapstructure:"watches"`
}

func (s *Settings) Decode(in interface{}) error {
	return metadata.DecodeMetadata(in, s)
}

func (s *Settings) Validate() error {
	if s.Timeout <= 0 {
		return fmt.Errorf("invalid timeout %s", s.Timeout)
	}

	if s.Endpoint == "" && s.NameServer == "" {
		return errors.New("either endpoint or nameserver must be configured")
	}

	return nil
}
