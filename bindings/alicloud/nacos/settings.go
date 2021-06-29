// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

// Nacos is an easy-to-use dynamic service discovery, configuration and service management platform
//
// See https://github.com/nacos-group/nacos-sdk-go/

package nacos

import (
	"errors"
	"fmt"
	"time"

	"github.com/dapr/kit/config"
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
	RotateTime           string        `mapstructure:"rotateTime"`
	MaxAge               int           `mapstructure:"maxAge"`
	LogLevel             string        `mapstructure:"logLevel"`
	Config               string        `mapstructure:"config"`
	Watches              string        `mapstructure:"watches"`
}

func (s *Settings) Decode(in interface{}) error {
	return config.Decode(in, s)
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
