// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

// Nacos is an easy-to-use dynamic service discovery, configuration and service management platform
//
// See https://github.com/nacos-group/nacos-sdk-go/

package nsq

import (
	"fmt"
	"strings"
	"time"

	"github.com/dapr/kit/config"
	"github.com/nsqio/go-nsq"
)

type Settings struct {
	// nsqd address
	nsqds []string

	// nsqlooupd address
	lookups []string

	// used for publish
	NsqAddress string `mapstructure:"nsq_address"`

	// used for subscribe
	NsqLookups string `mapstructure:"nsq_lookups"`

	// nsq config
	DialTimeout         time.Duration `mapstructure:"dial_timeout"`
	ReadTimeout         time.Duration `mapstructure:"read_timeout"`
	WriteTimeout        time.Duration `mapstructure:"write_timeout"`
	MsgTimeout          time.Duration `mapstructure:"msg_timeout"`
	MaxRequeueDelay     time.Duration `mapstructure:"max_requeue_delay"`
	LookupdPollInterval time.Duration `mapstructure:"lookupd_poll_interval"`
	DefaultRequeueDelay time.Duration `mapstructure:"default_requeue_delay"`
	HeartbeatInterval   time.Duration `mapstructure:"heartbeat_interval"`

	ClientID   string `mapstructure:"client_id"`
	UserAgent  string `mapstructure:"user_agent"`
	AuthSecret string `mapstructure:"auth_secret"`

	LookupdPollJitter float64 `mapstructure:"lookupd_poll_jitter"`

	MaxAttempts int `mapstructure:"max_attempts"`
	MaxInFlight int `mapstructure:"max_in_flight"`
	SampleRate  int `mapstructure:"sample_rate"`

	// sub
	Channel     string `mapstructure:"channel"`
	Concurrency int    `mapstructure:"concurrency"`
}

func (s *Settings) Decode(in interface{}) error {
	err := config.Decode(in, s)
	if err == nil {
		if len(s.NsqAddress) > 0 {
			s.nsqds = strings.Split(s.NsqAddress, splitter)
		}
		if len(s.NsqLookups) > 0 {
			s.lookups = strings.Split(s.NsqLookups, splitter)
		}
	}
	return nil
}

func (s *Settings) Validate() error {
	if len(s.nsqds) <= 0 {
		return fmt.Errorf("nsq error: missing nsqd Address")
	}

	return nil
}

func (s *Settings) ToNSQConfig() *nsq.Config {
	cfg := nsq.NewConfig()
	if s.DialTimeout > 0 {
		cfg.DialTimeout = s.DialTimeout
	}
	if s.ReadTimeout > 0 {
		cfg.ReadTimeout = s.ReadTimeout
	}
	if s.WriteTimeout > 0 {
		cfg.WriteTimeout = s.WriteTimeout
	}
	if s.MsgTimeout > 0 {
		cfg.MsgTimeout = s.MsgTimeout
	}
	if s.MaxRequeueDelay > 0 {
		cfg.MaxRequeueDelay = s.MaxRequeueDelay
	}
	if s.LookupdPollInterval > 0 {
		cfg.LookupdPollInterval = s.LookupdPollInterval
	}
	if s.LookupdPollJitter > 0 {
		cfg.LookupdPollJitter = s.LookupdPollJitter
	}
	if s.DefaultRequeueDelay > 0 {
		cfg.DefaultRequeueDelay = s.DefaultRequeueDelay
	}
	if s.HeartbeatInterval > 0 {
		cfg.HeartbeatInterval = s.HeartbeatInterval
	}
	if s.SampleRate > 0 {
		cfg.SampleRate = int32(s.SampleRate)
	}
	if s.MaxAttempts > 0 {
		cfg.MaxAttempts = uint16(s.MaxAttempts)
	}
	if s.MaxInFlight > 0 {
		cfg.MaxInFlight = s.MaxInFlight
	}
	if len(s.ClientID) > 0 {
		cfg.ClientID = s.ClientID
	}
	if len(s.UserAgent) > 0 {
		cfg.UserAgent = s.UserAgent
	}
	if len(s.AuthSecret) > 0 {
		cfg.AuthSecret = s.AuthSecret
	}
	return cfg
}
