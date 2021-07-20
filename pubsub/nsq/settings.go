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
	"reflect"
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
	if err := config.Decode(in, s); err == nil {
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
	if len(s.nsqds) == 0 {
		return fmt.Errorf("nsq error: missing nsqd Address")
	}

	return nil
}

func (s *Settings) update(src, dst interface{}) {
	rtDst := reflect.ValueOf(dst)
	if rtDst.IsZero() {
		return
	}
	rtSrc := reflect.ValueOf(src)
	rtSrc.Elem().Set(rtDst)
}

func (s *Settings) ToNSQConfig() *nsq.Config {
	cfg := nsq.NewConfig()

	s.update(&cfg.DialTimeout, s.DialTimeout)
	s.update(&cfg.ReadTimeout, s.ReadTimeout)
	s.update(&cfg.WriteTimeout, s.WriteTimeout)
	s.update(&cfg.MsgTimeout, s.MsgTimeout)
	s.update(&cfg.MaxRequeueDelay, s.MaxRequeueDelay)
	s.update(&cfg.LookupdPollInterval, s.LookupdPollInterval)
	s.update(&cfg.DefaultRequeueDelay, s.DefaultRequeueDelay)
	s.update(&cfg.HeartbeatInterval, s.HeartbeatInterval)

	s.update(&cfg.ClientID, s.ClientID)
	s.update(&cfg.UserAgent, s.UserAgent)
	s.update(&cfg.AuthSecret, s.AuthSecret)

	s.update(&cfg.LookupdPollJitter, s.LookupdPollJitter)

	s.update(&cfg.MaxAttempts, uint16(s.MaxAttempts))
	s.update(&cfg.MaxInFlight, s.MaxInFlight)
	s.update(&cfg.SampleRate, int32(s.SampleRate))

	return cfg
}
