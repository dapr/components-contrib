// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

// DingTalk webhook are a simple way to post messages from apps into DingTalk
//
// See https://developers.dingtalk.com/document/app/custom-robot-access for details

package webhook

import (
	"errors"

	"github.com/dapr/kit/config"
)

type Settings struct {
	ID     string `mapstructure:"id"`
	URL    string `mapstructure:"url"`
	Secret string `mapstructure:"secret"`
}

func (s *Settings) Decode(in interface{}) error {
	return config.Decode(in, s)
}

func (s *Settings) Validate() error {
	if s.ID == "" {
		return errors.New("webhook error: missing webhook id")
	}
	if s.URL == "" {
		return errors.New("webhook error: missing webhook url")
	}

	return nil
}
