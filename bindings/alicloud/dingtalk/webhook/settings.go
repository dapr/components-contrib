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

// DingTalk webhook are a simple way to post messages from apps into DingTalk
//
// See https://developers.dingtalk.com/document/app/custom-robot-access for details

package webhook

import (
	"errors"

	"github.com/dapr/components-contrib/metadata"
)

type Settings struct {
	ID     string `mapstructure:"id"`
	URL    string `mapstructure:"url"`
	Secret string `mapstructure:"secret"`
}

func (s *Settings) Decode(in interface{}) error {
	return metadata.DecodeMetadata(in, s)
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
