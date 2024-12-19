//go:generate make -f ../../../Makefile component-metadata-manifest type=bindings status=alpha version=v1 direction=output origin=$PWD "title=Alibaba Cloud DingTalk"

/*
Copyright 2024 The Dapr Authors
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

	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
	"github.com/dapr/components-contrib/common/component"
	kitmd "github.com/dapr/kit/metadata"
)

// implement MetadataBuilder so each component will be properly parsed for the ast to auto-generate metadata manifest.
var _ component.MetadataBuilder = &webhookMetadata{}

type webhookMetadata struct {
	// Unique ID.
	ID string `json:"id" mapstructure:"id"`
	// DingTalk's Webhook URL.
	URL string `json:"url" mapstructure:"url"`
	// The secret of DingTalk's Webhook.
	Secret string `json:"secret,omitempty" mapstructure:"secret"`

	// TODO: docs metadata is incorrect for this.
}

func (w *webhookMetadata) Decode(in interface{}) error {
	return kitmd.DecodeMetadata(in, w)
}

func (w *webhookMetadata) Validate() error {
	if w.ID == "" {
		return errors.New("webhook error: missing webhook id")
	}
	if w.URL == "" {
		return errors.New("webhook error: missing webhook url")
	}

	return nil
}

// Set the default values here.
// This unifies the setup across all components,
// and makes it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanks to Go.
func (w *webhookMetadata) Defaults() any {
	return webhookMetadata{}
}

// Note: we do not include any mdignored field.
func (w *webhookMetadata) Examples() any {
	return webhookMetadata{
		ID:     "test_webhook_id",
		URL:    "https://oapi.dingtalk.com/robot/send?access_token=******",
		Secret: "****************",
	}
}

func (w *webhookMetadata) Binding() metadataschema.Binding {
	return metadataschema.Binding{
		Input:  false,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "create",
				Description: "Create object",
			},
		},
	}
}
