//go:generate make -f ../../../Makefile component-metadata-manifest type=bindings builtinAuth="aws" status=alpha version=v1 direction=output origin=$PWD "title=AWS SES"

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

package ses

import (
	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
)

type sesMetadata struct {
	// AccessKey is the AWS access key to authenticate requests.
	AccessKey string `json:"accessKey" mapstructure:"accessKey" mdignore:"true"`
	// SecretKey is the AWS secret key associated with the access key.
	SecretKey string `json:"secretKey" mapstructure:"secretKey" mdignore:"true"`
	// SessionToken is the session token for temporary AWS credentials.
	SessionToken string `json:"sessionToken" mapstructure:"sessionToken" mdignore:"true"`
	// Region specifies the AWS region for the bucket.
	Region string `json:"region" mapstructure:"region" mapstructurealiases:"awsRegion" mdignore:"true"`
	// If set, this specifies the email address of the sender.
	EmailFrom string `json:"emailFrom,omitempty"`
	// If set, this specifies the email address of the receiver.
	EmailTo string `json:"emailTo,omitempty"`
	// If set, this specifies the subject of the email message.
	Subject string `json:"subject,omitempty"`
	// If set, this specifies the email address to CC in.
	EmailCc string `json:"emailCc,omitempty"`
	// If set, this specifies email address to BCC in.
	EmailBcc string `json:"emailBcc,omitempty"`
}

// Set the default values here.
// This unifies the setup across all components,
// and makes it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanks to Go.
func (s *sesMetadata) Defaults() any {
	return sesMetadata{}
}

// Note: we do not include any mdignored field.
func (s *sesMetadata) Examples() any {
	return sesMetadata{
		EmailFrom: "sender@example.com",
		EmailTo:   "receiver@example.com",
		EmailCc:   "cc@example.com",
		EmailBcc:  "bcc@example.com",
		Subject:   "subject",
	}
}

func (s *sesMetadata) Binding() metadataschema.Binding {
	return metadataschema.Binding{
		Input:  false,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "create",
				Description: "Create email",
			},
		},
	}
}
