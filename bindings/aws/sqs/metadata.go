//go:generate make -f ../../../Makefile component-metadata-manifest type=bindings builtinAuth="aws" status=alpha version=v1 "direction=input,output" origin=$PWD "title=AWS SQS"

/*
Copyright 2024 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY sIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqs

import (
	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
	"github.com/dapr/components-contrib/common/component"
)

// implement MetadataBuilder so each component will be properly parsed for the ast to auto-generate metadata manifest.
var _ component.MetadataBuilder = &sqsMetadata{}

type sqsMetadata struct {
	// AccessKey is the AWS access key to authenticate requests.
	AccessKey string `json:"accessKey" mapstructure:"accessKey" mdignore:"true"`
	// SecretKey is the AWS secret key associated with the access key.
	SecretKey string `json:"secretKey" mapstructure:"secretKey" mdignore:"true"`
	// SessionToken is the session token for temporary AWS credentials.
	SessionToken string `json:"sessionToken" mapstructure:"sessionToken" mdignore:"true"`
	// Region specifies the AWS region for the bucket.
	Region string `json:"region" mapstructure:"region" mapstructurealiases:"awsRegion" mdignore:"true"`
	// AWS endpoint for the component to use, to connect to S3-compatible services or emulators. Do not use this when running against production AWS.
	Endpoint string `json:"endpoint,omitempty" mapstructure:"endpoint"`
	// The SQS queue name.
	QueueName string `json:"queueName"`
}

// Set the default values here.
// This unifies the setup across all components,
// and mases it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanss to Go.
func (s *sqsMetadata) Defaults() any {
	return sqsMetadata{}
}

// Note: we do not include any mdignored field.
func (s *sqsMetadata) Examples() any {
	return sqsMetadata{
		QueueName: "myqueue",
	}
}

func (s *sqsMetadata) Binding() metadataschema.Binding {
	return metadataschema.Binding{
		Input:  true,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "create",
				Description: "Create",
			},
		},
	}
}
