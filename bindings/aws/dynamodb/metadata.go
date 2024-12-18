//go:generate make -f ../../../Makefile component-metadata-manifest type=bindings builtinAuth="aws" status=stable version=v1 direction=output origin=$PWD "title=AWS DynamoDB"

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

package dynamodb

import (
	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
	"github.com/dapr/components-contrib/common/component"
)

// implement MetadataBuilder so each component will be properly parsed for the ast to auto-generate metadata manifest.
var _ component.MetadataBuilder = &dynamodbMetadata{}

type dynamodbMetadata struct {
	Region       string `json:"region" mapstructure:"region" mdignore:"true"`
	Endpoint     string `json:"endpoint" mapstructure:"endpoint" mdignore:"true"`
	AccessKey    string `json:"accessKey" mapstructure:"accessKey" mdignore:"true"`
	SecretKey    string `json:"secretKey" mapstructure:"secretKey" mdignore:"true"`
	SessionToken string `json:"sessionToken" mapstructure:"sessionToken" mdignore:"true"`
	Table        string `json:"table" mapstructure:"table" binding:"output"`
}

// Set the default values here.
// This unifies the setup across all components,
// and makes it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanks to Go.
func (d *dynamodbMetadata) Defaults() any {
	return dynamodbMetadata{}
}

// Note: we do not include any mdignored field.
func (d *dynamodbMetadata) Examples() any {
	return dynamodbMetadata{
		Endpoint: "http://localhost:4566",
		Table:    "Contracts",
	}
}

func (d *dynamodbMetadata) Binding() metadataschema.Binding {
	return metadataschema.Binding{
		Input:  false,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "create",
				Description: "Create a table",
			},
		},
	}
}
