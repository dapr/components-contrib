//go:generate make -f ../../../Makefile component-metadata-manifest type=bindings status=alpha version=v1 direction=output origin=$PWD "title=Alibaba Cloud Log Storage Service"

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
package sls

import (
	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
	"github.com/dapr/components-contrib/common/component"
)

// implement MetadataBuilder so each component will be properly parsed for the ast to auto-generate metadata manifest.
var _ component.MetadataBuilder = &slsMetadata{}

type slsMetadata struct {
	// Alicloud SLS endpoint.
	Endpoint string `json:"endpoint" mapstructure:"endpoint"`
	// Access key ID credential.
	AccessKeyID string `json:"accessKeyID" mapstructure:"accessKeyID"`
	// Access key credential secret.
	AccessKeySecret string `json:"accessKeySecret" mapstructure:"accessKeySecret"`
}

// Set the default values here.
// This unifies the setup across all components,
// and makes it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanks to Go.
func (sls *slsMetadata) Defaults() any {
	return slsMetadata{}
}

// Note: we do not include any mdignored field.
func (sls *slsMetadata) Examples() any {
	return slsMetadata{}
}

func (sls *slsMetadata) Binding() metadataschema.Binding {
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
