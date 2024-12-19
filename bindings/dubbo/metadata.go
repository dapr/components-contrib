//go:generate make -f ../../Makefile component-metadata-manifest type=bindings status=alpha version=v1 direction=output origin=$PWD "title=Apache Dubbo"
// TODO: is this input? output? or both? No idea.
// TODO: open docs issue bc no docs for me to reference here...
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

package dubbo

import (
	"dubbo.apache.org/dubbo-go/v3/config/generic"
	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
	"github.com/dapr/components-contrib/common/component"
)

// implement MetadataBuilder so each component will be properly parsed for the ast to auto-generate metadata manifest.
var _ component.MetadataBuilder = &dubboMetadata{}

type dubboMetadata struct {
	// Logical group for the Dubbo service.
	Group string `json:"group"`

	// Version of the Dubbo service interface.
	Version string `json:"version"`

	// Fully qualified name of the Dubbo service interface.
	InterfaceName string `json:"interfaceName"`

	// Hostname of the Dubbo service provider.
	ProviderHostname string `json:"providerHostname"`

	// Port number of the Dubbo service provider.
	ProviderPort string `json:"providerPort"`

	// Name of the method to be invoked on the service.
	MethodName string `json:"methodName"`

	// TODO: these need to be refactored out of this struct
	inited bool                    `mdignore:"true"`
	client *generic.GenericService `mdignore:"true"`
}

// Set the default values here.
// This unifies the setup across all components,
// and makes it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanks to Go.
func (s *dubboMetadata) Defaults() any {
	return dubboMetadata{}
}

// Note: we do not include any mdignored field.
func (s *dubboMetadata) Examples() any {
	return dubboMetadata{
		Group:            "example-group",
		Version:          "1.0.0",
		InterfaceName:    "com.example.Service",
		ProviderHostname: "provider.example.com",
		ProviderPort:     "20880",
		MethodName:       "exampleMethod",
	}
}

func (s *dubboMetadata) Binding() metadataschema.Binding {
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
