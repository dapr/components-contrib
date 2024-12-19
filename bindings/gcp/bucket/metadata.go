//go:generate make -f ../../../Makefile component-metadata-manifest type=bindings builtinAuth="gcp" status=alpha version=v1 direction=output origin=$PWD "title=GCP Storage Bucket"

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

package bucket

import (
	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
	"github.com/dapr/components-contrib/common/component"
)

// implement MetadataBuilder so each component will be properly parsed for the ast to auto-generate metadata manifest.
var _ component.MetadataBuilder = &bucketMetadata{}

type bucketMetadata struct {
	// Ignored by metadata parser because included in built-in authentication profile
	Type                string `json:"type" mapstructure:"type" mdignore:"true"`
	ProjectID           string `json:"project_id" mapstructure:"projectID" mdignore:"true" mapstructurealiases:"project_id"`
	PrivateKeyID        string `json:"private_key_id" mapstructure:"privateKeyID" mdignore:"true" mapstructurealiases:"private_key_id"`
	PrivateKey          string `json:"private_key" mapstructure:"privateKey" mdignore:"true" mapstructurealiases:"private_key"`
	ClientEmail         string `json:"client_email" mapstructure:"clientEmail" mdignore:"true" mapstructurealiases:"client_email"`
	ClientID            string `json:"client_id" mapstructure:"clientID" mdignore:"true" mapstructurealiases:"client_id"`
	AuthURI             string `json:"auth_uri" mapstructure:"authURI" mdignore:"true" mapstructurealiases:"auth_uri"`
	TokenURI            string `json:"token_uri" mapstructure:"tokenURI" mdignore:"true" mapstructurealiases:"token_uri"`
	AuthProviderCertURL string `json:"auth_provider_x509_cert_url" mapstructure:"authProviderX509CertURL" mdignore:"true" mapstructurealiases:"auth_provider_x509_cert_url"`
	ClientCertURL       string `json:"client_x509_cert_url" mapstructure:"clientX509CertURL" mdignore:"true" mapstructurealiases:"client_x509_cert_url"`

	// The bucket name.
	Bucket string `json:"bucket" mapstructure:"bucket"`
	// Configuration to decode base64 file content before saving to bucket storage. (In case of saving a file with binary content). true is the only allowed positive value. Other positive variations like "True", "1" are not acceptable.
	DecodeBase64 bool `json:"decodeBase64,string" mapstructure:"decodeBase64"`
	// Configuration to encode base64 file content before return the content. (In case of opening a file with binary content). true is the only allowed positive value. Other positive variations like "True", "1" are not acceptable.
	EncodeBase64 bool   `json:"encodeBase64,string" mapstructure:"encodeBase64"`
	SignTTL      string `json:"signTTL" mapstructure:"signTTL"  mdignore:"true"`
}

// Set the default values here.
// This unifies the setup across all components,
// and makes it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanks to Go.
func (b *bucketMetadata) Defaults() any {
	return bucketMetadata{
		DecodeBase64: false,
		EncodeBase64: false,
	}
}

// Note: we do not include any mdignored field.
func (b *bucketMetadata) Examples() any {
	return bucketMetadata{
		Bucket:       "mybucket",
		DecodeBase64: true,
		EncodeBase64: true,
	}
}

func (b *bucketMetadata) Binding() metadataschema.Binding {
	return metadataschema.Binding{
		Input:  false,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "create",
				Description: "Create an item",
			},
		},
	}
}
