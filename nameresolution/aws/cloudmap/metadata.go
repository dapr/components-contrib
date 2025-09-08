/*
Copyright 2025 The Dapr Authors
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

package cloudmap

import (
	"errors"
)

// cloudMapMetadata contains configuration for AWS CloudMap name resolver.
type cloudMapMetadata struct {
	// AWS Authentication
	Region       string `json:"region" mapstructure:"region"`
	Endpoint     string `json:"endpoint" mapstructure:"endpoint"`
	AccessKey    string `json:"accessKey" mapstructure:"accessKey"`
	SecretKey    string `json:"secretKey" mapstructure:"secretKey"`
	SessionToken string `json:"sessionToken" mapstructure:"sessionToken"`

	// CloudMap Configuration
	NamespaceName   string `json:"namespaceName" mapstructure:"namespaceName"`
	NamespaceID     string `json:"namespaceId" mapstructure:"namespaceId"`
	DefaultDaprPort int    `json:"defaultDaprPort" mapstructure:"defaultDaprPort"`
}

// Validate validates the metadata configuration.
func (m *cloudMapMetadata) Validate() error {
	if m.NamespaceName == "" && m.NamespaceID == "" {
		return errors.New("either namespaceName or namespaceId must be provided")
	}
	return nil
}