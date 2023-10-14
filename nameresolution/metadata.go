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

package nameresolution

import (
	"strconv"

	"github.com/dapr/components-contrib/metadata"
)

const (
	// HostAddress is the address of the instance.
	HostAddress string = "HOST_ADDRESS"
	// DaprHTTPPort is the dapr api http port.
	DaprHTTPPort string = "DAPR_HTTP_PORT"
	// DaprPort is the dapr internal grpc port (sidecar to sidecar).
	DaprPort string = "DAPR_PORT"
	// AppPort is the port of the application, http/grpc depending on mode.
	AppPort string = "APP_PORT"
	// AppID is the ID of the application.
	AppID string = "APP_ID"
	// Namespace is the namespace of the application.
	Namespace string = "NAMESPACE"
)

// Metadata contains a name resolution specific set of metadata properties.
type Metadata struct {
	metadata.Base `json:",inline"`
	Configuration interface{}
}

// GetHostAddress returns the host address property.
func (m Metadata) GetHostAddress() string {
	return m.Properties[HostAddress]
}

// GetDaprHTTPPort returns the Dapr HTTP port property.
// If the port is invalid, returns 0.
func (m Metadata) GetDaprHTTPPort() int {
	p, _ := strconv.Atoi(m.Properties[DaprHTTPPort])
	if p < 0 {
		p = 0
	}
	return p
}

// GetDaprPort returns the Dapr port property.
// If the port is invalid, returns 0.
func (m Metadata) GetDaprPort() int {
	p, _ := strconv.Atoi(m.Properties[DaprPort])
	if p < 0 {
		p = 0
	}
	return p
}

// GetAppPort returns the app port property.
// If the port is invalid, returns 0.
func (m Metadata) GetAppPort() int {
	p, _ := strconv.Atoi(m.Properties[AppPort])
	if p < 0 {
		p = 0
	}
	return p
}

// GetAppID returns the app ID property.
func (m Metadata) GetAppID() string {
	return m.Properties[AppID]
}

// GetNamespace returns the namespace property.
func (m Metadata) GetNamespace() string {
	return m.Properties[AppID]
}
