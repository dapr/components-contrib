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

// These constants are used for the "legacy" way to pass instance information using a map.
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
	Instance      Instance
	Configuration any
}

// Instance contains information about the instance.
type Instance struct {
	// App ID.
	AppID string
	// Namespace of the app.
	Namespace string
	// Address of the instance.
	Address string
	// Dapr HTTP API port.
	DaprHTTPPort int
	// Dapr internal gRPC port (for sidecar-to-sidecar communication).
	DaprInternalPort int
	// Port the application is listening on (either HTTP or gRPC).
	AppPort int
}

// GetPropertiesMap returns a map with the instance properties.
// This is used by components that haven't adopted the new Instance struct to receive instance information.
func (m Metadata) GetPropertiesMap() map[string]string {
	var daprHTTPPort, daprPort, appPort string
	if m.Instance.DaprHTTPPort > 0 {
		daprHTTPPort = strconv.Itoa(m.Instance.DaprHTTPPort)
	}
	if m.Instance.DaprInternalPort > 0 {
		daprPort = strconv.Itoa(m.Instance.DaprInternalPort)
	}
	if m.Instance.AppPort > 0 {
		appPort = strconv.Itoa(m.Instance.AppPort)
	}

	return map[string]string{
		HostAddress:  m.Instance.Address,
		DaprHTTPPort: daprHTTPPort,
		DaprPort:     daprPort,
		AppPort:      appPort,
		AppID:        m.Instance.AppID,
		Namespace:    m.Instance.Namespace,
	}
}
