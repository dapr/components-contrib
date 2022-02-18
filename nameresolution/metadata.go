/*
Copyright 2022 The Dapr Authors
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

const (
	// MDNSInstanceName is the instance name which is broadcasted.
	MDNSInstanceName string = "name"
	// MDNSInstanceAddress is the address of the instance.
	MDNSInstanceAddress string = "address"
	// MDNSInstancePort is the port of instance.
	MDNSInstancePort string = "port"
	// MDNSInstanceID is an optional unique instance ID.
	MDNSInstanceID string = "instance"

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
)

// Metadata contains a name resolution specific set of metadata properties.
type Metadata struct {
	Properties    map[string]string `json:"properties"`
	Configuration interface{}
}
