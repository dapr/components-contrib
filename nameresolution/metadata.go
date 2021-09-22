// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

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
