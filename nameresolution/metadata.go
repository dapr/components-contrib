// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
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
)

// Metadata contains a name resolution specific set of metadata properties
type Metadata struct {
	Properties map[string]string `json:"properties"`
}
