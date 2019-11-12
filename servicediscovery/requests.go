// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package servicediscovery

type ResolveRequest struct {
	ID       string
	GrpcPort int
	Data     map[string]string
}
