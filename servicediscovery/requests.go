// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package servicediscovery

type ResolveRequest struct {
	Id       string
	GrpcPort int
	Data     map[string]string
}
