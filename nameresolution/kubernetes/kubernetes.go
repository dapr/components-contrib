// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kubernetes

import (
	"fmt"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

const (
	DefaultClusterDomain = "cluster.local"
	ClusterDomainKey     = "clusterDomain"
)

type resolver struct {
	logger logger.Logger
}

// NewResolver creates Kubernetes name resolver.
func NewResolver(logger logger.Logger) nameresolution.Resolver {
	return &resolver{logger: logger}
}

// Init initializes Kubernetes name resolver.
func (k *resolver) Init(metadata nameresolution.Metadata) error {
	return nil
}

// ResolveID resolves name to address in Kubernetes.
func (k *resolver) ResolveID(req nameresolution.ResolveRequest) (string, error) {
	clusterDomain := req.Data[ClusterDomainKey]
	if clusterDomain == "" {
		clusterDomain = DefaultClusterDomain
	}

	// Dapr requires this formatting for Kubernetes services
	return fmt.Sprintf("%s-dapr.%s.svc.%s:%d", req.ID, req.Namespace, clusterDomain, req.Port), nil
}
