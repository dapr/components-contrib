// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kubernetes

import (
	"fmt"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/config"
	"github.com/dapr/kit/logger"
)

const (
	DefaultClusterDomain = "cluster.local"
	ClusterDomainKey     = "clusterDomain"
)

type resolver struct {
	logger        logger.Logger
	clusterDomain string
}

// NewResolver creates Kubernetes name resolver.
func NewResolver(logger logger.Logger) nameresolution.Resolver {
	return &resolver{
		logger:        logger,
		clusterDomain: DefaultClusterDomain,
	}
}

// Init initializes Kubernetes name resolver.
func (k *resolver) Init(metadata nameresolution.Metadata) error {
	configInterface, err := config.Normalize(metadata.Configuration)
	if err != nil {
		return err
	}
	if config, ok := configInterface.(map[string]string); ok {
		clusterDomain := config[ClusterDomainKey]
		if clusterDomain != "" {
			k.clusterDomain = clusterDomain
		}
	}

	return nil
}

// ResolveID resolves name to address in Kubernetes.
func (k *resolver) ResolveID(req nameresolution.ResolveRequest) (string, error) {
	// Dapr requires this formatting for Kubernetes services
	return fmt.Sprintf("%s-dapr.%s.svc.%s:%d", req.ID, req.Namespace, k.clusterDomain, req.Port), nil
}
