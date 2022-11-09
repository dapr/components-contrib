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
	if config, ok := configInterface.(map[string]interface{}); ok {
		clusterDomainPtr := config[ClusterDomainKey]
		if clusterDomainPtr != nil {
			clusterDomain, _ := clusterDomainPtr.(string)
			if clusterDomain != "" {
				k.clusterDomain = clusterDomain
			}
		}
	}

	return nil
}

// ResolveID resolves name to address in Kubernetes.
func (k *resolver) ResolveID(req nameresolution.ResolveRequest) (string, error) {
	// Dapr requires this formatting for Kubernetes services
	return fmt.Sprintf("%s-dapr.%s.svc.%s:%d", req.ID, req.Namespace, k.clusterDomain, req.Port), nil
}
