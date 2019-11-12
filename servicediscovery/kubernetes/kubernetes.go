// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kubernetes

import (
	"fmt"
	"os"

	"github.com/dapr/components-contrib/servicediscovery"
)

type Resolver struct {
}

func NewKubernetesResolver() *Resolver {
	return &Resolver{}
}

func (z *Resolver) ResolveID(req *servicediscovery.ResolveRequest) (string, error) {
	namespace := os.Getenv("NAMESPACE")

	// Dapr requires this formatting for Kubernetes services
	return fmt.Sprintf("%s-dapr.%s.svc.cluster.local:%d", req.ID, namespace, req.GrpcPort), nil
}
