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

type KubernetesResolver struct {
}

func NewKubernetesResolver() *KubernetesResolver {
	return &KubernetesResolver{}
}

func (z *KubernetesResolver) ResolveID(req *servicediscovery.ResolveRequest) (string, error) {
	namespace := os.Getenv("NAMESPACE")

	// Dapr requires this formatting for Kubernetes services
	return fmt.Sprintf("%s-dapr.%s.svc.cluster.local:%d", req.Id, namespace, req.GrpcPort), nil
}
