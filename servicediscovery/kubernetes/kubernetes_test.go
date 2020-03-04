// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kubernetes

import (
	"fmt"
	"testing"

	"github.com/dapr/components-contrib/servicediscovery"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestResolve(t *testing.T) {
	resolver := NewKubernetesResolver(logger.NewLogger("test"))
	request := servicediscovery.ResolveRequest{ID: "myid", Namespace: "abc", Port: 1234}

	u := fmt.Sprintf("myid-dapr.abc.svc.cluster.local:1234")
	target, err := resolver.ResolveID(request)

	assert.Nil(t, err)
	assert.Equal(t, target, u)
}
