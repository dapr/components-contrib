// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kubernetes

import (
	"fmt"
	"os"
	"testing"

	"github.com/dapr/components-contrib/servicediscovery"
	"github.com/stretchr/testify/assert"
)

func TestResolve(t *testing.T) {
	resolver := NewKubernetesResolver()
	request := servicediscovery.ResolveRequest{Id: "myid", GrpcPort: 1234}

	// ResolveID(), the function we are testing, formats this environment variable into part of the returned string.
	// So as not to affect the local environment, we'll just read in whatever value the local environment
	// has it set to (probably nothing, or empty string) and verify the string format vs that.
	namespace := os.Getenv("NAMESPACE")
	u := fmt.Sprintf("myid-dapr.%s.svc.cluster.local:1234", namespace)
	target, err := resolver.ResolveID(&request)

	assert.Nil(t, err)
	assert.Equal(t, target, u)
}
