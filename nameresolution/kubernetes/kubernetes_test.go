// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kubernetes

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

func TestResolve(t *testing.T) {
	resolver := NewResolver(logger.NewLogger("test"))
	request := nameresolution.ResolveRequest{ID: "myid", Namespace: "abc", Port: 1234}

	u := "myid-dapr.abc.svc.cluster.local:1234"
	target, err := resolver.ResolveID(request)

	assert.Nil(t, err)
	assert.Equal(t, target, u)
}

func TestResolveWithCustomClusterDomain(t *testing.T) {
	resolver := NewResolver(logger.NewLogger("test"))
	_ = resolver.Init(nameresolution.Metadata{
		Configuration: map[string]string{
			"clusterDomain": "mydomain.com",
		},
	})
	request := nameresolution.ResolveRequest{ID: "myid", Namespace: "abc", Port: 1234}

	u := "myid-dapr.abc.svc.mydomain.com:1234"
	target, err := resolver.ResolveID(request)

	assert.Nil(t, err)
	assert.Equal(t, target, u)
}
