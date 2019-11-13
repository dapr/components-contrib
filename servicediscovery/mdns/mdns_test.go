// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mdns

import (
	//	"fmt"
	"testing"

	"github.com/dapr/components-contrib/servicediscovery"
	"github.com/stretchr/testify/assert"
)

func TestResolve(t *testing.T) {
	resolver := NewMDNSResolver()

	request := servicediscovery.ResolveRequest{ID: "myid", Namespace: "abc", Port: 1234}

	resolver.ResolveID(&request)
	temp := resolver.Resolver

	resolver.ResolveID(&request)

	// this test that the inner Resolver object is not created more than once
	assert.Equal(t, temp, resolver.Resolver)
}
