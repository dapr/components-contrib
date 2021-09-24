// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package dns

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

func TestResolve(t *testing.T) {
	resolver := NewResolver(logger.NewLogger("test"))
	request := nameresolution.ResolveRequest{ID: "myid", Namespace: "abc", Port: 1234}

	u := "myid-dapr.abc.svc:1234"
	target, err := resolver.ResolveID(request)

	assert.Nil(t, err)
	assert.Equal(t, target, u)
}
