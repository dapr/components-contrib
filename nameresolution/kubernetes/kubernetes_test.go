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
		Configuration: map[string]interface{}{
			"clusterDomain": "mydomain.com",
		},
	})
	request := nameresolution.ResolveRequest{ID: "myid", Namespace: "abc", Port: 1234}

	u := "myid-dapr.abc.svc.mydomain.com:1234"
	target, err := resolver.ResolveID(request)

	assert.Nil(t, err)
	assert.Equal(t, target, u)
}
