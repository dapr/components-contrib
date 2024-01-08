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

package nameresolution

import (
	"math/rand"
)

// Resolver is the interface of name resolver.
type Resolver interface {
	// Init initializes name resolver.
	Init(metadata Metadata) error
	// ResolveID resolves name to address.
	ResolveID(req ResolveRequest) (string, error)
}

// ResolverMulti is an optional interface for name resolvers that can return multiple addresses.
type ResolverMulti interface {
	ResolveIDMulti(req ResolveRequest) (AddressList, error)
}

// AddressList is a list of addresses resolved by the nameresolver
type AddressList []string

// Pick returns a random address from the list
func (a AddressList) Pick() string {
	l := len(a)
	switch l {
	case 0:
		return ""
	case 1:
		return a[0]
	default:
		// We use math/rand here as we are just picking a random address, so we don't need a CSPRNG
		//nolint:gosec
		return a[rand.Intn(l)]
	}
}
