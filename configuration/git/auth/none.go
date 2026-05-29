/*
Copyright 2026 The Dapr Authors
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

package auth

import (
	"context"

	"github.com/go-git/go-git/v5/plumbing/transport"
)

type none struct{}

// NewNone returns a Strategy that performs no authentication — used for
// public HTTPS repos and file:// URLs.
func NewNone() Strategy { return &none{} }

func (*none) AuthMethod(context.Context) (transport.AuthMethod, error) { return nil, nil }
func (*none) Close() error                                             { return nil }
