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
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
)

type pat struct {
	username string
	token    string
}

// NewPAT returns a Strategy for HTTPS basic auth using a personal access
// token. An empty username is replaced with the GitHub-recommended
// "x-access-token".
func NewPAT(username, token string) Strategy {
	if username == "" {
		username = "x-access-token"
	}
	return &pat{username: username, token: token}
}

func (a *pat) AuthMethod(context.Context) (transport.AuthMethod, error) {
	return &githttp.BasicAuth{Username: a.username, Password: a.token}, nil
}

func (a *pat) Close() error { return nil }
