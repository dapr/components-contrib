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

package redis

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
)

// rotatingTokenCredential is a stub azcore.TokenCredential whose token can be
// swapped at runtime, simulating the credential cache crossing the token-TTL
// boundary (azcore returns a refreshed token once the old one nears expiry).
type rotatingTokenCredential struct {
	mu    sync.Mutex
	token string
}

func (r *rotatingTokenCredential) set(tok string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.token = tok
}

func (r *rotatingTokenCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return azcore.AccessToken{Token: r.token, ExpiresOn: time.Now().Add(time.Hour)}, nil
}

// TestEntraIDTokenRotationAcrossConnections deterministically simulates the
// dapr/components-contrib#3554 token-TTL boundary, without waiting for a real
// Entra token to expire:
//
//  1. the server (miniredis, AUTH <oid> <token>) initially accepts token-A;
//  2. the boundary is crossed: the server only accepts token-B, and the
//     credential starts returning token-B — exactly what happens at TTL when
//     the old token dies and azcore's cache refreshes;
//  3. connection churn (tiny ConnMaxLifetime, as with the maxConnAge metadata)
//     forces brand-new pool connections.
//
// With the OnConnect hook, new connections AUTH with the freshly-fetched
// token-B and operations keep succeeding. The counterfactual subtest shows the
// pre-fix behavior (static snapshot password) fails at the same boundary,
// proving this test would catch a regression.
func TestEntraIDTokenRotationAcrossConnections(t *testing.T) {
	const oid = "00000000-0000-0000-0000-000000000000"
	ctx := context.Background()

	t.Run("OnConnect picks up rotated token on new connections", func(t *testing.T) {
		mr := miniredis.RunT(t)
		mr.RequireUserAuth(oid, "token-A")

		cred := &rotatingTokenCredential{token: "token-A"}
		s := &Settings{
			Host:                   mr.Addr(),
			RedisType:              NodeType,
			UseEntraID:             true,
			MaxConnAge:             Duration(50 * time.Millisecond), // aggressive churn: every op after this dials fresh
			entraIDUsername:        oid,
			entraIDTokenCredential: cred,
		}
		c, err := newV9Client(s)
		require.NoError(t, err)
		defer c.Close()

		// Phase 1: connections AUTH with token-A.
		require.NoError(t, c.DoWrite(ctx, "SET", "k", "before-boundary"))

		// Phase 2: cross the TTL boundary — server rejects token-A, credential
		// now yields token-B.
		mr.RequireUserAuth(oid, "token-B")
		cred.set("token-B")

		// Phase 3: outlive ConnMaxLifetime so the pool retires the old
		// connection; the next op must dial fresh and AUTH via OnConnect.
		time.Sleep(80 * time.Millisecond)
		require.NoError(t, c.DoWrite(ctx, "SET", "k", "after-boundary"),
			"new pool connection must AUTH with the rotated token via OnConnect")
		got, err := c.Get(ctx, "k")
		require.NoError(t, err)
		require.Equal(t, "after-boundary", got)
	})

	t.Run("counterfactual: static snapshot password fails at the boundary (the #3554 bug)", func(t *testing.T) {
		mr := miniredis.RunT(t)
		mr.RequireUserAuth(oid, "token-A")

		// Pre-fix behavior: token snapshotted into Username/Password at init,
		// no OnConnect. New connections replay the stale snapshot.
		s := &Settings{
			Host:       mr.Addr(),
			RedisType:  NodeType,
			Username:   oid,
			Password:   "token-A",
			MaxConnAge: Duration(50 * time.Millisecond),
		}
		c, err := newV9Client(s)
		require.NoError(t, err)
		defer c.Close()

		require.NoError(t, c.DoWrite(ctx, "SET", "k", "before-boundary"))

		// Boundary: token-A dies server-side.
		mr.RequireUserAuth(oid, "token-B")

		time.Sleep(80 * time.Millisecond)
		err = c.DoWrite(ctx, "SET", "k", "after-boundary")
		require.Error(t, err,
			"static snapshot password must fail once the old token is invalid — this is the bug the OnConnect hook fixes")
	})
}
