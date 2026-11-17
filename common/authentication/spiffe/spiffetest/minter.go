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

// Package spiffetest provides a fake JWT SVID minter for tests.
package spiffetest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"strconv"
	"sync"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/spiffe/go-spiffe/v2/svid/jwtsvid"
)

// Minter mints JWT SVIDs signed with a throwaway key, each with a unique jti,
// and records them.
type Minter struct {
	// Err, when set, is returned instead of minting.
	Err error

	lock   sync.Mutex
	key    *ecdsa.PrivateKey
	tokens []string
}

func (m *Minter) MintJWTSVID(_ context.Context, params jwtsvid.Params) (*jwtsvid.SVID, error) {
	if m.Err != nil {
		return nil, m.Err
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	if m.key == nil {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, err
		}
		m.key = key
	}

	now := time.Now()
	tkn, err := jwt.NewBuilder().
		JwtID(strconv.Itoa(len(m.tokens))).
		Subject("spiffe://example.org/ns/default/app").
		Audience(append([]string{params.Audience}, params.ExtraAudiences...)).
		IssuedAt(now).
		Expiration(now.Add(5 * time.Minute)).
		Build()
	if err != nil {
		return nil, err
	}
	signed, err := jwt.Sign(tkn, jwt.WithKey(jwa.ES256, m.key))
	if err != nil {
		return nil, err
	}

	m.tokens = append(m.tokens, string(signed))
	return jwtsvid.ParseInsecure(string(signed), []string{params.Audience})
}

// Tokens returns the JWT SVIDs minted so far, in order.
func (m *Minter) Tokens() []string {
	m.lock.Lock()
	defer m.lock.Unlock()
	return append([]string(nil), m.tokens...)
}
