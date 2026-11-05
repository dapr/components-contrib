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

// Package spiffe authenticates components to external services with JWT SVIDs
// minted by the Dapr runtime (workload identity federation).
package spiffe

import (
	"context"
	"errors"
	"fmt"

	"github.com/spiffe/go-spiffe/v2/svid/jwtsvid"

	kitspiffe "github.com/dapr/kit/crypto/spiffe"
	spiffecontext "github.com/dapr/kit/crypto/spiffe/context"
)

// MinterFrom returns the JWT SVID minter the Dapr runtime attached to ctx.
// The minter outlives ctx, so it can be captured from the Init context.
func MinterFrom(ctx context.Context) (kitspiffe.JWTSVIDMinter, error) {
	minter, ok := spiffecontext.JWTMinterFrom(ctx)
	if !ok {
		return nil, errors.New("workload identity federation requires the Dapr SPIFFE identity: enable mTLS, and enable JWT issuing in Sentry")
	}
	return minter, nil
}

// MintJWT mints a new JWT SVID for audience and returns it in compact form.
func MintJWT(ctx context.Context, minter kitspiffe.JWTSVIDMinter, audience string) (string, error) {
	svid, err := minter.MintJWTSVID(ctx, jwtsvid.Params{Audience: audience})
	if err != nil {
		return "", fmt.Errorf("failed to mint JWT SVID for audience %q: %w", audience, err)
	}
	return svid.Marshal(), nil
}
