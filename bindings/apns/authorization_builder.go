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

package apns

import (
	"crypto"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"

	"github.com/dapr/kit/logger"
)

// The "issued at" timestamp in the JWT must be within one hour from the
// APNS server time. I set the expiration time at 55 minutes to ensure that
// a new certificate gets generated before it gets too close and risking a
// failure.
const expirationMinutes = time.Minute * 55

type authorizationBuilder struct {
	logger              logger.Logger
	mutex               sync.RWMutex
	authorizationHeader string
	tokenExpiresAt      time.Time
	keyID               string
	teamID              string
	privateKey          crypto.PrivateKey
	pk                  jwk.Key
}

func (a *authorizationBuilder) setPK() error {
	if a.privateKey == nil {
		return errors.New("privateKey property is nil")
	}
	if a.keyID == "" {
		return errors.New("keyID property is nil")
	}

	pk, err := jwk.FromRaw(a.privateKey)
	if err != nil {
		return fmt.Errorf("failed to parse private key: %w", err)
	}
	pk.Set("kid", a.keyID)

	a.pk = pk
	return nil
}

func (a *authorizationBuilder) getAuthorizationHeader() (string, error) {
	authorizationHeader, ok := a.readAuthorizationHeader()
	if ok {
		return authorizationHeader, nil
	}

	return a.generateAuthorizationHeader()
}

func (a *authorizationBuilder) readAuthorizationHeader() (string, bool) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	if time.Now().After(a.tokenExpiresAt) {
		return "", false
	}

	return a.authorizationHeader, true
}

func (a *authorizationBuilder) generateAuthorizationHeader() (string, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	a.logger.Debug("Authorization token expired; generating new token")

	var err error
	if a.pk == nil {
		err = a.setPK()
		if err != nil {
			return "", err
		}
	}

	now := time.Now()
	token, err := jwt.NewBuilder().
		Issuer(a.teamID).
		IssuedAt(now).
		Build()
	if err != nil {
		return "", fmt.Errorf("failed to build token: %w", err)
	}
	signed, err := jwt.Sign(token, jwt.WithKey(jwa.ES256, a.pk))
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}

	a.authorizationHeader = "bearer " + string(signed)
	a.tokenExpiresAt = now.Add(expirationMinutes)

	return a.authorizationHeader, nil
}
