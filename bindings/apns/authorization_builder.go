// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package apns

import (
	"sync"
	"time"

	"github.com/golang-jwt/jwt"

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
	privateKey          interface{}
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

	now := time.Now()
	claims := jwt.StandardClaims{
		IssuedAt: time.Now().Unix(),
		Issuer:   a.teamID,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = a.keyID
	signedToken, err := token.SignedString(a.privateKey)
	if err != nil {
		return "", err
	}

	a.authorizationHeader = "bearer " + signedToken
	a.tokenExpiresAt = now.Add(expirationMinutes)

	return a.authorizationHeader, nil
}
