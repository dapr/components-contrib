// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package apns

import (
	"sync"
	"time"

	"github.com/dapr/dapr/pkg/logger"
	"github.com/dgrijalva/jwt-go"
)

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
		IssuedAt: now.Unix(),
		Issuer:   a.teamID,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = a.keyID
	signedToken, err := token.SignedString(a.privateKey)
	if err != nil {
		return "", err
	}

	a.authorizationHeader = "bearer " + signedToken
	a.tokenExpiresAt = now.Add(time.Minute * 55)

	return a.authorizationHeader, nil
}
