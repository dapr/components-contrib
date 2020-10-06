// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package apns

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
)

const (
	collapseIDKey     = "apns-collapse-id"
	developmentKey    = "development"
	developmentPrefix = "https://api.sandbox.push.apple.com/3/device/"
	deviceTokenKey    = "device-token"
	expirationKey     = "apns-expiration"
	keyIDKey          = "key-id"
	messageIDKey      = "apns-id"
	priorityKey       = "apns-priority"
	privateKeyKey     = "private-key"
	productionPrefix  = "https://api.push.apple.com/3/device/"
	pushTypeKey       = "apns-push-type"
	teamIDKey         = "team-id"
	topicKey          = "apns-topic"
)

type notificationResponse struct {
	MessageID string `json:"messageID"`
}

type errorResponse struct {
	Reason    string `json:"reason"`
	Timestamp int64  `json:"timestamp"`
}

// APNS implements an outbound binding that allows services to send push
// notifications to Apple devices using Apple's Push Notification Service.
type APNS struct {
	logger               logger.Logger
	client               *http.Client
	urlPrefix            string
	authorizationBuilder *authorizationBuilder
}

// Creates a new output binding instance for the Apple Push Notification
// Service.
func NewAPNS(logger logger.Logger) *APNS {
	return &APNS{
		logger: logger,
		client: &http.Client{},
		authorizationBuilder: &authorizationBuilder{
			logger: logger,
			mutex:  sync.RWMutex{},
		},
	}
}

func (a *APNS) Init(metadata bindings.Metadata) error {
	if err := a.makeURLPrefix(metadata); err != nil {
		return err
	}

	if err := a.extractKeyID(metadata); err != nil {
		return err
	}

	if err := a.extractTeamID(metadata); err != nil {
		return err
	}

	return a.extractPrivateKey(metadata)
}

func (a *APNS) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *APNS) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	if req.Operation != bindings.CreateOperation {
		return nil, fmt.Errorf("operation not supported: %v", req.Operation)
	}

	op := &createOperation{
		authorizationBuilder: a.authorizationBuilder,
		client:               a.client,
		logger:               a.logger,
		req:                  req,
		urlPrefix:            a.urlPrefix,
	}
	return op.run()
}

func (a *APNS) makeURLPrefix(metadata bindings.Metadata) error {
	if value, ok := metadata.Properties[developmentKey]; ok && value != "" {
		switch value {
		case "true":
			a.logger.Debug("Using the development APNS service")
			a.urlPrefix = developmentPrefix

		case "false":
			a.logger.Debug("Using the production APNS service")
			a.urlPrefix = productionPrefix

		default:
			return fmt.Errorf(
				"invalid value for development parameter: %v",
				value,
			)
		}
	} else {
		a.logger.Debug("Using the production APNS service")
		a.urlPrefix = productionPrefix
	}

	return nil
}

func (a *APNS) extractKeyID(metadata bindings.Metadata) error {
	if value, ok := metadata.Properties[keyIDKey]; ok && value != "" {
		a.authorizationBuilder.keyID = value
		return nil
	}

	return errors.New("the key-id parameter is required")
}

func (a *APNS) extractTeamID(metadata bindings.Metadata) error {
	if value, ok := metadata.Properties[teamIDKey]; ok && value != "" {
		a.authorizationBuilder.teamID = value
		return nil
	}

	return errors.New("the team-id parameter is required")
}

func (a *APNS) extractPrivateKey(metadata bindings.Metadata) error {
	if value, ok := metadata.Properties[privateKeyKey]; ok && value != "" {
		block, _ := pem.Decode([]byte(value))
		if block == nil {
			return errors.New("unable to read the private key")
		}

		privateKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return err
		}

		a.authorizationBuilder.privateKey = privateKey
	} else {
		return errors.New("the private-key parameter is required")
	}

	return nil
}
