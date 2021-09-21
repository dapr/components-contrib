// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package apns

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"sync"

	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
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

// NewAPNS will create a new APNS output binding.
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

// Init will configure the APNS output binding using the metadata specified
// in the binding's configuration.
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

// Operations will return the set of operations supported by the APNS output
// binding. The APNS output binding only supports the "create" operation for
// sending new push notifications to the APNS service.
func (a *APNS) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

// Invoke is called by Dapr to send a push notification to the APNS output
// binding.
func (a *APNS) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	if req.Operation != bindings.CreateOperation {
		return nil, fmt.Errorf("operation not supported: %v", req.Operation)
	}

	return a.sendPushNotification(req)
}

func (a *APNS) sendPushNotification(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	deviceToken, ok := req.Metadata[deviceTokenKey]
	if !ok || deviceToken == "" {
		return nil, errors.New("the device-token parameter is required")
	}

	httpResponse, err := a.sendPushNotificationToAPNS(deviceToken, req)
	if err != nil {
		return nil, err
	}

	defer httpResponse.Body.Close()

	if httpResponse.StatusCode == http.StatusOK {
		return makeSuccessResponse(httpResponse)
	}

	return makeErrorResponse(httpResponse)
}

func (a *APNS) sendPushNotificationToAPNS(deviceToken string, req *bindings.InvokeRequest) (*http.Response, error) {
	url := a.urlPrefix + deviceToken
	httpRequest, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		url,
		bytes.NewReader(req.Data),
	)
	if err != nil {
		return nil, err
	}

	authorizationHeader, err := a.authorizationBuilder.getAuthorizationHeader()
	if err != nil {
		return nil, err
	}

	httpRequest.Header.Add("authorization", authorizationHeader)
	addRequestHeader(pushTypeKey, req.Metadata, httpRequest)
	addRequestHeader(messageIDKey, req.Metadata, httpRequest)
	addRequestHeader(expirationKey, req.Metadata, httpRequest)
	addRequestHeader(priorityKey, req.Metadata, httpRequest)
	addRequestHeader(topicKey, req.Metadata, httpRequest)
	addRequestHeader(collapseIDKey, req.Metadata, httpRequest)

	return a.client.Do(httpRequest)
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

func addRequestHeader(key string, metadata map[string]string, httpRequest *http.Request) {
	if value, ok := metadata[key]; ok && value != "" {
		httpRequest.Header.Add(key, value)
	}
}

func makeSuccessResponse(httpResponse *http.Response) (*bindings.InvokeResponse, error) {
	messageID := httpResponse.Header.Get(messageIDKey)
	output := notificationResponse{MessageID: messageID}
	var data bytes.Buffer
	encoder := jsoniter.NewEncoder(&data)
	err := encoder.Encode(output)
	if err != nil {
		return nil, err
	}

	return &bindings.InvokeResponse{Data: data.Bytes()}, nil
}

func makeErrorResponse(httpResponse *http.Response) (*bindings.InvokeResponse, error) {
	var errorReply errorResponse
	decoder := jsoniter.NewDecoder(httpResponse.Body)
	err := decoder.Decode(&errorReply)
	if err == nil {
		err = errors.New(errorReply.Reason)
	}

	return nil, err
}
