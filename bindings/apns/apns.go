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
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sync"

	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/bindings"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
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

type APNSmetadata struct {
	Development bool   `mapstructure:"development"`
	KeyID       string `mapstructure:"key-id"`
	TeamID      string `mapstructure:"team-id"`
	PrivateKey  string `mapstructure:"private-key"`
}

// NewAPNS will create a new APNS output binding.
func NewAPNS(logger logger.Logger) bindings.OutputBinding {
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
func (a *APNS) Init(ctx context.Context, metadata bindings.Metadata) error {
	m := APNSmetadata{}
	err := kitmd.DecodeMetadata(metadata.Properties, &m)
	if err != nil {
		return err
	}

	if err := a.makeURLPrefix(m); err != nil {
		return err
	}

	if err := a.extractKeyID(m); err != nil {
		return err
	}

	if err := a.extractTeamID(m); err != nil {
		return err
	}

	return a.extractPrivateKey(m)
}

// Operations will return the set of operations supported by the APNS output
// binding. The APNS output binding only supports the "create" operation for
// sending new push notifications to the APNS service.
func (a *APNS) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

// Invoke is called by Dapr to send a push notification to the APNS output
// binding.
func (a *APNS) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	if req.Operation != bindings.CreateOperation {
		return nil, fmt.Errorf("operation not supported: %v", req.Operation)
	}

	return a.sendPushNotification(ctx, req)
}

func (a *APNS) sendPushNotification(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	deviceToken, ok := req.Metadata[deviceTokenKey]
	if !ok || deviceToken == "" {
		return nil, errors.New("the device-token parameter is required")
	}

	httpResponse, err := a.sendPushNotificationToAPNS(ctx, deviceToken, req)
	if err != nil {
		return nil, err
	}

	defer func() {
		// Drain before closing
		_, _ = io.Copy(io.Discard, httpResponse.Body)
		_ = httpResponse.Body.Close()
	}()

	if httpResponse.StatusCode == http.StatusOK {
		return makeSuccessResponse(httpResponse)
	}

	return makeErrorResponse(httpResponse)
}

func (a *APNS) sendPushNotificationToAPNS(ctx context.Context, deviceToken string, req *bindings.InvokeRequest) (*http.Response, error) {
	url := a.urlPrefix + deviceToken
	httpRequest, err := http.NewRequestWithContext(
		ctx,
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

func (a *APNS) makeURLPrefix(metadata APNSmetadata) error {
	if metadata.Development {
		a.logger.Debug("Using the development APNS service")
		a.urlPrefix = developmentPrefix
	} else {
		a.logger.Debug("Using the production APNS service")
		a.urlPrefix = productionPrefix
	}

	return nil
}

func (a *APNS) extractKeyID(metadata APNSmetadata) error {
	if metadata.KeyID != "" {
		a.authorizationBuilder.keyID = metadata.KeyID

		return nil
	}

	return errors.New("the key-id parameter is required")
}

func (a *APNS) extractTeamID(metadata APNSmetadata) error {
	if metadata.TeamID != "" {
		a.authorizationBuilder.teamID = metadata.TeamID

		return nil
	}

	return errors.New("the team-id parameter is required")
}

func (a *APNS) extractPrivateKey(metadata APNSmetadata) error {
	if metadata.PrivateKey != "" {
		block, _ := pem.Decode([]byte(metadata.PrivateKey))
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

// GetComponentMetadata returns the metadata of the component.
func (a *APNS) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := APNSmetadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BindingType)
	return
}

func (a *APNS) Close() error {
	return nil
}
