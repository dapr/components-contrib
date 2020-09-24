package apns

import (
	"bytes"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/dgrijalva/jwt-go"
	"net/http"
	"sync"
	"time"
)

const (
	collapseIDKey = "apns-collapse-id"
	developmentKey = "development"
	developmentPrefix = "https://api.sandbox.push.apple.com/3/device/"
	deviceTokenKey = "device-token"
	expirationKey = "apns-expiration"
	keyIDKey = "key-id"
	messageIDKey = "apns-id"
	priorityKey = "apns-priority"
	privateKeyKey = "private-key"
	productionPrefix = "https://api.push.apple.com/3/device/"
	pushTypeKey = "apns-push-type"
	teamIDKey = "team-id"
	topicKey = "apns-topic"
)

type notificationResponse struct {
	MessageID string `json:"messageID"`
}

type errorResponse struct {
	Reason string `json:"reason"`
	Timestamp int64 `json:"timestamp"`
}

// APNS implements an outbound binding that allows services to send push
// notifications to Apple devices using Apple's Push Notification Service.
type APNS struct {
	logger logger.Logger
	urlPrefix string
	keyID string
	teamID string
	privateKey interface{}
	authorizationHeader string
	tokenExpiresAt time.Time
	mutex sync.RWMutex
}

// Creates a new output binding instance for the Apple Push Notification
// Service.
func NewAPNS(logger logger.Logger) *APNS {
	return &APNS{
		logger: logger,
		mutex: sync.RWMutex{},
	}
}

func (a *APNS) Init(metadata bindings.Metadata) error {
	if value, ok := metadata.Properties[developmentKey]; ok && value != "" {
		switch value {
		case "true":
			a.logger.Debug("Using development APNS service")
			a.urlPrefix = developmentPrefix

		case "false":
			a.logger.Debug("Using production APNS service")
			a.urlPrefix = productionPrefix

		default:
			return fmt.Errorf(
				"invalid value for development parameter: %v",
				value,
				)
		}
	} else {
		a.urlPrefix = productionPrefix
	}

	if value, ok := metadata.Properties[keyIDKey]; ok && value != "" {
		a.keyID = value
	} else {
		return errors.New("the key-id parameter is required")
	}

	if value, ok := metadata.Properties[teamIDKey]; ok && value != "" {
		a.teamID = value
	} else {
		return errors.New("the team-id parameter is required")
	}

	if value, ok := metadata.Properties[privateKeyKey]; ok && value != "" {
		block, _ := pem.Decode([]byte(value))
		if block == nil {
			return errors.New("unable to read the private key")
		}

		privateKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return err
		}

		a.privateKey = privateKey
	}

	return nil
}

func (a *APNS) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *APNS) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	if req.Operation != bindings.CreateOperation {
		return nil, fmt.Errorf("operation not supported: %v", req.Operation)
	}

	deviceToken, ok := req.Metadata[deviceTokenKey]
	if !ok || deviceToken == "" {
		return nil, errors.New("the device-token parameter is required")
	}

	url := a.urlPrefix + deviceToken
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(req.Data))
	if err != nil {
		return nil, err
	}

	authorizationHeader, err := a.getAuthorizationHeader()
	if err != nil {
		return nil, err
	}

	a.logger.Debugf("Sending notification to device %v", deviceToken)
	request.Header.Add("authorization", authorizationHeader)
	addRequestHeader(pushTypeKey, request, req.Metadata)
	addRequestHeader(messageIDKey, request, req.Metadata)
	addRequestHeader(expirationKey, request, req.Metadata)
	addRequestHeader(priorityKey, request, req.Metadata)
	addRequestHeader(topicKey, request, req.Metadata)
	addRequestHeader(collapseIDKey, request, req.Metadata)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	if response.StatusCode == http.StatusOK {
		res := &bindings.InvokeResponse{}
		messageID := response.Header.Get(messageIDKey)
		output := notificationResponse{MessageID: messageID}
		var data bytes.Buffer
		encoder := json.NewEncoder(&data)
		err := encoder.Encode(&output)
		if err != nil {
			return nil, err
		}

		res.Data = data.Bytes()
		return res, nil
	}

	var errorResponse errorResponse
	decoder := json.NewDecoder(response.Body)
	err = decoder.Decode(&errorResponse)
	if err != nil {
		return nil, err
	}

	return nil, errors.New(errorResponse.Reason)
}

func (a *APNS) getAuthorizationHeader() (string, error) {
	authorizationHeader, ok := a.readAuthorizationHeader()
	if ok {
		return authorizationHeader, nil
	}

	return a.generateAuthorizationHeader()
}

func (a *APNS) readAuthorizationHeader() (string, bool) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	if time.Now().After(a.tokenExpiresAt) {
		return "", false
	}

	return a.authorizationHeader, true
}

func (a *APNS) generateAuthorizationHeader() (string, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	a.logger.Debug("Authorization token expired; generating new token")

	now := time.Now()
	claims := jwt.StandardClaims{
		IssuedAt: now.Unix(),
		Issuer: a.teamID,
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

func addRequestHeader(key string, request *http.Request, metadata map[string]string) {
	if value, ok := metadata[key]; ok && value != "" {
		request.Header.Add(key, value)
	}
}