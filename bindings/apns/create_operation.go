package apns

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
)

type createOperation struct {
	authorizationBuilder *authorizationBuilder
	client               *http.Client
	logger               logger.Logger
	req                  *bindings.InvokeRequest
	res                  *bindings.InvokeResponse
	err                  error

	urlPrefix           string
	deviceToken         string
	httpRequest         *http.Request
	httpResponse        *http.Response
	authorizationHeader string
}

func (op *createOperation) run() (*bindings.InvokeResponse, error) {
	op.getDeviceToken()
	op.makeHTTPRequest()
	op.getAuthorizationHeader()
	op.addHTTPHeaders()
	op.sendPushNotification()
	op.makeSuccessResponse()
	op.makeErrorResponse()
	op.closeResponseBody()
	return op.res, op.err
}

func (op *createOperation) getDeviceToken() {
	if op.err != nil {
		return
	}

	deviceToken, ok := op.req.Metadata[deviceTokenKey]
	if !ok || deviceToken == "" {
		op.err = errors.New("the device-token parameter is required")
		return
	}

	op.deviceToken = deviceToken
}

func (op *createOperation) makeHTTPRequest() {
	if op.err != nil {
		return
	}

	url := op.urlPrefix + op.deviceToken
	op.httpRequest, op.err =
		http.NewRequest(http.MethodPost, url, bytes.NewReader(op.req.Data))
}

func (op *createOperation) getAuthorizationHeader() {
	if op.err != nil {
		return
	}

	op.authorizationHeader, op.err =
		op.authorizationBuilder.getAuthorizationHeader()
}

func (op *createOperation) addHTTPHeaders() {
	if op.err != nil {
		return
	}

	op.httpRequest.Header.Add("authorization", op.authorizationHeader)
	op.addRequestHeader(pushTypeKey)
	op.addRequestHeader(messageIDKey)
	op.addRequestHeader(expirationKey)
	op.addRequestHeader(priorityKey)
	op.addRequestHeader(topicKey)
	op.addRequestHeader(collapseIDKey)
}

func (op *createOperation) addRequestHeader(key string) {
	if value, ok := op.req.Metadata[key]; ok && value != "" {
		op.httpRequest.Header.Add(key, value)
	}
}

func (op *createOperation) sendPushNotification() {
	if op.err != nil {
		return
	}

	op.logger.Debugf("Sending notification to device %v", op.deviceToken)

	op.httpResponse, op.err = op.client.Do(op.httpRequest) //nolint:bodyclose // Body.Close() is called later in the pipeline by the closeResponseBody function
}

func (op *createOperation) makeSuccessResponse() {
	if op.err != nil {
		return
	}

	if op.httpResponse.StatusCode != http.StatusOK {
		return
	}

	messageID := op.httpResponse.Header.Get(messageIDKey)
	output := notificationResponse{MessageID: messageID}
	var data bytes.Buffer
	encoder := json.NewEncoder(&data)
	op.err = encoder.Encode(&output)
	if op.err == nil {
		op.res = &bindings.InvokeResponse{Data: data.Bytes()}
	}
}

func (op *createOperation) makeErrorResponse() {
	if op.err != nil {
		return
	}

	hasResponse := op.httpResponse != nil
	if hasResponse && op.httpResponse.StatusCode == http.StatusOK {
		return
	}

	if op.httpResponse.Body == nil {
		return
	}

	var reply errorResponse
	decoder := json.NewDecoder(op.httpResponse.Body)
	op.err = decoder.Decode(&reply)
	if op.err == nil {
		op.err = errors.New(reply.Reason)
	}
}

func (op *createOperation) closeResponseBody() {
	if op.httpResponse != nil && op.httpResponse.Body != nil {
		_ = op.httpResponse.Body.Close()
	}
}
