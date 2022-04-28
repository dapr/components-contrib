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

package sms

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	toNumber      = "toNumber"
	fromNumber    = "fromNumber"
	accountSid    = "accountSid"
	authToken     = "authToken"
	timeout       = "timeout"
	twilioURLBase = "https://api.twilio.com/2010-04-01/Accounts/"
)

type SMS struct {
	metadata   twilioMetadata
	logger     logger.Logger
	httpClient *http.Client
}

type twilioMetadata struct {
	toNumber   string
	fromNumber string
	accountSid string
	authToken  string
	timeout    time.Duration
}

func NewSMS(logger logger.Logger) *SMS {
	return &SMS{
		logger:     logger,
		httpClient: &http.Client{},
	}
}

func (t *SMS) Init(metadata bindings.Metadata) error {
	twilioM := twilioMetadata{
		timeout: time.Minute * 5,
	}

	if metadata.Properties[fromNumber] == "" {
		return errors.New("\"fromNumber\" is a required field")
	}
	if metadata.Properties[accountSid] == "" {
		return errors.New("\"accountSid\" is a required field")
	}
	if metadata.Properties[authToken] == "" {
		return errors.New("\"authToken\" is a required field")
	}

	twilioM.toNumber = metadata.Properties[toNumber]
	twilioM.fromNumber = metadata.Properties[fromNumber]
	twilioM.accountSid = metadata.Properties[accountSid]
	twilioM.authToken = metadata.Properties[authToken]
	if metadata.Properties[timeout] != "" {
		t, err := time.ParseDuration(metadata.Properties[timeout])
		if err != nil {
			return fmt.Errorf("error parsing timeout: %s", err)
		}
		twilioM.timeout = t
	}

	t.metadata = twilioM
	t.httpClient.Timeout = twilioM.timeout

	return nil
}

func (t *SMS) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (t *SMS) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	toNumberValue := t.metadata.toNumber
	if toNumberValue == "" {
		toNumberFromRequest, ok := req.Metadata[toNumber]
		if !ok || toNumberFromRequest == "" {
			return nil, errors.New("twilio missing \"toNumber\" field")
		}
		toNumberValue = toNumberFromRequest
	}

	v := url.Values{}
	v.Set("To", toNumberValue)
	v.Set("From", t.metadata.fromNumber)
	v.Set("Body", string(req.Data))
	vDr := *strings.NewReader(v.Encode())

	twilioURL := fmt.Sprintf("%s%s/Messages.json", twilioURLBase, t.metadata.accountSid)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", twilioURL, &vDr)
	if err != nil {
		return nil, err
	}
	httpReq.SetBasicAuth(t.metadata.accountSid, t.metadata.authToken)
	httpReq.Header.Add("Accept", "application/json")
	httpReq.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := t.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		return nil, fmt.Errorf("error from Twilio: %s", resp.Status)
	}

	return nil, nil
}
