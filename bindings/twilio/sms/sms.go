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
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/dapr/components-contrib/bindings"
	commonutils "github.com/dapr/components-contrib/common/utils"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
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
	ToNumber   string        `mapstructure:"toNumber"`
	FromNumber string        `mapstructure:"fromNumber"`
	AccountSid string        `mapstructure:"accountSid"`
	AuthToken  string        `mapstructure:"authToken"`
	Timeout    time.Duration `mapstructure:"timeout"`
}

func NewSMS(logger logger.Logger) bindings.OutputBinding {
	return &SMS{
		logger: logger,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (t *SMS) Init(_ context.Context, meta bindings.Metadata) error {
	twilioM := twilioMetadata{
		Timeout: time.Minute * 5,
	}

	err := kitmd.DecodeMetadata(meta.Properties, &twilioM)
	if err != nil {
		return err
	}

	if twilioM.FromNumber == "" {
		return errors.New(`"fromNumber" is a required field`)
	}
	if twilioM.AccountSid == "" {
		return errors.New(`"accountSid" is a required field`)
	}
	if twilioM.AuthToken == "" {
		return errors.New(`"authToken" is a required field`)
	}

	t.metadata = twilioM
	t.httpClient.Timeout = twilioM.Timeout

	return nil
}

func (t *SMS) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (t *SMS) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	toNumberValue := t.metadata.ToNumber
	if toNumberValue == "" {
		toNumberFromRequest, ok := req.Metadata[toNumber]
		if !ok || toNumberFromRequest == "" {
			return nil, errors.New("twilio missing \"toNumber\" field")
		}
		toNumberValue = toNumberFromRequest
	}

	body := commonutils.Unquote(req.Data)

	v := url.Values{}
	v.Set("To", toNumberValue)
	v.Set("From", t.metadata.FromNumber)
	v.Set("Body", body)

	twilioURL := twilioURLBase + t.metadata.AccountSid + "/Messages.json"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, twilioURL, strings.NewReader(v.Encode()))
	if err != nil {
		return nil, err
	}
	httpReq.SetBasicAuth(t.metadata.AccountSid, t.metadata.AuthToken)
	httpReq.Header.Add("Accept", "application/json")
	httpReq.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := t.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() {
		// Drain the body before closing
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
	}()
	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		return nil, fmt.Errorf("error from Twilio (%d): %s", resp.StatusCode, resp.Status)
	}

	return nil, nil
}

// GetComponentMetadata returns the metadata of the component.
func (t *SMS) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := twilioMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}

func (t *SMS) Close() error {
	return nil
}
