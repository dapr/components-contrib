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

package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	webhookContentType       = "application/json"
	defaultHTTPClientTimeout = time.Second * 30
)

type DingTalkWebhook struct {
	logger     logger.Logger
	settings   Settings
	httpClient *http.Client
}

type webhookResult struct {
	ErrCode int    `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
}

type outgoingWebhook struct {
	handler bindings.Handler
}

var webhooks = struct { // nolint: gochecknoglobals
	sync.RWMutex
	m map[string]*outgoingWebhook
}{m: make(map[string]*outgoingWebhook)}

func NewDingTalkWebhook(l logger.Logger) *DingTalkWebhook {
	// See guidance on proper HTTP client settings here:
	// https://medium.com/@nate510/don-t-use-go-s-default-http-client-4804cb19f779
	dialer := &net.Dialer{ //nolint:exhaustivestruct
		Timeout: 5 * time.Second,
	}
	netTransport := &http.Transport{ //nolint:exhaustivestruct
		DialContext:         dialer.DialContext,
		TLSHandshakeTimeout: 5 * time.Second,
	}
	httpClient := &http.Client{ //nolint:exhaustivestruct
		Timeout:   defaultHTTPClientTimeout,
		Transport: netTransport,
	}

	return &DingTalkWebhook{ //nolint:exhaustivestruct
		logger:     l,
		httpClient: httpClient,
	}
}

// Init performs metadata parsing.
func (t *DingTalkWebhook) Init(metadata bindings.Metadata) error {
	var err error
	if err = t.settings.Decode(metadata.Properties); err != nil {
		return fmt.Errorf("dingtalk configuration error: %w", err)
	}
	if err = t.settings.Validate(); err != nil {
		return fmt.Errorf("dingtalk configuration error: %w", err)
	}

	return nil
}

// Read triggers the outgoing webhook, not yet production ready.
func (t *DingTalkWebhook) Read(handler bindings.Handler) error {
	t.logger.Debugf("dingtalk webhook: start read input binding")

	webhooks.Lock()
	defer webhooks.Unlock()
	_, loaded := webhooks.m[t.settings.ID]
	if loaded {
		return fmt.Errorf("dingtalk webhook error: duplicate id %s", t.settings.ID)
	}
	webhooks.m[t.settings.ID] = &outgoingWebhook{handler: handler}

	return nil
}

// Operations returns list of operations supported by dingtalk webhook binding.
func (t *DingTalkWebhook) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation, bindings.GetOperation}
}

func (t *DingTalkWebhook) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	rst := &bindings.InvokeResponse{Metadata: map[string]string{}, Data: nil}
	switch req.Operation {
	case bindings.CreateOperation:
		return rst, t.sendMessage(ctx, req)
	case bindings.GetOperation:
		return rst, t.receivedMessage(ctx, req)
	case bindings.DeleteOperation, bindings.ListOperation:
		return rst, fmt.Errorf("dingtalk webhook error: unsupported operation %s", req.Operation)
	default:
		return rst, fmt.Errorf("dingtalk webhook error: unsupported operation %s", req.Operation)
	}
}

func (t *DingTalkWebhook) getOutgoingWebhook() (*outgoingWebhook, error) {
	webhooks.RLock()
	defer webhooks.RUnlock()
	item, loaded := webhooks.m[t.settings.ID]
	if !loaded {
		return nil, fmt.Errorf("dingtalk webhook error: invalid component metadata.id %s", t.settings.ID)
	}

	return item, nil
}

func (t *DingTalkWebhook) receivedMessage(ctx context.Context, req *bindings.InvokeRequest) error {
	item, err := t.getOutgoingWebhook()
	if err != nil {
		return err
	}

	in := &bindings.ReadResponse{Data: req.Data, Metadata: req.Metadata}
	if _, err = item.handler(ctx, in); err != nil {
		return err
	}

	return nil
}

func (t *DingTalkWebhook) sendMessage(ctx context.Context, req *bindings.InvokeRequest) error {
	msg := req.Data

	postURL, err := getPostURL(t.settings.URL, t.settings.Secret)
	if err != nil {
		return fmt.Errorf("dingtalk webhook error: get url failed. %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, defaultHTTPClientTimeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, "POST", postURL, bytes.NewReader(msg))
	if err != nil {
		return fmt.Errorf("dingtalk webhook error: new request failed. %w", err)
	}

	httpReq.Header.Add("Accept", webhookContentType)
	httpReq.Header.Add("Content-Type", webhookContentType)

	resp, err := t.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("dingtalk webhook error: post failed. %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("dingtalk webhook error: post failed. status:%d", resp.StatusCode)
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("dingtalk webhook error: read body failed. %w", err)
	}

	var rst webhookResult
	err = json.Unmarshal(data, &rst)
	if err != nil {
		return fmt.Errorf("dingtalk webhook error: unmarshal body failed. %w", err)
	}

	if rst.ErrCode != 0 {
		return fmt.Errorf("dingtalk webhook error: send msg failed. %v", rst.ErrMsg)
	}

	return nil
}

func getPostURL(urlPath, secret string) (string, error) {
	if secret == "" {
		return urlPath, nil
	}

	timestamp := strconv.FormatInt(time.Now().Unix()*1000, 10)
	sign, err := sign(secret, timestamp)
	if err != nil {
		return urlPath, err
	}

	query := url.Values{}
	query.Set("timestamp", timestamp)
	query.Set("sign", sign)

	return urlPath + "&" + query.Encode(), nil
}

func sign(secret, timestamp string) (string, error) {
	stringToSign := fmt.Sprintf("%s\n%s", timestamp, secret)
	h := hmac.New(sha256.New, []byte(secret))
	if _, err := io.WriteString(h, stringToSign); err != nil {
		return "", fmt.Errorf("sign failed. %w", err)
	}

	return base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
}
