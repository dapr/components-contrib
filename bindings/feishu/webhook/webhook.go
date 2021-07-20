// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
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

type FeishuWebhook struct {
	logger     logger.Logger
	settings   Settings
	httpClient *http.Client
}

type webhookResult struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

type outgoingWebhook struct {
	handler func(*bindings.ReadResponse) ([]byte, error)
}

var webhooks = struct { // nolint: gochecknoglobals
	sync.RWMutex
	m map[string]*outgoingWebhook
}{m: make(map[string]*outgoingWebhook)}

func NewFeishuWebhook(l logger.Logger) *FeishuWebhook {
	// See guidance on proper HTTP client settings here:
	// https://medium.com/@nate510/don-t-use-go-s-default-http-client-4804cb19f779
	dialer := &net.Dialer{ //nolint:exhaustivestruct
		Timeout: 5 * time.Second,
	}
	var netTransport = &http.Transport{ //nolint:exhaustivestruct
		DialContext:         dialer.DialContext,
		TLSHandshakeTimeout: 5 * time.Second,
	}
	httpClient := &http.Client{ //nolint:exhaustivestruct
		Timeout:   defaultHTTPClientTimeout,
		Transport: netTransport,
	}

	return &FeishuWebhook{ //nolint:exhaustivestruct
		logger:     l,
		httpClient: httpClient,
	}
}

// Init performs metadata parsing
func (t *FeishuWebhook) Init(metadata bindings.Metadata) error {
	var err error
	if err = t.settings.Decode(metadata.Properties); err != nil {
		return fmt.Errorf("feishu configuration error: %w", err)
	}
	if err = t.settings.Validate(); err != nil {
		return fmt.Errorf("feishu configuration error: %w", err)
	}

	return nil
}

// Read triggers the outgoing webhook, not yet production ready
func (t *FeishuWebhook) Read(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	t.logger.Debugf("Feishu webhook: start read input binding")

	webhooks.Lock()
	defer webhooks.Unlock()
	_, loaded := webhooks.m[t.settings.ID]
	if loaded {
		return fmt.Errorf("feishu webhook error: duplicate id %s", t.settings.ID)
	}
	webhooks.m[t.settings.ID] = &outgoingWebhook{handler: handler}

	return nil
}

// Operations returns list of operations supported by Feishu webhook binding
func (t *FeishuWebhook) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation, bindings.GetOperation}
}

func (t *FeishuWebhook) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	rst := &bindings.InvokeResponse{Metadata: map[string]string{}, Data: nil}
	switch req.Operation {
	case bindings.CreateOperation:
		return rst, t.sendMessage(req)
	case bindings.GetOperation:
		return rst, t.receivedMessage(req)
	case bindings.DeleteOperation, bindings.ListOperation:
		return rst, fmt.Errorf("feishu webhook error: unsupported operation %s", req.Operation)
	default:
		return rst, fmt.Errorf("feishu webhook error: unsupported operation %s", req.Operation)
	}
}

func (t *FeishuWebhook) getOutgoingWebhook() (*outgoingWebhook, error) {
	webhooks.RLock()
	defer webhooks.RUnlock()
	item, loaded := webhooks.m[t.settings.ID]
	if !loaded {
		return nil, fmt.Errorf("feishu webhook error: invalid component metadata.id %s", t.settings.ID)
	}

	return item, nil
}

func (t *FeishuWebhook) receivedMessage(req *bindings.InvokeRequest) error {
	item, err := t.getOutgoingWebhook()
	if err != nil {
		return err
	}

	in := &bindings.ReadResponse{Data: req.Data, Metadata: req.Metadata}
	if _, err = item.handler(in); err != nil {
		return err
	}

	return nil
}

func (t *FeishuWebhook) sendMessage(req *bindings.InvokeRequest) error {
	msg := req.Data

	// Sign the message
	var err error
	if t.settings.Secret != "" {
		msg, err = signMessage(msg, t.settings.Secret)
		if err != nil {
			return fmt.Errorf("feishu webhook error: sign message failed. %w", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultHTTPClientTimeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, "POST", t.settings.URL, bytes.NewReader(msg))
	if err != nil {
		return fmt.Errorf("feishu webhook error: new request failed. %w", err)
	}

	httpReq.Header.Add("Accept", webhookContentType)
	httpReq.Header.Add("Content-Type", webhookContentType)

	resp, err := t.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("feishu webhook error: post failed. %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("feishu webhook error: post failed. status:%d", resp.StatusCode)
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("feishu webhook error: read body failed. %w", err)
	}

	var rst webhookResult
	err = json.Unmarshal(data, &rst)
	if err != nil {
		return fmt.Errorf("feishu webhook error: unmarshal body failed. %w", err)
	}

	if rst.Code != 0 {
		return fmt.Errorf("feishu webhook error: send msg failed. %v", rst.Msg)
	}

	return nil
}

func signMessage(message []byte, secret string) (msg []byte, err error) {
	var m map[string]interface{}
	if err = json.Unmarshal(message, &m); err != nil {
		return
	}

	now := strconv.FormatInt(time.Now().Unix(), 10)
	var signature string
	signature, err = sign(secret, now)
	m["timestamp"] = now
	m["sign"] = signature
	msg, err = json.Marshal(m)
	return
}

func sign(secret, timestamp string) (string, error) {
	// Refer: https://open.feishu.cn/document/ukTMukTMukTM/ucTM5YjL3ETO24yNxkjN#348211be
	stringToSign := timestamp + "\n" + secret
	var data []byte
	h := hmac.New(sha256.New, []byte(stringToSign))
	_, err := h.Write(data)
	if err != nil {
		return "", err
	}
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))
	return signature, nil
}
