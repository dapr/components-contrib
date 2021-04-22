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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
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
	metadata   metadata
	httpClient *http.Client
}

type webhookResult struct {
	ErrCode int    `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
}

type outgoingWebhook struct {
	handler func(*bindings.ReadResponse) ([]byte, error)
}

var webhooks sync.Map //nolint:gochecknoglobals

func NewDingTalkWebhook(l logger.Logger) *DingTalkWebhook {
	return &DingTalkWebhook{ //nolint:exhaustivestruct
		logger:     l,
		httpClient: &http.Client{Timeout: defaultHTTPClientTimeout}, //nolint:exhaustivestruct
	}
}

// Init performs metadata parsing
func (t *DingTalkWebhook) Init(metadata bindings.Metadata) error {
	var err error
	t.metadata, err = parseMetadata(metadata)
	if err != nil {
		return err
	}

	return nil
}

func parseMetadata(meta bindings.Metadata) (metadata, error) {
	m := metadata{id: "", url: "", secret: ""}

	if val, ok := meta.Properties["id"]; ok && val != "" {
		m.id = val
	} else {
		return m, errors.New("webhook error: missing webhook id")
	}

	if val, ok := meta.Properties["url"]; ok && val != "" {
		m.url = val
	} else {
		return m, errors.New("webhook error: missing webhook url")
	}

	if val, ok := meta.Properties["secret"]; ok && val != "" {
		m.secret = val
	}

	return m, nil
}

// Read triggers the outgoing webhook, not yet production ready
func (t *DingTalkWebhook) Read(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	t.logger.Debugf("dingtalk webhook: start read input binding")
	item := &outgoingWebhook{handler: handler}

	_, loaded := webhooks.LoadOrStore(t.metadata.id, item)
	if loaded {
		return fmt.Errorf("dingtalk webhook error: duplicate id %s", t.metadata.id)
	}

	return nil
}

// Operations returns list of operations supported by dingtalk webhook binding
func (t *DingTalkWebhook) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation, bindings.GetOperation}
}

func (t *DingTalkWebhook) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	rst := &bindings.InvokeResponse{Metadata: map[string]string{}, Data: nil}
	switch req.Operation {
	case bindings.CreateOperation:
		return rst, t.sendMessage(req)
	case bindings.GetOperation:
		return rst, t.receivedMessage(req)
	case bindings.DeleteOperation, bindings.ListOperation:
		return rst, fmt.Errorf("dingtalk webhook error: unsupported operation %s", req.Operation)
	default:
		return rst, fmt.Errorf("dingtalk webhook error: unsupported operation %s", req.Operation)
	}
}

func (t *DingTalkWebhook) getOutgoingWebhook() (*outgoingWebhook, error) {
	routeItem, loaded := webhooks.Load(t.metadata.id)
	if !loaded {
		return nil, fmt.Errorf("dingtalk webhook error: invalid component metadata.id %s", t.metadata.id)
	}
	item, ok := routeItem.(*outgoingWebhook)
	if !ok {
		return nil, fmt.Errorf("type [outgoingWebhook] convert failed. ")
	}

	return item, nil
}

func (t *DingTalkWebhook) receivedMessage(req *bindings.InvokeRequest) error {
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

func (t *DingTalkWebhook) sendMessage(req *bindings.InvokeRequest) error {
	msg := req.Data

	postURL, err := getPostURL(t.metadata.url, t.metadata.secret)
	if err != nil {
		return fmt.Errorf("dingtalk webhook error: get url failed. %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultHTTPClientTimeout)
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
