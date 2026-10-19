/*
Copyright 2026 The Dapr Authors
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

package opensearch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonaws "github.com/dapr/components-contrib/common/aws"
	"github.com/dapr/components-contrib/common/aws/auth"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/kit/logger"
	kitmetadata "github.com/dapr/kit/metadata"
)

// Metadata uses the same credential chain, role assumption and IAM Roles
// Anywhere support as the other AWS components.
type Metadata struct {
	auth.Options `mapstructure:",squash"`
	Timeout      time.Duration `mapstructure:"timeout"`
}

type Client struct {
	config   aws.Config
	endpoint string
	http     *http.Client
	signer   *v4.Signer
	mu       sync.RWMutex
	closed   bool
}

func NewClient(ctx context.Context, properties map[string]string, log logger.Logger) (*Client, error) {
	md := Metadata{Timeout: 30 * time.Second}
	if err := kitmetadata.DecodeMetadata(properties, &md); err != nil {
		return nil, fmt.Errorf("opensearch metadata: %w", err)
	}
	endpoint, err := url.Parse(md.Endpoint)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") ||
		endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, errors.New("opensearch endpoint must be an HTTP(S) URL without credentials, query or fragment")
	}
	if md.Timeout <= 0 {
		return nil, errors.New("opensearch timeout must be positive")
	}
	md.Logger = log
	md.Properties = properties
	transport := http.DefaultTransport.(*http.Transport).Clone()
	httpClient := &http.Client{
		Transport: transport,
		Timeout:   md.Timeout,
		// A redirect cannot reuse an AWS signature and must not forward credentials.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	// The required endpoint is the domain data plane, not an STS endpoint.
	// Passing it as AWS BaseEndpoint would redirect AssumeRole to OpenSearch.
	authOptions := md.Options
	authOptions.Endpoint = ""
	config, err := commonaws.NewConfig(ctx, authOptions, commonaws.WithHTTPClient(httpClient))
	if err != nil {
		transport.CloseIdleConnections()
		return nil, fmt.Errorf("opensearch AWS configuration: %w", err)
	}
	if config.Region == "" {
		transport.CloseIdleConnections()
		return nil, errors.New("opensearch AWS region is required")
	}
	return &Client{
		config: config, endpoint: strings.TrimRight(endpoint.String(), "/"),
		http: httpClient, signer: v4.NewSigner(),
	}, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.http.CloseIdleConnections()
	return nil
}

func (c *Client) Do(ctx context.Context, method, path string, body, result any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "encode opensearch request: %v", err)
		}
	}
	return c.do(ctx, method, path, data, "application/json", result)
}

func (c *Client) do(ctx context.Context, method, path string, data []byte, contentType string, result any) error {
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return status.Error(codes.FailedPrecondition, "opensearch component is closed")
	}
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, bytes.NewReader(data))
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "create opensearch request: %v", err)
	}
	req.Header.Set("Content-Type", contentType)
	hash := sha256.Sum256(data)
	payloadHash := hex.EncodeToString(hash[:])
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	credentials, err := c.config.Credentials.Retrieve(ctx)
	if err != nil {
		return contextError(err, codes.Unauthenticated, "retrieve opensearch credentials")
	}
	if err = c.signer.SignHTTP(ctx, credentials, req, payloadHash, "es", c.config.Region, time.Now()); err != nil {
		return contextError(err, codes.Unauthenticated, "sign opensearch request")
	}
	// The component operator configures the endpoint; request paths are
	// validated index names and fixed API suffixes, never caller URLs.
	response, err := c.http.Do(req) //nolint:gosec // G704: operator-configured OpenSearch endpoint.
	if err != nil {
		return contextError(err, codes.Unavailable, "opensearch request")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		// Provider error reasons may contain caller document content. Return
		// the error type only, never echo raw response bodies.
		var envelope struct {
			Error json.RawMessage `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(response.Body, 16384)).Decode(&envelope)
		return providerError(response.StatusCode, envelope.Error)
	}
	if result == nil {
		_, err = io.Copy(io.Discard, response.Body)
	} else {
		err = json.NewDecoder(response.Body).Decode(result)
	}
	if err != nil {
		return contextError(err, codes.Internal, "decode opensearch response")
	}
	return nil
}

func contextError(err error, fallback codes.Code, operation string) error {
	switch {
	case errors.Is(err, context.Canceled):
		fallback = codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		fallback = codes.DeadlineExceeded
	}
	return status.Errorf(fallback, "%s: %v", operation, err)
}

func providerError(httpStatus int, raw json.RawMessage) error {
	var detail struct {
		Type string `json:"type"`
	}
	// Some proxies return an error string instead of an OpenSearch object.
	// The HTTP status remains authoritative when the body cannot be decoded.
	if len(raw) > 0 && raw[0] == '{' {
		_ = json.Unmarshal(raw, &detail)
	}
	code := codes.Internal
	switch httpStatus {
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		code = codes.InvalidArgument
	case http.StatusUnauthorized:
		code = codes.Unauthenticated
	case http.StatusForbidden:
		code = codes.PermissionDenied
	case http.StatusNotFound:
		code = codes.NotFound
	case http.StatusConflict:
		code = codes.AlreadyExists
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		code = codes.DeadlineExceeded
	case http.StatusRequestEntityTooLarge, http.StatusTooManyRequests:
		code = codes.ResourceExhausted
	case http.StatusBadGateway, http.StatusServiceUnavailable:
		code = codes.Unavailable
	}
	switch detail.Type {
	case "resource_already_exists_exception":
		code = codes.AlreadyExists
	case "index_not_found_exception", "document_missing_exception":
		code = codes.NotFound
	case "mapper_parsing_exception", "strict_dynamic_mapping_exception", "illegal_argument_exception":
		code = codes.InvalidArgument
	case "es_rejected_execution_exception":
		code = codes.ResourceExhausted
	}
	return status.Errorf(code, "opensearch returned HTTP %d (%s)", httpStatus, detail.Type)
}

var indexName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

func IndexPath(name string) (string, error) {
	if len(name) > 255 || !indexName.MatchString(name) {
		return "", status.Error(codes.InvalidArgument, "opensearch index must start with a lowercase letter or digit and contain only lowercase letters, digits, dots, underscores and hyphens (at most 255 bytes)")
	}
	return "/" + name, nil
}

func WithWriteContext(ctx context.Context, opts search.IndexingOptions) (context.Context, context.CancelFunc, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, status.FromContextError(err).Err()
	}
	if err := search.ValidateIndexingOptions(ctx, opts, false); err != nil {
		return nil, nil, err
	}
	if opts.Mode == search.IndexingModeWaitForCompletion {
		c, cancel := context.WithTimeout(ctx, opts.WaitTimeout)
		return c, cancel, nil
	}
	c, cancel := context.WithCancel(ctx)
	return c, cancel, nil
}

type Document struct {
	ID     string
	Source json.RawMessage
}

// Bulk waits for refresh even in return-on-acceptance mode: OpenSearch has
// no durable task acknowledgement, so every successful write is completed.
func (c *Client) Bulk(ctx context.Context, index string, docs []Document, deletes []string) ([]search.FailedItem, error) {
	path, err := IndexPath(index)
	if err != nil {
		return nil, err
	}
	if len(docs) != 0 && len(deletes) != 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot mix indexing and deletion in one bulk request")
	}
	ids := make([]string, 0, len(docs)+len(deletes))
	var data bytes.Buffer
	enc := json.NewEncoder(&data)
	for _, doc := range docs {
		ids = append(ids, doc.ID)
		if err = enc.Encode(map[string]any{"index": map[string]string{"_id": doc.ID}}); err != nil {
			return nil, status.Error(codes.InvalidArgument, "encode opensearch bulk action")
		}
		if err = enc.Encode(doc.Source); err != nil {
			return nil, status.Error(codes.InvalidArgument, "encode opensearch bulk document")
		}
	}
	for _, id := range deletes {
		if id == "" {
			return nil, status.Error(codes.InvalidArgument, "delete item has an empty id")
		}
		ids = append(ids, id)
		if err = enc.Encode(map[string]any{"delete": map[string]string{"_id": id}}); err != nil {
			return nil, status.Error(codes.InvalidArgument, "encode opensearch bulk deletion")
		}
	}
	if len(docs) > 0 {
		if err = search.ValidateWriteIDs(ids); err != nil {
			return nil, err
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	var result struct {
		Items []map[string]struct {
			ID     string          `json:"_id"`
			Status int             `json:"status"`
			Error  json.RawMessage `json:"error"`
		} `json:"items"`
	}
	// AWS OpenSearch ignores URL parameters on SigV4-signed POST requests.
	// PUT preserves refresh=wait_for and thus the completed/searchable boundary.
	if err = c.do(ctx, http.MethodPut, path+"/_bulk?refresh=wait_for", data.Bytes(), "application/x-ndjson", &result); err != nil {
		return nil, search.IndexingOutcomeUnknownError(status.Code(err), err.Error())
	}
	unknown := func() ([]search.FailedItem, error) {
		return nil, search.IndexingOutcomeUnknownError(codes.Internal, "incomplete or inconsistent opensearch bulk response")
	}
	if len(result.Items) != len(ids) {
		return unknown()
	}
	action := "index"
	if len(deletes) > 0 {
		action = "delete"
	}
	var failed []search.FailedItem
	for i, item := range result.Items {
		entry, ok := item[action]
		if !ok || len(item) != 1 || entry.ID != ids[i] || entry.Status < 200 || entry.Status > 599 {
			return unknown()
		}
		if action == "delete" && entry.Status == http.StatusNotFound && (len(entry.Error) == 0 || string(entry.Error) == "null") {
			continue
		}
		if entry.Status >= 300 {
			failed = append(failed, search.FailedItem{ID: entry.ID, Error: status.Convert(providerError(entry.Status, entry.Error))})
		} else if len(entry.Error) > 0 && string(entry.Error) != "null" {
			return unknown()
		}
	}
	return failed, nil
}

func (c *Client) GetDocuments(ctx context.Context, index string, ids []string) ([]Document, error) {
	path, err := IndexPath(index)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return []Document{}, nil
	}
	var result struct {
		Docs []struct {
			ID     string          `json:"_id"`
			Found  *bool           `json:"found"`
			Source json.RawMessage `json:"_source"`
			Error  json.RawMessage `json:"error"`
		} `json:"docs"`
	}
	if err = c.Do(ctx, http.MethodPost, path+"/_mget", map[string]any{"ids": ids}, &result); err != nil {
		return nil, err
	}
	if len(result.Docs) != len(ids) {
		return nil, status.Error(codes.Internal, "incomplete opensearch multi-get response")
	}
	docs := make([]Document, 0, len(ids))
	for i, doc := range result.Docs {
		if doc.ID != ids[i] {
			return nil, status.Error(codes.Internal, "out-of-order opensearch multi-get response")
		}
		if len(doc.Error) > 0 && string(doc.Error) != "null" {
			return nil, providerError(http.StatusInternalServerError, doc.Error)
		}
		if doc.Found == nil {
			return nil, status.Error(codes.Internal, "missing opensearch multi-get found flag")
		}
		if *doc.Found {
			if len(doc.Source) == 0 || string(doc.Source) == "null" {
				return nil, status.Error(codes.Internal, "missing opensearch document source")
			}
			docs = append(docs, Document{ID: doc.ID, Source: doc.Source})
		}
	}
	return docs, nil
}

func (c *Client) ListIndexes(ctx context.Context, kind string) ([]string, error) {
	var result map[string]struct {
		Mappings struct {
			Meta struct {
				Kind string `json:"dapr_kind"`
			} `json:"_meta"`
		} `json:"mappings"`
	}
	if err := c.Do(ctx, http.MethodGet, "/_mapping", nil, &result); err != nil {
		return nil, err
	}
	indexes := make([]string, 0, len(result))
	for name, index := range result {
		if index.Mappings.Meta.Kind == kind {
			indexes = append(indexes, name)
		}
	}
	sort.Strings(indexes)
	return indexes, nil
}
