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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/search"
	"github.com/dapr/kit/logger"
)

func testClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := NewClient(t.Context(), map[string]string{
		"endpoint": server.URL, "region": "us-east-1",
		"accessKey": "test", "secretKey": "test", "sessionToken": "test-session",
	}, logger.NewLogger("opensearch-test"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client
}

func TestSigning(t *testing.T) {
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.Header.Get("Authorization"), "Credential=test/")
		assert.Contains(t, r.Header.Get("Authorization"), "/us-east-1/es/aws4_request")
		assert.Equal(t, "test-session", r.Header.Get("X-Amz-Security-Token"))
		data, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		hash := sha256.Sum256(data)
		assert.Equal(t, hex.EncodeToString(hash[:]), r.Header.Get("X-Amz-Content-Sha256"))
		assert.JSONEq(t, `{"value":"hello"}`, string(data))
		assert.Equal(t, "/test/_search", r.URL.Path)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	var result struct{ OK bool }
	require.NoError(t, client.Do(t.Context(), http.MethodPost, "/test/_search", map[string]string{"value": "hello"}, &result))
	assert.True(t, result.OK)
	require.NoError(t, client.Close())
	assert.Equal(t, codes.FailedPrecondition, status.Code(client.Do(t.Context(), http.MethodGet, "/", nil, nil)))
}

func TestAssumeRoleUsesSTSEndpoint(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "source-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "source-secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	var stsCalls atomic.Int32
	sts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stsCalls.Add(1)
		require.NoError(t, r.ParseForm())
		assert.Equal(t, "AssumeRole", r.Form.Get("Action"))
		assert.Equal(t, "arn:aws:iam::123456789012:role/test-role", r.Form.Get("RoleArn"))
		assert.Contains(t, r.Header.Get("Authorization"), "Credential=source-key/")
		w.Header().Set("Content-Type", "text/xml")
		_, _ = w.Write([]byte(`<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult><Credentials><AccessKeyId>role-key</AccessKeyId><SecretAccessKey>role-secret</SecretAccessKey><SessionToken>role-token</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials><AssumedRoleUser><Arn>arn:aws:sts::123456789012:assumed-role/test-role/test</Arn><AssumedRoleId>role:test</AssumedRoleId></AssumedRoleUser></AssumeRoleResult></AssumeRoleResponse>`))
	}))
	defer sts.Close()
	t.Setenv("AWS_ENDPOINT_URL_STS", sts.URL)
	data := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/test/_search", r.URL.Path)
		assert.Contains(t, r.Header.Get("Authorization"), "Credential=role-key/")
		assert.Contains(t, r.Header.Get("Authorization"), "/es/aws4_request")
		assert.Equal(t, "role-token", r.Header.Get("X-Amz-Security-Token"))
		_, _ = w.Write([]byte(`{}`))
	}))
	defer data.Close()
	client, err := NewClient(t.Context(), map[string]string{
		"endpoint": data.URL, "region": "us-east-1",
		"assumeRoleArn":         "arn:aws:iam::123456789012:role/test-role",
		"assumeRoleSessionName": "test",
	}, logger.NewLogger("test"))
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.Do(t.Context(), http.MethodPost, "/test/_search", map[string]any{}, nil))
	require.NoError(t, client.Do(t.Context(), http.MethodPost, "/test/_search", map[string]any{}, nil))
	assert.EqualValues(t, 1, stsCalls.Load(), "assumed credentials should be cached")
}

func TestDefaultCredentialChain(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "environment-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "environment-secret")
	t.Setenv("AWS_SESSION_TOKEN", "environment-token")
	t.Setenv("AWS_REGION", "eu-west-1")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.Header.Get("Authorization"), "Credential=environment-key/")
		assert.Contains(t, r.Header.Get("Authorization"), "/eu-west-1/es/aws4_request")
		assert.Equal(t, "environment-token", r.Header.Get("X-Amz-Security-Token"))
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	client, err := NewClient(t.Context(), map[string]string{"endpoint": server.URL}, logger.NewLogger("test"))
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.Do(t.Context(), http.MethodGet, "/_mapping", nil, nil))
}

func TestMetadataValidation(t *testing.T) {
	for _, endpoint := range []string{"", "localhost:9200", "ftp://localhost", "http://user:pass@localhost", "http://localhost?foo=bar", "http://localhost#fragment"} {
		t.Run(endpoint, func(t *testing.T) {
			_, err := NewClient(t.Context(), map[string]string{"endpoint": endpoint}, logger.NewLogger("test"))
			require.Error(t, err)
		})
	}
	for _, timeout := range []string{"-1s", "0s", "invalid"} {
		_, err := NewClient(t.Context(), map[string]string{"endpoint": "http://localhost", "timeout": timeout}, logger.NewLogger("test"))
		require.Error(t, err)
	}
	client, err := NewClient(t.Context(), map[string]string{
		"endpoint": "http://localhost:9200", "awsRegion": "us-west-2",
		"accessKey": "test", "secretKey": "test",
	}, logger.NewLogger("test"))
	require.NoError(t, err)
	defer client.Close()
	assert.Equal(t, "us-west-2", client.config.Region)
}

func TestIndexPath(t *testing.T) {
	for _, name := range []string{"", "*", "a,b", "../test", "a/b", "A", ".hidden", "_all", "-bad", "a?b", strings.Repeat("a", 256)} {
		_, err := IndexPath(name)
		assert.Equal(t, codes.InvalidArgument, status.Code(err), name)
	}
	path, err := IndexPath("test-index_1.2")
	require.NoError(t, err)
	assert.Equal(t, "/test-index_1.2", path)
}

func TestProviderErrors(t *testing.T) {
	for _, tc := range []struct {
		httpStatus int
		code       codes.Code
	}{
		{400, codes.InvalidArgument}, {401, codes.Unauthenticated}, {403, codes.PermissionDenied},
		{404, codes.NotFound}, {409, codes.AlreadyExists}, {429, codes.ResourceExhausted},
		{503, codes.Unavailable}, {504, codes.DeadlineExceeded},
	} {
		assert.Equal(t, tc.code, status.Code(providerError(tc.httpStatus, nil)))
	}
	assert.Equal(t, codes.AlreadyExists, status.Code(providerError(400, json.RawMessage(`{"type":"resource_already_exists_exception"}`))))
}

func TestCancellationAndRedirect(t *testing.T) {
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/elsewhere", http.StatusTemporaryRedirect)
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	assert.Equal(t, codes.Canceled, status.Code(client.Do(ctx, http.MethodGet, "/", nil, nil)))
	assert.Equal(t, codes.Internal, status.Code(client.Do(t.Context(), http.MethodGet, "/", nil, nil)))
}

func TestBulk(t *testing.T) {
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPut, r.Method)
		assert.Equal(t, "/test/_bulk", r.URL.Path)
		assert.Equal(t, "wait_for", r.URL.Query().Get("refresh"))
		assert.Equal(t, "application/x-ndjson", r.Header.Get("Content-Type"))
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Equal(t, "{\"index\":{\"_id\":\"one\"}}\n{\"value\":1}\n{\"index\":{\"_id\":\"two\"}}\n{\"value\":2}\n", string(body)) //nolint:testifylint // NDJSON is not a single JSON value.
		_, _ = w.Write([]byte(`{"items":[{"index":{"_id":"one","status":201}},{"index":{"_id":"two","status":400,"error":{"type":"mapper_parsing_exception","reason":"sensitive"}}}]}`))
	})
	failed, err := client.Bulk(t.Context(), "test", []Document{
		{ID: "one", Source: json.RawMessage(`{"value":1}`)},
		{ID: "two", Source: json.RawMessage(`{"value":2}`)},
	}, nil)
	require.NoError(t, err)
	require.Len(t, failed, 1)
	assert.Equal(t, "two", failed[0].ID)
	assert.Equal(t, codes.InvalidArgument, failed[0].Error.Code())
	assert.NotContains(t, failed[0].Error.Message(), "sensitive")
}

func TestBulkUnknownOutcome(t *testing.T) {
	for _, body := range []string{
		`{}`, `{"items":[]}`, `{"items":[{"index":{"_id":"wrong","status":201}}]}`,
		`{"items":[{"index":{"_id":"one","status":201,"error":{"type":"unexpected"}}}]}`,
		`not JSON`,
	} {
		t.Run(body, func(t *testing.T) {
			client := testClient(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) })
			_, err := client.Bulk(t.Context(), "test", []Document{{ID: "one", Source: json.RawMessage(`{}`)}}, nil)
			require.Error(t, err)
			st := status.Convert(err)
			require.Len(t, st.Details(), 1)
			detail, ok := st.Details()[0].(*errdetails.ErrorInfo)
			require.True(t, ok)
			assert.Equal(t, search.ReasonIndexingOutcomeUnknown, detail.GetReason())
		})
	}
}

func TestBulkRequestFailure(t *testing.T) {
	client := testClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":{"type":"unavailable","reason":"do not disclose document data"}}`))
	})
	_, err := client.Bulk(t.Context(), "test", []Document{{ID: "one", Source: json.RawMessage(`{}`)}}, nil)
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.NotContains(t, err.Error(), "do not disclose")
	details := status.Convert(err).Details()
	require.Len(t, details, 1)
	detail, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	assert.Equal(t, search.ReasonIndexingOutcomeUnknown, detail.GetReason())
}

func TestBulkValidationBeforeSubmission(t *testing.T) {
	client := testClient(t, func(http.ResponseWriter, *http.Request) {
		t.Error("invalid batch must not be submitted")
	})
	for _, docs := range [][]Document{
		{{ID: "", Source: json.RawMessage(`{}`)}},
		{{ID: "one", Source: json.RawMessage(`{}`)}, {ID: "one", Source: json.RawMessage(`{}`)}},
		{{ID: "one", Source: json.RawMessage(`invalid`)}},
	} {
		_, err := client.Bulk(t.Context(), "test", docs, nil)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	_, err := client.Bulk(t.Context(), "test", []Document{{ID: "one", Source: json.RawMessage(`{}`)}}, []string{"one"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = client.Bulk(t.Context(), "*", nil, nil)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	failed, err := client.Bulk(t.Context(), "test", nil, nil)
	require.NoError(t, err)
	assert.Empty(t, failed)
}

func TestDeleteMissingAndMultiGet(t *testing.T) {
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/test/_bulk":
			_, _ = w.Write([]byte(`{"items":[{"delete":{"_id":"missing","status":404,"result":"not_found"}}]}`))
		case "/test/_mget":
			_, _ = w.Write([]byte(`{"docs":[{"_id":"two","found":true,"_source":{"n":2}},{"_id":"missing","found":false},{"_id":"one","found":true,"_source":{"n":1}}]}`))
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	})
	failed, err := client.Bulk(t.Context(), "test", nil, []string{"missing"})
	require.NoError(t, err)
	assert.Empty(t, failed)
	docs, err := client.GetDocuments(t.Context(), "test", []string{"two", "missing", "one"})
	require.NoError(t, err)
	require.Len(t, docs, 2)
	assert.Equal(t, "two", docs[0].ID)
	assert.Equal(t, "one", docs[1].ID)
}

func TestDeleteRepeatedIDs(t *testing.T) {
	client := testClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"items":[{"delete":{"_id":"one","status":200}},{"delete":{"_id":"one","status":404,"result":"not_found"}}]}`))
	})
	failed, err := client.Bulk(t.Context(), "test", nil, []string{"one", "one"})
	require.NoError(t, err)
	assert.Empty(t, failed)
}

func TestListIndexes(t *testing.T) {
	client := testClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"z":{"mappings":{"_meta":{"dapr_kind":"search"}}},"a":{"mappings":{"_meta":{"dapr_kind":"search"}}},"v":{"mappings":{"_meta":{"dapr_kind":"vector"}}},"other":{"mappings":{}}}`))
	})
	names, err := client.ListIndexes(t.Context(), "search")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "z"}, names)
}

func TestMultiGetErrors(t *testing.T) {
	for _, tc := range []struct {
		body string
		code codes.Code
	}{
		{`{"docs":[{"_id":"one","error":{"type":"index_not_found_exception"}}]}`, codes.NotFound},
		{`{"docs":[{"_id":"one","found":true}]}`, codes.Internal},
		{`{"docs":[{"_id":"one"}]}`, codes.Internal},
		{`{"docs":[{"_id":"different","found":false}]}`, codes.Internal},
		{`{"docs":[]}`, codes.Internal},
	} {
		t.Run(tc.body, func(t *testing.T) {
			client := testClient(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(tc.body)) })
			_, err := client.GetDocuments(t.Context(), "test", []string{"one"})
			assert.Equal(t, tc.code, status.Code(err))
		})
	}
}

func TestWriteContext(t *testing.T) {
	ctx, cancel, err := WithWriteContext(t.Context(), search.IndexingOptions{})
	require.NoError(t, err)
	cancel()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	opts := search.IndexingOptions{
		Mode: search.IndexingModeWaitForCompletion, WaitTimeout: time.Second,
		OnWaitTimeout: search.IndexingWaitTimeoutActionContinueAsync,
	}
	_, _, err = WithWriteContext(t.Context(), opts)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	opts.OnWaitTimeout = search.IndexingWaitTimeoutActionFailRequest
	ctx, cancel, err = WithWriteContext(t.Context(), opts)
	require.NoError(t, err)
	defer cancel()
	_, ok := ctx.Deadline()
	assert.True(t, ok)
	canceled, cancelRequest := context.WithCancel(t.Context())
	cancelRequest()
	_, _, err = WithWriteContext(canceled, opts)
	assert.Equal(t, codes.Canceled, status.Code(err))
}
