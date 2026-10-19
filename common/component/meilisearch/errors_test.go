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

package meilisearch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStatusErrorClassifiesRequestsWithoutAResponse(t *testing.T) {
	t.Parallel()

	// The provider never answers, so only the request context ends a call.
	release := make(chan struct{})
	silent := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() {
		close(release)
		silent.Close()
	})
	unreachable := httptest.NewServer(http.NotFoundHandler())
	unreachable.Close()

	tests := []struct {
		name     string
		host     string
		ctx      func(t *testing.T) (context.Context, context.CancelFunc)
		wantCode codes.Code
	}{
		{
			name: "a request canceled by the caller is CANCELED",
			host: silent.URL,
			ctx: func(t *testing.T) (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(t.Context())
				time.AfterFunc(20*time.Millisecond, cancel)
				return ctx, cancel
			},
			wantCode: codes.Canceled,
		},
		{
			name: "a request that outlives its deadline is DEADLINE_EXCEEDED",
			host: silent.URL,
			ctx: func(t *testing.T) (context.Context, context.CancelFunc) {
				return context.WithTimeout(t.Context(), 20*time.Millisecond)
			},
			wantCode: codes.DeadlineExceeded,
		},
		{
			name: "an unreachable provider is UNAVAILABLE",
			host: unreachable.URL,
			ctx: func(t *testing.T) (context.Context, context.CancelFunc) {
				return context.WithCancel(t.Context())
			},
			wantCode: codes.Unavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, err := NewClient(MeilisearchMetadata{Host: tt.host})
			require.NoError(t, err)
			ctx, cancel := tt.ctx(t)
			defer cancel()

			_, err = client.GetIndexWithContext(ctx, "books")

			require.Error(t, err)
			assert.False(t, Rejected(err))
			assert.Equal(t, tt.wantCode, status.Code(StatusError(err, "get meilisearch index")))
		})
	}
}

func TestStatusErrorClassifiesResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		status       int
		body         string
		wantCode     codes.Code
		wantMessage  string
		wantRejected bool
	}{
		{
			name:         "a Meilisearch error is a rejection",
			status:       http.StatusNotFound,
			body:         `{"message":"Index ` + "`books`" + ` not found.","code":"index_not_found","type":"invalid_request","link":""}`,
			wantCode:     codes.NotFound,
			wantMessage:  "add documents: Index `books` not found.",
			wantRejected: true,
		},
		{
			name:        "an accepted request whose response cannot be decoded is not a rejection",
			status:      http.StatusAccepted,
			body:        `{"taskUid":`,
			wantCode:    codes.Internal,
			wantMessage: "add documents: decode the meilisearch response (HTTP 202)",
		},
		{
			name:        "an error page from a gateway is not a rejection",
			status:      http.StatusRequestEntityTooLarge,
			body:        `<html>413 Request Entity Too Large</html>`,
			wantCode:    codes.ResourceExhausted,
			wantMessage: "add documents: meilisearch returned HTTP 413",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			t.Cleanup(server.Close)
			client, err := NewClient(MeilisearchMetadata{Host: server.URL})
			require.NoError(t, err)

			_, err = client.Index("books").AddDocumentsWithContext(t.Context(), []map[string]any{{"id": "1"}}, nil)

			require.Error(t, err)
			assert.Equal(t, tt.wantRejected, Rejected(err))
			statusErr := StatusError(err, "add documents")
			assert.Equal(t, tt.wantCode, status.Code(statusErr))
			assert.Contains(t, status.Convert(statusErr).Message(), tt.wantMessage)
		})
	}
}
