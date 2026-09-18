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

package cfqueues

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	testAccountID = "testaccount"
	testQueueName = "testqueue"
	testQueueID   = "8ceb4b1b5b6f4f8fa54b8b0dd7c4a6d9"
)

// Fake Cloudflare worker and API, serving the endpoints the component uses.
type testServer struct {
	*httptest.Server
	queues   []map[string]string
	messages []pullMessage
	acks     []ackRequest
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()
	ts := &testServer{
		queues: []map[string]string{{"queue_id": testQueueID, "queue_name": testQueueName}},
	}

	mux := http.NewServeMux()
	// Worker info endpoint, used while initializing the component
	mux.HandleFunc("/.well-known/dapr/info", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"version": "20221219",
			"queues":  []string{testQueueName},
		})
	})
	mux.HandleFunc("/client/v4/accounts/"+testAccountID+"/queues", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result":      ts.queues,
			"result_info": map[string]int{"total_pages": 1},
		})
	})
	mux.HandleFunc("/client/v4/accounts/"+testAccountID+"/queues/"+testQueueID+"/messages/pull", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{"messages": ts.messages},
		})
	})
	mux.HandleFunc("/client/v4/accounts/"+testAccountID+"/queues/"+testQueueID+"/messages/ack", func(w http.ResponseWriter, r *http.Request) {
		ack := ackRequest{}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&ack))
		ts.acks = append(ts.acks, ack)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
	})

	ts.Server = httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	origBaseURL := cfAPIBaseURL
	cfAPIBaseURL = ts.URL + "/client/v4"
	t.Cleanup(func() { cfAPIBaseURL = origBaseURL })

	return ts
}

func testKey(t *testing.T) string {
	t.Helper()
	_, pk, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	der, err := x509.MarshalPKCS8PrivateKey(pk)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

func initComponent(t *testing.T, ts *testServer, extraProps map[string]string) *CFQueues {
	t.Helper()
	props := map[string]string{
		"workerUrl":   ts.URL,
		"workerName":  "testworker",
		"queueName":   testQueueName,
		"cfAccountID": testAccountID,
		"cfAPIToken":  "testtoken",
		"key":         testKey(t),
	}
	for k, v := range extraProps {
		props[k] = v
	}

	q := NewCFQueues(logger.NewLogger("test")).(*CFQueues)
	require.NoError(t, q.Init(t.Context(), bindings.Metadata{Base: contribMetadata.Base{Properties: props}}))
	t.Cleanup(func() { require.NoError(t, q.Close()) })
	return q
}

func TestPollOnceAcknowledgesAndRetries(t *testing.T) {
	ts := newTestServer(t)
	ts.messages = []pullMessage{
		{Body: json.RawMessage(`"hello"`), ID: "msg1", LeaseID: "lease1", Attempts: 1, TimestampMs: 1689615013586},
		{Body: json.RawMessage(`{"key":"value"}`), ID: "msg2", LeaseID: "lease2", Attempts: 2},
		{Body: json.RawMessage(`"fails"`), ID: "msg3", LeaseID: "lease3", Attempts: 1},
	}
	q := initComponent(t, ts, nil)

	received := []*bindings.ReadResponse{}
	handler := func(_ context.Context, res *bindings.ReadResponse) ([]byte, error) {
		received = append(received, res)
		if string(res.Data) == "fails" {
			return nil, errors.New("simulated failure")
		}
		return nil, nil
	}

	count, err := q.pollOnce(t.Context(), testQueueID, handler)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	require.Len(t, received, 3)
	// Bodies published as strings are unwrapped, structured bodies are passed through as JSON
	assert.Equal(t, "hello", string(received[0].Data))
	assert.JSONEq(t, `{"key":"value"}`, string(received[1].Data))
	assert.Equal(t, "msg1", received[0].Metadata["id"])
	assert.Equal(t, "1", received[0].Metadata["attempts"])
	assert.Equal(t, "1689615013586", received[0].Metadata["timestamp"])

	// Only the messages the app processed are acknowledged; the failed one is retried
	require.Len(t, ts.acks, 1)
	assert.Equal(t, []leaseRef{{LeaseID: "lease1"}, {LeaseID: "lease2"}}, ts.acks[0].Acks)
	assert.Equal(t, []leaseRef{{LeaseID: "lease3"}}, ts.acks[0].Retries)
}

func TestPollOnceWithEmptyQueue(t *testing.T) {
	ts := newTestServer(t)
	q := initComponent(t, ts, nil)

	count, err := q.pollOnce(t.Context(), testQueueID, func(context.Context, *bindings.ReadResponse) ([]byte, error) {
		t.Error("handler must not be invoked when the queue is empty")
		return nil, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 0, count)
	assert.Empty(t, ts.acks)
}

func TestResolveQueueID(t *testing.T) {
	t.Run("looked up by name", func(t *testing.T) {
		ts := newTestServer(t)
		ts.queues = append([]map[string]string{{"queue_id": "otherid", "queue_name": "otherqueue"}}, ts.queues...)
		q := initComponent(t, ts, nil)

		queueID, err := q.resolveQueueID(t.Context())
		require.NoError(t, err)
		assert.Equal(t, testQueueID, queueID)
	})

	t.Run("from metadata without a lookup", func(t *testing.T) {
		ts := newTestServer(t)
		ts.queues = nil
		q := initComponent(t, ts, map[string]string{"queueID": testQueueID})

		queueID, err := q.resolveQueueID(t.Context())
		require.NoError(t, err)
		assert.Equal(t, testQueueID, queueID)
	})

	t.Run("queue not in the account", func(t *testing.T) {
		ts := newTestServer(t)
		ts.queues = []map[string]string{{"queue_id": "otherid", "queue_name": "otherqueue"}}
		q := initComponent(t, ts, nil)

		_, err := q.resolveQueueID(t.Context())
		require.Error(t, err)
		assert.ErrorContains(t, err, "was not found")
	})
}

func TestReadDeliversMessages(t *testing.T) {
	ts := newTestServer(t)
	ts.messages = []pullMessage{{Body: json.RawMessage(`"hello"`), ID: "msg1", LeaseID: "lease1"}}
	q := initComponent(t, ts, map[string]string{"pollingInterval": "1s"})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	delivered := make(chan []byte, 1)
	require.NoError(t, q.Read(ctx, func(_ context.Context, res *bindings.ReadResponse) ([]byte, error) {
		select {
		case delivered <- res.Data:
		default:
		}
		return nil, nil
	}))

	select {
	case data := <-delivered:
		assert.Equal(t, "hello", string(data))
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a message")
	}
}

func TestReadRequiresAPIToken(t *testing.T) {
	ts := newTestServer(t)
	q := NewCFQueues(logger.NewLogger("test")).(*CFQueues)
	props := map[string]string{
		"workerUrl":  ts.URL,
		"workerName": "testworker",
		"queueName":  testQueueName,
		"key":        testKey(t),
	}
	require.NoError(t, q.Init(t.Context(), bindings.Metadata{Base: contribMetadata.Base{Properties: props}}))
	t.Cleanup(func() { require.NoError(t, q.Close()) })

	err := q.Read(t.Context(), func(context.Context, *bindings.ReadResponse) ([]byte, error) { return nil, nil })
	require.Error(t, err)
	assert.ErrorContains(t, err, "cfAPIToken")
}
