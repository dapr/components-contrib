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
	"encoding/base64"
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

// Returns a message as the pull API returns it when it was published with the "json" content type,
// which is the default (and what the output binding's worker uses).
func jsonMessage(t *testing.T, body any, id string, leaseID string) pullMessage {
	t.Helper()
	enc, err := json.Marshal(body)
	require.NoError(t, err)
	return pullMessage{
		Body:     base64.StdEncoding.EncodeToString(enc),
		Metadata: map[string]string{contentTypeMetadataKey: "json"},
		ID:       id,
		LeaseID:  leaseID,
	}
}

func TestPullMessageData(t *testing.T) {
	binary := []byte{0x00, 0xff, 0x10, 0x80}
	tests := []struct {
		name    string
		msg     pullMessage
		want    []byte
		wantErr string
	}{
		{
			name: "json string is unwrapped",
			msg:  pullMessage{Body: base64.StdEncoding.EncodeToString([]byte(`"hello"`)), Metadata: map[string]string{contentTypeMetadataKey: "json"}},
			want: []byte("hello"),
		},
		{
			name: "structured json is passed through",
			msg:  pullMessage{Body: base64.StdEncoding.EncodeToString([]byte(`{"key":"value"}`)), Metadata: map[string]string{contentTypeMetadataKey: "json"}},
			want: []byte(`{"key":"value"}`),
		},
		{
			name: "missing content type defaults to json",
			msg:  pullMessage{Body: base64.StdEncoding.EncodeToString([]byte(`"hello"`))},
			want: []byte("hello"),
		},
		{
			name: "bytes are decoded",
			msg:  pullMessage{Body: base64.StdEncoding.EncodeToString(binary), Metadata: map[string]string{contentTypeMetadataKey: "bytes"}},
			want: binary,
		},
		{
			name: "text is returned as-is",
			msg:  pullMessage{Body: `"quoted" text`, Metadata: map[string]string{contentTypeMetadataKey: "text"}},
			want: []byte(`"quoted" text`),
		},
		{
			name:    "invalid base64",
			msg:     pullMessage{Body: "not base64!", Metadata: map[string]string{contentTypeMetadataKey: "json"}},
			wantErr: "base64",
		},
		{
			name:    "v8 is not supported",
			msg:     pullMessage{Body: "AAAA", Metadata: map[string]string{contentTypeMetadataKey: "v8"}},
			wantErr: "unsupported message content type 'v8'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.msg.Data()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, data)
		})
	}
}

func TestPullResponseFormat(t *testing.T) {
	// Example message from the Cloudflare pull consumer documentation, with a string-valued body
	res := `{"body":"ImhlbGxvIg==","id":"1","timestamp_ms":1689615013586,"attempts":2,"metadata":{"CF-sourceMessageSource":"dash","CF-Content-Type":"json"},"lease_id":"lease"}`
	msg := pullMessage{}
	require.NoError(t, json.Unmarshal([]byte(res), &msg))
	data, err := msg.Data()
	require.NoError(t, err)
	assert.Equal(t, "hello", string(data))
	assert.Equal(t, 2, msg.Attempts)
	assert.Equal(t, "lease", msg.LeaseID)
}

func TestPollOnceAcknowledgesAndRetries(t *testing.T) {
	ts := newTestServer(t)
	msg1 := jsonMessage(t, "hello", "msg1", "lease1")
	msg1.Attempts = 1
	msg1.TimestampMs = 1689615013586
	msg2 := jsonMessage(t, map[string]string{"key": "value"}, "msg2", "lease2")
	msg2.Attempts = 2
	undecodable := pullMessage{Body: "AAAA", Metadata: map[string]string{contentTypeMetadataKey: "v8"}, ID: "msg4", LeaseID: "lease4"}
	ts.messages = []pullMessage{msg1, msg2, jsonMessage(t, "fails", "msg3", "lease3"), undecodable}
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
	assert.Equal(t, 4, count)

	// The message that can't be decoded is not delivered to the app
	require.Len(t, received, 3)
	// Bodies published as strings are unwrapped, structured bodies are passed through as JSON
	assert.Equal(t, "hello", string(received[0].Data))
	assert.JSONEq(t, `{"key":"value"}`, string(received[1].Data))
	assert.Equal(t, "msg1", received[0].Metadata["id"])
	assert.Equal(t, "1", received[0].Metadata["attempts"])
	assert.Equal(t, "1689615013586", received[0].Metadata["timestamp"])

	// Only the messages the app processed are acknowledged; the failed and undecodable ones are retried
	require.Len(t, ts.acks, 1)
	assert.Equal(t, []leaseRef{{LeaseID: "lease1"}, {LeaseID: "lease2"}}, ts.acks[0].Acks)
	assert.Equal(t, []leaseRef{{LeaseID: "lease3"}, {LeaseID: "lease4"}}, ts.acks[0].Retries)
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
	ts.messages = []pullMessage{jsonMessage(t, "hello", "msg1", "lease1")}
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
