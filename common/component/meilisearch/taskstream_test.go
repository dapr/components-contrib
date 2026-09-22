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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	meilisearchgo "github.com/meilisearch/meilisearch-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/search"
	kitlogger "github.com/dapr/kit/logger"
)

// taskServer is a Meilisearch test double exposing the task-change stream and
// the task status API used for reconciliation and polling.
type taskServer struct {
	server *httptest.Server

	mu          sync.Mutex
	taskStatus  map[int64]string
	streamError *streamFailure
	taskError   *streamFailure
	streamTypes string

	// dials counts every stream request, connections the accepted ones and
	// taskReads the task status reads.
	dials       atomic.Int32
	connections atomic.Int32
	taskReads   atomic.Int32
	// dropTaskReads makes that many task status reads fail without an HTTP
	// response.
	dropTaskReads atomic.Int32

	events chan string
}

type streamFailure struct {
	statusCode int
	body       string
}

var (
	featureNotEnabled = streamFailure{statusCode: http.StatusBadRequest, body: `{"message":"getting task changes requires enabling the tasks streaming route","code":"feature_not_enabled","type":"invalid_request"}`}
	routeMissing      = streamFailure{statusCode: http.StatusNotFound, body: `{"message":"not found","code":"not_found","type":"invalid_request"}`}
	invalidAPIKey     = streamFailure{statusCode: http.StatusForbidden, body: `{"message":"the provided API key is invalid","code":"invalid_api_key","type":"auth"}`}
)

func newTaskServer(t *testing.T, failure *streamFailure) *taskServer {
	t.Helper()
	ts := &taskServer{
		taskStatus:  map[int64]string{},
		events:      make(chan string, 8),
		streamError: failure,
	}
	ts.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == TasksStreamPath:
			ts.handleStream(w, r)
		case strings.HasPrefix(r.URL.Path, "/tasks/"):
			ts.handleTask(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(ts.server.Close)
	return ts
}

func (ts *taskServer) handleStream(w http.ResponseWriter, r *http.Request) {
	ts.dials.Add(1)
	ts.mu.Lock()
	failure := ts.streamError
	ts.streamTypes = r.URL.Query().Get("types")
	ts.mu.Unlock()
	if failure != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(failure.statusCode)
		_, _ = w.Write([]byte(failure.body))
		return
	}
	ts.connections.Add(1)
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flusher, ok := w.(http.Flusher)
	if !ok {
		return
	}
	flusher.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case event, open := <-ts.events:
			if !open {
				return
			}
			_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
			flusher.Flush()
		}
	}
}

func (ts *taskServer) handleTask(w http.ResponseWriter, r *http.Request) {
	ts.taskReads.Add(1)
	var uid int64
	if _, err := fmt.Sscanf(strings.TrimPrefix(r.URL.Path, "/tasks/"), "%d", &uid); err != nil {
		http.NotFound(w, r)
		return
	}
	if ts.dropTaskReads.Load() > 0 {
		ts.dropTaskReads.Add(-1)
		if hijacker, ok := w.(http.Hijacker); ok {
			if conn, _, err := hijacker.Hijack(); err == nil {
				_ = conn.Close()
				return
			}
		}
	}
	ts.mu.Lock()
	taskStatus, ok := ts.taskStatus[uid]
	failure := ts.taskError
	ts.mu.Unlock()
	if failure != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(failure.statusCode)
		_, _ = w.Write([]byte(failure.body))
		return
	}
	if !ok {
		taskStatus = "enqueued"
	}
	task := map[string]any{"uid": uid, "status": taskStatus, "type": "documentAdditionOrUpdate", "indexUid": "books"}
	if taskStatus == "failed" {
		task["error"] = map[string]any{"message": "invalid document", "code": "invalid_document_fields", "type": "invalid_request"}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(task)
}

func (ts *taskServer) setTaskStatus(uid int64, taskStatus string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.taskStatus[uid] = taskStatus
}

func (ts *taskServer) setStreamFailure(failure *streamFailure) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.streamError = failure
}

func (ts *taskServer) setTaskFailure(failure *streamFailure) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.taskError = failure
}

func (ts *taskServer) requestedTypes() string {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.streamTypes
}

func (ts *taskServer) emit(t *testing.T, change map[string]any) {
	t.Helper()
	encoded, err := json.Marshal(change)
	require.NoError(t, err)
	ts.events <- string(encoded)
}

func newTestDispatcher(t *testing.T, ts *taskServer) *TaskDispatcher {
	t.Helper()
	md := MeilisearchMetadata{Host: ts.server.URL}
	client, err := NewClient(md)
	require.NoError(t, err)
	dispatcher := NewTaskDispatcher(md, client, kitlogger.NewLogger("test"))
	dispatcher.InitialBackoff = 10 * time.Millisecond
	dispatcher.MaxBackoff = 50 * time.Millisecond
	dispatcher.LivenessTimeout = 200 * time.Millisecond
	dispatcher.PollInitialInterval = 10 * time.Millisecond
	dispatcher.PollMaxInterval = 50 * time.Millisecond
	t.Cleanup(func() { require.NoError(t, dispatcher.Close()) })
	return dispatcher
}

func TestEnsureStreamingFallsBackToPollingWhenTheStreamIsUnavailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		failure streamFailure
	}{
		{name: "feature not enabled", failure: featureNotEnabled},
		{name: "route missing", failure: routeMissing},
		{name: "bad request without a payload", failure: streamFailure{statusCode: http.StatusBadRequest, body: "nope"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			failure := tt.failure
			ts := newTaskServer(t, &failure)
			dispatcher := newTestDispatcher(t, ts)

			require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
			require.NoError(t, dispatcher.EnsureStreaming(t.Context()))

			assert.True(t, dispatcher.PollingOnly())
			assert.Equal(t, int32(1), ts.dials.Load(), "the outcome is remembered: the stream is dialed once")
		})
	}
}

func TestEnsureStreamingReportsOtherFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		failure  streamFailure
		wantCode codes.Code
	}{
		{name: "missing tasks.get permission", failure: invalidAPIKey, wantCode: codes.Unauthenticated},
		{
			name:     "provider failure",
			failure:  streamFailure{statusCode: http.StatusInternalServerError, body: `{"message":"boom","code":"internal","type":"internal"}`},
			wantCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			failure := tt.failure
			dispatcher := newTestDispatcher(t, newTaskServer(t, &failure))

			err := dispatcher.EnsureStreaming(t.Context())

			require.Error(t, err)
			assert.Equal(t, tt.wantCode, status.Code(err))
			assert.False(t, dispatcher.PollingOnly(), "a failure that is not a disabled route does not settle on polling")
		})
	}
}

func TestEnsureStreamingConnectsOnce(t *testing.T) {
	t.Parallel()

	ts := newTaskServer(t, nil)
	dispatcher := newTestDispatcher(t, ts)

	require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
	require.NoError(t, dispatcher.EnsureStreaming(t.Context()))

	assert.Equal(t, int32(1), ts.connections.Load())
	assert.False(t, dispatcher.PollingOnly())
	assert.ElementsMatch(t, []string{"documentAdditionOrUpdate", "documentDeletion"}, strings.Split(ts.requestedTypes(), ","),
		"the stream is filtered to the write and deletion task types")
}

func TestWaitReturnsTerminalChangeFromTheStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		taskStatus string
		wantCode   codes.Code
	}{
		{name: "succeeded", taskStatus: "succeeded", wantCode: codes.OK},
		{name: "failed", taskStatus: "failed", wantCode: codes.InvalidArgument},
		{name: "canceled", taskStatus: "canceled", wantCode: codes.Aborted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := newTaskServer(t, nil)
			dispatcher := newTestDispatcher(t, ts)
			require.NoError(t, dispatcher.EnsureStreaming(t.Context()))

			go func() {
				// The stream is the notification channel; the task status API
				// still reports the task as enqueued.
				time.Sleep(20 * time.Millisecond)
				ts.emit(t, map[string]any{
					"uid": 42, "status": tt.taskStatus, "type": "documentDeletion", "indexUid": "books",
					"error": map[string]any{"message": "invalid document", "code": "invalid_document_fields", "type": "invalid_request"},
				})
			}()

			change, err := dispatcher.Wait(t.Context(), 42, 5*time.Second)
			require.NoError(t, err)
			assert.Equal(t, int64(42), change.UID)
			require.True(t, change.Terminal())

			statusErr := change.TaskStatusError()
			if tt.wantCode == codes.OK {
				require.NoError(t, statusErr)
				return
			}
			require.Error(t, statusErr)
			assert.Equal(t, tt.wantCode, status.Code(statusErr))
		})
	}
}

func TestWaitUsesTheTerminalCacheForTheRegistrationRace(t *testing.T) {
	t.Parallel()

	ts := newTaskServer(t, nil)
	dispatcher := newTestDispatcher(t, ts)
	require.NoError(t, dispatcher.EnsureStreaming(t.Context()))

	// The terminal change arrives before any waiter is registered.
	ts.emit(t, map[string]any{"uid": 7, "status": "succeeded", "type": "documentAdditionOrUpdate"})
	require.Eventually(t, func() bool {
		dispatcher.mu.Lock()
		defer dispatcher.mu.Unlock()
		_, ok := dispatcher.cache[7]
		return ok
	}, time.Second, 5*time.Millisecond)

	change, err := dispatcher.Wait(t.Context(), 7, time.Second)

	require.NoError(t, err)
	assert.Equal(t, meilisearchgo.TaskStatusSucceeded, change.Status)
}

func TestWaitReconcilesWithTheTaskStatusAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// when the task becomes terminal in the task status API.
		terminalAfter time.Duration
		waitTimeout   time.Duration
	}{
		{
			name:          "reconciles right after registration",
			terminalAfter: 0,
			waitTimeout:   5 * time.Second,
		},
		{
			name:          "reconciles before applying the timeout action",
			terminalAfter: 30 * time.Millisecond,
			waitTimeout:   80 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := newTaskServer(t, nil)
			dispatcher := newTestDispatcher(t, ts)
			require.NoError(t, dispatcher.EnsureStreaming(t.Context()))

			if tt.terminalAfter == 0 {
				ts.setTaskStatus(11, "succeeded")
			} else {
				go func() {
					time.Sleep(tt.terminalAfter)
					ts.setTaskStatus(11, "succeeded")
				}()
			}

			// No stream event is ever emitted for this task.
			change, err := dispatcher.Wait(t.Context(), 11, tt.waitTimeout)

			require.NoError(t, err)
			assert.Equal(t, meilisearchgo.TaskStatusSucceeded, change.Status)
		})
	}
}

func TestWaitTimesOutWhenTheTaskStaysPending(t *testing.T) {
	t.Parallel()

	ts := newTaskServer(t, nil)
	dispatcher := newTestDispatcher(t, ts)
	require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
	ts.setTaskStatus(3, "processing")

	_, err := dispatcher.Wait(t.Context(), 3, 50*time.Millisecond)

	require.ErrorIs(t, err, ErrWaitTimeout)

	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	assert.Empty(t, dispatcher.waiters, "the waiter must be removed when the wait times out")
}

func TestWaitStopsOnRequestCancellation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cancel   bool
		wantCode codes.Code
	}{
		{name: "canceled request", cancel: true, wantCode: codes.Canceled},
		{name: "expired deadline", wantCode: codes.DeadlineExceeded},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := newTaskServer(t, nil)
			dispatcher := newTestDispatcher(t, ts)
			require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
			ts.setTaskStatus(5, "processing")

			var ctx context.Context
			var cancel context.CancelFunc
			if tt.cancel {
				ctx, cancel = context.WithCancel(t.Context())
				go func() {
					time.Sleep(20 * time.Millisecond)
					cancel()
				}()
			} else {
				ctx, cancel = context.WithTimeout(t.Context(), 20*time.Millisecond)
			}
			defer cancel()

			_, err := dispatcher.Wait(ctx, 5, time.Minute)

			require.Error(t, err)
			assert.Equal(t, tt.wantCode, status.Code(err))

			dispatcher.mu.Lock()
			defer dispatcher.mu.Unlock()
			assert.Empty(t, dispatcher.waiters, "the waiter must be removed when the request ends")
		})
	}
}

func TestWaitReconnectsAndReconcilesAfterTheStreamDrops(t *testing.T) {
	t.Parallel()

	ts := newTaskServer(t, nil)
	dispatcher := newTestDispatcher(t, ts)
	require.NoError(t, dispatcher.EnsureStreaming(t.Context()))

	go func() {
		// Drop the connection without emitting a change for the task, then
		// author the task terminal so reconciliation after the reconnect finds it.
		time.Sleep(20 * time.Millisecond)
		ts.setTaskStatus(9, "succeeded")
		dispatcher.mu.Lock()
		body := dispatcher.body
		dispatcher.mu.Unlock()
		if body != nil {
			_ = body.Close()
		}
	}()

	change, err := dispatcher.Wait(t.Context(), 9, 5*time.Second)

	require.NoError(t, err)
	assert.Equal(t, meilisearchgo.TaskStatusSucceeded, change.Status)
	assert.GreaterOrEqual(t, ts.connections.Load(), int32(1))
}

func TestWaitSwitchesToPollingWhenTheStreamDisappears(t *testing.T) {
	t.Parallel()

	ts := newTaskServer(t, nil)
	dispatcher := newTestDispatcher(t, ts)
	require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
	ts.setTaskStatus(13, "processing")

	go func() {
		// The route is disabled under the running stream: the reconnect is
		// refused, the dispatcher settles on polling and the waiter that was
		// registered on the stream finishes by polling.
		time.Sleep(20 * time.Millisecond)
		ts.setStreamFailure(&featureNotEnabled)
		dispatcher.mu.Lock()
		body := dispatcher.body
		dispatcher.mu.Unlock()
		if body != nil {
			_ = body.Close()
		}
		time.Sleep(60 * time.Millisecond)
		ts.setTaskStatus(13, "succeeded")
	}()

	change, err := dispatcher.Wait(t.Context(), 13, 5*time.Second)

	require.NoError(t, err)
	assert.Equal(t, meilisearchgo.TaskStatusSucceeded, change.Status)
	assert.True(t, dispatcher.PollingOnly())

	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	assert.Empty(t, dispatcher.waiters, "the stream waiter must be removed once the wait completes by polling")
	assert.False(t, dispatcher.running, "the stream loop stops once the route is unavailable")
}

func TestWaitPollsWhenTheStreamIsUnavailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		taskStatus string
		wantCode   codes.Code
	}{
		{name: "succeeded", taskStatus: "succeeded", wantCode: codes.OK},
		{name: "failed", taskStatus: "failed", wantCode: codes.InvalidArgument},
		{name: "canceled", taskStatus: "canceled", wantCode: codes.Aborted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := newTaskServer(t, &featureNotEnabled)
			dispatcher := newTestDispatcher(t, ts)
			require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
			ts.setTaskStatus(17, "processing")
			go func() {
				time.Sleep(30 * time.Millisecond)
				ts.setTaskStatus(17, tt.taskStatus)
			}()

			change, err := dispatcher.Wait(t.Context(), 17, 5*time.Second)

			require.NoError(t, err)
			assert.Equal(t, int64(17), change.UID)
			require.True(t, change.Terminal())
			assert.GreaterOrEqual(t, ts.taskReads.Load(), int32(2), "the task status is polled until it is terminal")
			assert.Equal(t, int32(1), ts.dials.Load(), "later waits go straight to polling")

			statusErr := change.TaskStatusError()
			if tt.wantCode == codes.OK {
				require.NoError(t, statusErr)
				return
			}
			require.Error(t, statusErr)
			assert.Equal(t, tt.wantCode, status.Code(statusErr))
		})
	}
}

func TestWaitPollingTimesOutWithExponentialBackoff(t *testing.T) {
	t.Parallel()

	ts := newTaskServer(t, &featureNotEnabled)
	dispatcher := newTestDispatcher(t, ts)
	require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
	ts.setTaskStatus(19, "processing")

	started := time.Now()
	_, err := dispatcher.Wait(t.Context(), 19, 200*time.Millisecond)

	require.ErrorIs(t, err, ErrWaitTimeout)
	assert.GreaterOrEqual(t, time.Since(started), 200*time.Millisecond, "the wait timeout is honoured")
	// The interval doubles from 10ms up to 50ms: a fixed 10ms interval would
	// read the status about 20 times in 200ms.
	reads := ts.taskReads.Load()
	assert.GreaterOrEqual(t, reads, int32(3), "the status is read repeatedly, including once right before the timeout action")
	assert.Less(t, reads, int32(15), "the polling interval backs off exponentially")
}

func TestWaitPollingStopsOnRequestCancellation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cancel   bool
		wantCode codes.Code
	}{
		{name: "canceled request", cancel: true, wantCode: codes.Canceled},
		{name: "expired deadline", wantCode: codes.DeadlineExceeded},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := newTaskServer(t, &featureNotEnabled)
			dispatcher := newTestDispatcher(t, ts)
			require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
			ts.setTaskStatus(23, "processing")

			var ctx context.Context
			var cancel context.CancelFunc
			if tt.cancel {
				ctx, cancel = context.WithCancel(t.Context())
				go func() {
					time.Sleep(20 * time.Millisecond)
					cancel()
				}()
			} else {
				ctx, cancel = context.WithTimeout(t.Context(), 20*time.Millisecond)
			}
			defer cancel()

			_, err := dispatcher.Wait(ctx, 23, time.Minute)

			require.Error(t, err)
			assert.Equal(t, tt.wantCode, status.Code(err))
		})
	}
}

func TestWaitPollingSurfacesTaskStatusFailures(t *testing.T) {
	t.Parallel()

	ts := newTaskServer(t, &featureNotEnabled)
	dispatcher := newTestDispatcher(t, ts)
	require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
	ts.setTaskFailure(&invalidAPIKey)

	_, err := dispatcher.Wait(t.Context(), 29, 5*time.Second)

	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.Equal(t, int32(1), ts.taskReads.Load(), "a rejected status read is not retried")
}

func TestWaitPollingRetriesReadsWithoutAResponse(t *testing.T) {
	t.Parallel()

	ts := newTaskServer(t, &featureNotEnabled)
	dispatcher := newTestDispatcher(t, ts)
	require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
	ts.setTaskStatus(31, "succeeded")
	ts.dropTaskReads.Store(2)

	change, err := dispatcher.Wait(t.Context(), 31, 5*time.Second)

	require.NoError(t, err)
	assert.Equal(t, meilisearchgo.TaskStatusSucceeded, change.Status)
	assert.GreaterOrEqual(t, ts.taskReads.Load(), int32(3), "reads that fail without a response are retried with backoff")
}

func TestWaitForWriteAppliesTheAcknowledgementSemantics(t *testing.T) {
	t.Parallel()

	mechanisms := []struct {
		name    string
		failure *streamFailure
	}{
		{name: "stream", failure: nil},
		{name: "polling", failure: &featureNotEnabled},
	}

	tests := []struct {
		name       string
		taskStatus string
		opts       search.IndexingOptions
		wantAck    search.IndexAck
		wantCode   codes.Code
	}{
		{
			name:       "a succeeded task completes the write",
			taskStatus: "succeeded",
			opts:       search.IndexingOptions{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: 5 * time.Second, OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest},
			wantAck:    search.IndexAckCompleted,
		},
		{
			name:       "a failed task fails the request",
			taskStatus: "failed",
			opts:       search.IndexingOptions{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: 5 * time.Second, OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest},
			wantCode:   codes.InvalidArgument,
		},
		{
			name:       "a canceled task is aborted",
			taskStatus: "canceled",
			opts:       search.IndexingOptions{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: 5 * time.Second, OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest},
			wantCode:   codes.Aborted,
		},
		{
			name:       "continue async queues a pending task",
			taskStatus: "processing",
			opts:       search.IndexingOptions{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: 50 * time.Millisecond, OnWaitTimeout: search.IndexingWaitTimeoutActionContinueAsync},
			wantAck:    search.IndexAckQueued,
		},
		{
			name:       "fail request exceeds the deadline",
			taskStatus: "processing",
			opts:       search.IndexingOptions{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: 50 * time.Millisecond, OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest},
			wantCode:   codes.DeadlineExceeded,
		},
	}

	for _, mechanism := range mechanisms {
		t.Run(mechanism.name, func(t *testing.T) {
			t.Parallel()

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()

					ts := newTaskServer(t, mechanism.failure)
					dispatcher := newTestDispatcher(t, ts)
					require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
					assert.Equal(t, mechanism.failure != nil, dispatcher.PollingOnly())
					ts.setTaskStatus(21, tt.taskStatus)

					ack, err := WaitForWrite(t.Context(), dispatcher, 21, tt.opts)

					if tt.wantCode != codes.OK {
						require.Error(t, err)
						assert.Equal(t, tt.wantCode, status.Code(err))
						assert.Equal(t, search.IndexAckUnspecified, ack)
						return
					}
					require.NoError(t, err)
					assert.Equal(t, tt.wantAck, ack)
				})
			}
		})
	}
}

func TestEnqueueWrite(t *testing.T) {
	t.Parallel()

	waitOpts := search.IndexingOptions{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: 5 * time.Second, OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest}
	rejected := &meilisearchgo.Error{StatusCode: http.StatusNotFound}
	rejected.MeilisearchApiError.Code = "index_not_found"
	rejected.MeilisearchApiError.Message = "index books not found"
	rejected.MeilisearchApiError.Type = "invalid_request"

	tests := []struct {
		name          string
		streamFailure *streamFailure
		taskStatus    string
		opts          search.IndexingOptions
		enqueueErr    error
		wantAck       search.IndexAck
		wantCode      codes.Code
		wantReason    string
		wantEnqueued  bool
	}{
		{
			name:         "return on acceptance is queued as soon as the task is accepted",
			taskStatus:   "processing",
			opts:         search.IndexingOptions{},
			wantAck:      search.IndexAckQueued,
			wantEnqueued: true,
		},
		{
			name:         "wait for completion waits for the task through the stream",
			taskStatus:   "succeeded",
			opts:         waitOpts,
			wantAck:      search.IndexAckCompleted,
			wantEnqueued: true,
		},
		{
			name:          "wait for completion waits for the task by polling",
			streamFailure: &featureNotEnabled,
			taskStatus:    "succeeded",
			opts:          waitOpts,
			wantAck:       search.IndexAckCompleted,
			wantEnqueued:  true,
		},
		{
			name:          "wait for completion fails before enqueueing when the credentials are rejected",
			streamFailure: &invalidAPIKey,
			opts:          waitOpts,
			wantCode:      codes.Unauthenticated,
		},
		{
			name:         "a rejected enqueue keeps its canonical code",
			opts:         search.IndexingOptions{},
			enqueueErr:   rejected,
			wantCode:     codes.NotFound,
			wantEnqueued: true,
		},
		{
			name:         "an enqueue without a response has an unknown outcome",
			opts:         search.IndexingOptions{},
			enqueueErr:   errors.New("connection reset by peer"),
			wantCode:     codes.Internal,
			wantReason:   search.ReasonIndexingOutcomeUnknown,
			wantEnqueued: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := newTaskServer(t, tt.streamFailure)
			dispatcher := newTestDispatcher(t, ts)
			ts.setTaskStatus(33, tt.taskStatus)

			var enqueued atomic.Int32
			ack, err := EnqueueWrite(t.Context(), dispatcher, tt.opts, "write to books", func(ctx context.Context) (*meilisearchgo.TaskInfo, error) {
				enqueued.Add(1)
				if tt.enqueueErr != nil {
					return nil, tt.enqueueErr
				}
				return &meilisearchgo.TaskInfo{TaskUID: 33, Status: meilisearchgo.TaskStatusEnqueued}, nil
			})

			assert.Equal(t, tt.wantEnqueued, enqueued.Load() == 1)
			if tt.wantCode != codes.OK {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, status.Code(err))
				assert.Equal(t, search.IndexAckUnspecified, ack)
				assert.Equal(t, tt.wantReason, errorReason(err))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantAck, ack)
			if tt.opts.Mode != search.IndexingModeWaitForCompletion {
				assert.Zero(t, ts.dials.Load(), "return on acceptance never touches the stream")
				assert.Zero(t, ts.taskReads.Load(), "return on acceptance never waits for the task")
			}
		})
	}
}

func errorReason(err error) string {
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return ""
}

func TestStreamPayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		line string
		want string
		ok   bool
	}{
		{name: "sse data line", line: `data: {"uid":1}`, want: `{"uid":1}`, ok: true},
		{name: "ndjson line", line: `{"uid":1}`, want: `{"uid":1}`, ok: true},
		{name: "keep-alive comment", line: ":", ok: false},
		{name: "blank line", line: "", ok: false},
		{name: "event name", line: "event: taskChange", ok: false},
		{name: "retry hint", line: "retry: 1000", ok: false},
		{name: "non object data", line: "data: hello", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			payload, ok := streamPayload(tt.line)

			assert.Equal(t, tt.ok, ok)
			if tt.ok {
				assert.JSONEq(t, tt.want, string(payload))
			}
		})
	}
}
