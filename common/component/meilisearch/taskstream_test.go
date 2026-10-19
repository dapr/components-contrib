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

	// streamGate, when set, holds every stream request until it is closed.
	streamGate chan struct{}

	// dials counts every stream request, connections the accepted ones,
	// activeStreams the accepted ones that are still open and taskReads the
	// task status reads.
	dials         atomic.Int32
	connections   atomic.Int32
	activeStreams atomic.Int32
	taskReads     atomic.Int32
	// taskAnswers counts the task status reads whose answer has been decided:
	// a status set after that point is not seen by them.
	taskAnswers atomic.Int32
	// dropTaskReads makes that many task status reads fail without an HTTP
	// response.
	dropTaskReads atomic.Int32
	// slowTaskReads makes that many task status reads answer only after
	// taskReadDelay nanoseconds, or not at all if the client gives up first.
	slowTaskReads atomic.Int32
	taskReadDelay atomic.Int64

	events chan string
}

type streamFailure struct {
	statusCode int
	body       string
}

var (
	taskNotFoundError = streamFailure{statusCode: http.StatusNotFound, body: `{"message":"Task ` + "`29`" + ` not found.","code":"task_not_found","type":"invalid_request"}`}
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
	t.Cleanup(func() {
		// Closing the server waits for its handlers, and a stream handler only
		// returns when its client goes away: a dispatcher that leaked its
		// stream must fail its test, not hang the suite.
		ts.server.CloseClientConnections()
		ts.server.Close()
	})
	return ts
}

func (ts *taskServer) handleStream(w http.ResponseWriter, r *http.Request) {
	ts.dials.Add(1)
	ts.mu.Lock()
	failure := ts.streamError
	gate := ts.streamGate
	ts.streamTypes = r.URL.Query().Get("types")
	ts.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}
	}
	if failure != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(failure.statusCode)
		_, _ = w.Write([]byte(failure.body))
		return
	}
	ts.connections.Add(1)
	ts.activeStreams.Add(1)
	defer ts.activeStreams.Add(-1)
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
	if ts.slowTaskReads.Load() > 0 {
		ts.slowTaskReads.Add(-1)
		select {
		case <-time.After(time.Duration(ts.taskReadDelay.Load())):
		case <-r.Context().Done():
			return
		}
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
	ts.taskAnswers.Add(1)
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

// slowDownTaskReads delays the next count task status reads by delay.
func (ts *taskServer) slowDownTaskReads(count int32, delay time.Duration) {
	ts.taskReadDelay.Store(int64(delay))
	ts.slowTaskReads.Store(count)
}

// holdStreamRequests makes stream requests wait until the returned function is
// called.
func (ts *taskServer) holdStreamRequests() (release func()) {
	gate := make(chan struct{})
	ts.mu.Lock()
	ts.streamGate = gate
	ts.mu.Unlock()
	return sync.OnceFunc(func() { close(gate) })
}

// awaitTaskReads blocks until at least count task status reads were received.
func (ts *taskServer) awaitTaskReads(t *testing.T, count int32) {
	t.Helper()
	require.Eventually(t, func() bool { return ts.taskReads.Load() >= count }, 2*time.Second, time.Millisecond)
}

// awaitTaskAnswers blocks until the answer of at least count task status reads
// has been decided, so that a status set afterwards cannot reach them.
func (ts *taskServer) awaitTaskAnswers(t *testing.T, count int32) {
	t.Helper()
	require.Eventually(t, func() bool { return ts.taskAnswers.Load() >= count }, 2*time.Second, time.Millisecond)
}

// dropStream closes the dispatcher's current stream connection.
func dropStream(dispatcher *TaskDispatcher) {
	dispatcher.mu.Lock()
	body := dispatcher.body
	dispatcher.mu.Unlock()
	if body != nil {
		_ = body.Close()
	}
}

// waitWithin waits for a task with a wait timeout far beyond limit and a
// request deadline of limit. The reconciliation that precedes a timeout action
// can therefore never be what completes the wait: a test using it fails unless
// the recovery path it exercises delivers the terminal change.
func waitWithin(t *testing.T, dispatcher *TaskDispatcher, taskUID int64, limit time.Duration) (TaskChange, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), limit)
	defer cancel()
	return dispatcher.Wait(ctx, taskUID, time.Hour)
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
	// Periodic reconciliation is off by default so that each test exercises
	// exactly the recovery path it names.
	dispatcher.ReconcileInterval = 0
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

	// No stream event is ever emitted for the task in any of these cases.

	t.Run("reconciles right after registration", func(t *testing.T) {
		t.Parallel()

		ts := newTaskServer(t, nil)
		dispatcher := newTestDispatcher(t, ts)
		require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
		ts.setTaskStatus(11, "succeeded")

		change, err := waitWithin(t, dispatcher, 11, 3*time.Second)

		require.NoError(t, err)
		assert.Equal(t, meilisearchgo.TaskStatusSucceeded, change.Status)
		assert.Equal(t, int32(1), ts.taskReads.Load(), "the read that follows registration finds the task")
	})

	t.Run("reconciles before applying the timeout action", func(t *testing.T) {
		t.Parallel()

		ts := newTaskServer(t, nil)
		dispatcher := newTestDispatcher(t, ts)
		require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
		go func() {
			// The task becomes terminal after the read that follows
			// registration, so only the read at the timeout can find it.
			ts.awaitTaskAnswers(t, 1)
			ts.setTaskStatus(11, "succeeded")
		}()

		change, err := dispatcher.Wait(t.Context(), 11, 150*time.Millisecond)

		require.NoError(t, err)
		assert.Equal(t, meilisearchgo.TaskStatusSucceeded, change.Status)
		assert.Equal(t, int32(2), ts.taskReads.Load(), "the status is read after registration and at the timeout")
	})

	t.Run("reconciles periodically without reconnecting a silent stream", func(t *testing.T) {
		t.Parallel()

		ts := newTaskServer(t, nil)
		dispatcher := newTestDispatcher(t, ts)
		dispatcher.ReconcileInterval = 20 * time.Millisecond
		require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
		go func() {
			// The stream stays connected and silent across several intervals
			// before the task becomes terminal.
			ts.awaitTaskAnswers(t, 4)
			ts.setTaskStatus(11, "succeeded")
		}()

		change, err := waitWithin(t, dispatcher, 11, 3*time.Second)

		require.NoError(t, err)
		assert.Equal(t, meilisearchgo.TaskStatusSucceeded, change.Status)
		assert.Equal(t, int32(1), ts.connections.Load(), "a stream that is merely silent is not reconnected")
	})
}

func TestEnsureStreamingIsCanceledByClose(t *testing.T) {
	t.Parallel()

	ts := newTaskServer(t, nil)
	release := ts.holdStreamRequests()
	t.Cleanup(release)
	dispatcher := newTestDispatcher(t, ts)

	opening := make(chan error, 1)
	go func() { opening <- dispatcher.EnsureStreaming(t.Context()) }()
	require.Eventually(t, func() bool { return ts.dials.Load() == 1 }, 2*time.Second, time.Millisecond)
	waiting := make(chan error, 1)
	go func() { waiting <- dispatcher.EnsureStreaming(t.Context()) }()

	// The stalled dial is never answered: Close alone releases both requests.
	require.NoError(t, dispatcher.Close())
	for _, ch := range []chan error{opening, waiting} {
		select {
		case err := <-ch:
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		case <-time.After(time.Second):
			t.Fatal("EnsureStreaming did not return after Close")
		}
	}
	assert.Equal(t, int32(1), ts.dials.Load(), "the waiting request must not dial a closed dispatcher")
	assert.Equal(t, int32(0), ts.connections.Load())

	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	assert.False(t, dispatcher.running, "a closed dispatcher must not start its stream loop")
}

func TestEnsureStreamingWaitersHonourTheirOwnContext(t *testing.T) {
	t.Parallel()

	ts := newTaskServer(t, nil)
	release := ts.holdStreamRequests()
	t.Cleanup(release)
	dispatcher := newTestDispatcher(t, ts)

	opening := make(chan error, 1)
	go func() { opening <- dispatcher.EnsureStreaming(t.Context()) }()
	require.Eventually(t, func() bool { return ts.dials.Load() == 1 }, 2*time.Second, time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	waiting := make(chan error, 1)
	go func() { waiting <- dispatcher.EnsureStreaming(ctx) }()

	select {
	case err := <-waiting:
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	case <-time.After(time.Second):
		t.Fatal("a stalled dial held up another request beyond its deadline")
	}
	assert.Equal(t, int32(1), ts.dials.Load())

	release()
	require.NoError(t, <-opening)
}

func TestEnsureStreamingIsTakenOverWhenTheOpeningRequestEnds(t *testing.T) {
	t.Parallel()

	ts := newTaskServer(t, nil)
	release := ts.holdStreamRequests()
	t.Cleanup(release)
	dispatcher := newTestDispatcher(t, ts)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	opening := make(chan error, 1)
	go func() { opening <- dispatcher.EnsureStreaming(ctx) }()
	require.Eventually(t, func() bool { return ts.dials.Load() == 1 }, 2*time.Second, time.Millisecond)
	waiting := make(chan error, 1)
	go func() { waiting <- dispatcher.EnsureStreaming(t.Context()) }()

	cancel()
	assert.Equal(t, codes.Canceled, status.Code(<-opening))
	require.Eventually(t, func() bool { return ts.dials.Load() == 2 }, 2*time.Second, time.Millisecond,
		"the waiting request opens the stream itself")

	release()
	require.NoError(t, <-waiting)
	assert.Equal(t, int32(1), ts.connections.Load())
	assert.False(t, dispatcher.PollingOnly())
}

func TestNewTaskDispatcherKeepsTheDefaultTransportSettings(t *testing.T) {
	t.Parallel()

	timeout := 5 * time.Second
	dispatcher := NewTaskDispatcher(MeilisearchMetadata{Host: "http://localhost:7700", Timeout: &timeout}, nil, nil)
	transport, ok := dispatcher.httpClient.Transport.(*http.Transport)
	require.True(t, ok)

	defaults := http.DefaultTransport.(*http.Transport)
	assert.Equal(t, timeout, transport.ResponseHeaderTimeout)
	assert.NotNil(t, transport.Proxy, "the stream must honour the proxy settings like every other request")
	assert.NotNil(t, transport.DialContext)
	assert.Equal(t, defaults.TLSHandshakeTimeout, transport.TLSHandshakeTimeout)
}

func TestEnsureStreamingStopsWhenTheRequestEndsDuringTheDial(t *testing.T) {
	t.Parallel()

	ts := newTaskServer(t, nil)
	release := ts.holdStreamRequests()
	t.Cleanup(release)
	dispatcher := newTestDispatcher(t, ts)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	err := dispatcher.EnsureStreaming(ctx)

	require.Error(t, err)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err), "the request's own deadline is reported, not the stream's cancellation")

	// Nothing was settled: the next request opens the stream.
	release()
	require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
	assert.False(t, dispatcher.PollingOnly())
	require.Eventually(t, func() bool { return ts.activeStreams.Load() == 1 }, 2*time.Second, 5*time.Millisecond)
}

func TestStreamOutlivesTheRequestThatOpensIt(t *testing.T) {
	t.Parallel()

	ts := newTaskServer(t, nil)
	dispatcher := newTestDispatcher(t, ts)

	// The first wait-for-completion write opens the stream, then completes.
	ctx, cancel := context.WithCancel(t.Context())
	require.NoError(t, dispatcher.EnsureStreaming(ctx))
	cancel()

	assert.Never(t, func() bool { return ts.dials.Load() > 1 }, 150*time.Millisecond, 5*time.Millisecond,
		"the stream must not be reconnected when the request that opened it ends")

	go func() {
		ts.awaitTaskReads(t, 1)
		ts.emit(t, map[string]any{"uid": 53, "status": "succeeded", "type": "documentAdditionOrUpdate"})
	}()
	change, err := waitWithin(t, dispatcher, 53, 3*time.Second)

	require.NoError(t, err)
	assert.Equal(t, meilisearchgo.TaskStatusSucceeded, change.Status)
	assert.Equal(t, int32(1), ts.connections.Load())
}

func TestCloseReleasesRequestsThatAreStillWaiting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		failure *streamFailure
	}{
		{name: "stream waiter"},
		{name: "polling waiter", failure: &featureNotEnabled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := newTaskServer(t, tt.failure)
			dispatcher := newTestDispatcher(t, ts)
			require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
			ts.setTaskStatus(37, "processing")

			waited := make(chan error, 1)
			go func() {
				// No wait timeout and no request deadline: only Close can end
				// this wait.
				_, err := dispatcher.Wait(t.Context(), 37, 0)
				waited <- err
			}()
			ts.awaitTaskReads(t, 1)
			require.NoError(t, dispatcher.Close())

			select {
			case err := <-waited:
				require.Error(t, err)
				assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			case <-time.After(3 * time.Second):
				t.Fatal("Close did not release the waiting request")
			}
		})
	}
}

func TestWaitTimeoutCountsFromTheStartOfTheWait(t *testing.T) {
	t.Parallel()

	ts := newTaskServer(t, nil)
	dispatcher := newTestDispatcher(t, ts)
	require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
	ts.setTaskStatus(41, "processing")
	// The read that follows registration uses most of the wait timeout.
	ts.slowDownTaskReads(1, 300*time.Millisecond)

	started := time.Now()
	_, err := dispatcher.Wait(t.Context(), 41, 400*time.Millisecond)
	elapsed := time.Since(started)

	require.ErrorIs(t, err, ErrWaitTimeout)
	assert.GreaterOrEqual(t, elapsed, 400*time.Millisecond, "the wait timeout is honoured")
	assert.Less(t, elapsed, 600*time.Millisecond, "the time spent reconciling counts towards the wait timeout")
}

func TestWaitIsNotHeldByASlowStatusRead(t *testing.T) {
	t.Parallel()

	ts := newTaskServer(t, nil)
	dispatcher := newTestDispatcher(t, ts)
	dispatcher.StatusReadTimeout = 50 * time.Millisecond
	require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
	// The read that follows registration hangs far longer than the wait
	// should take; the terminal change arrives on the stream meanwhile.
	ts.slowDownTaskReads(1, 10*time.Second)
	go func() {
		ts.awaitTaskReads(t, 1)
		ts.emit(t, map[string]any{"uid": 43, "status": "succeeded", "type": "documentAdditionOrUpdate"})
	}()

	started := time.Now()
	change, err := dispatcher.Wait(t.Context(), 43, time.Hour)

	require.NoError(t, err)
	assert.Equal(t, meilisearchgo.TaskStatusSucceeded, change.Status)
	assert.Less(t, time.Since(started), 2*time.Second, "a status read is bounded so the stream event is not held back")
}

func TestTerminalCacheSweepsExpiredChanges(t *testing.T) {
	t.Parallel()

	succeeded := func(uid int64) TaskChange {
		return TaskChange{UID: uid, Status: meilisearchgo.TaskStatusSucceeded}
	}

	t.Run("expired changes are neither served nor kept", func(t *testing.T) {
		t.Parallel()

		dispatcher := newTestDispatcher(t, newTaskServer(t, nil))
		dispatcher.TerminalCacheTTL = 30 * time.Millisecond
		dispatcher.publish(succeeded(1))
		dispatcher.publish(succeeded(2))
		time.Sleep(50 * time.Millisecond)

		// An expired change is never handed to a waiter, swept or not.
		w, cached, _, _, err := dispatcher.register(1)
		require.NoError(t, err)
		assert.Nil(t, cached)
		dispatcher.unregister(1, w)

		// The first publish after the TTL removes what expired.
		dispatcher.publish(succeeded(3))

		dispatcher.mu.Lock()
		defer dispatcher.mu.Unlock()
		assert.Len(t, dispatcher.cache, 1)
		assert.Contains(t, dispatcher.cache, int64(3))
	})

	t.Run("the cache is swept at most once per TTL", func(t *testing.T) {
		t.Parallel()

		dispatcher := newTestDispatcher(t, newTaskServer(t, nil))
		dispatcher.TerminalCacheTTL = time.Hour
		dispatcher.publish(succeeded(1))
		dispatcher.mu.Lock()
		swept := dispatcher.cacheSwept
		dispatcher.mu.Unlock()
		require.False(t, swept.IsZero())

		for uid := int64(2); uid <= 50; uid++ {
			dispatcher.publish(succeeded(uid))
		}

		dispatcher.mu.Lock()
		defer dispatcher.mu.Unlock()
		assert.Equal(t, swept, dispatcher.cacheSwept, "publishing within the TTL does not walk the cache again")
		assert.Len(t, dispatcher.cache, 50)
	})
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
		// Once the read that follows registration has found the task pending,
		// make it terminal and drop the connection without emitting a change
		// for it: only the reconciliation after the reconnect can find it.
		ts.awaitTaskAnswers(t, 1)
		ts.setTaskStatus(9, "succeeded")
		dropStream(dispatcher)
	}()

	change, err := waitWithin(t, dispatcher, 9, 3*time.Second)

	require.NoError(t, err)
	assert.Equal(t, meilisearchgo.TaskStatusSucceeded, change.Status)
	assert.GreaterOrEqual(t, ts.connections.Load(), int32(2), "the dropped stream is reconnected")
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
		ts.awaitTaskAnswers(t, 1)
		ts.setStreamFailure(&featureNotEnabled)
		dropStream(dispatcher)
		// The task only becomes terminal once the waiter is polling.
		ts.awaitTaskAnswers(t, 3)
		ts.setTaskStatus(13, "succeeded")
	}()

	change, err := waitWithin(t, dispatcher, 13, 3*time.Second)

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

func TestWaitOnTheStreamEndsWhenTheTaskIsNotFound(t *testing.T) {
	t.Parallel()

	t.Run("right after registration", func(t *testing.T) {
		t.Parallel()

		ts := newTaskServer(t, nil)
		dispatcher := newTestDispatcher(t, ts)
		require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
		ts.setTaskFailure(&taskNotFoundError)

		_, err := waitWithin(t, dispatcher, 29, 3*time.Second)

		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
		assert.Equal(t, int32(1), ts.taskReads.Load())
	})

	t.Run("before applying the timeout action", func(t *testing.T) {
		t.Parallel()

		ts := newTaskServer(t, nil)
		dispatcher := newTestDispatcher(t, ts)
		require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
		go func() {
			ts.awaitTaskAnswers(t, 1)
			ts.setTaskFailure(&taskNotFoundError)
		}()

		_, err := dispatcher.Wait(t.Context(), 29, 100*time.Millisecond)

		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err), "a task that no longer exists is not reported as a timeout")
	})
}

func TestWaitOnTheStreamOutlastsARejectedStatusRead(t *testing.T) {
	t.Parallel()

	ts := newTaskServer(t, nil)
	dispatcher := newTestDispatcher(t, ts)
	require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
	ts.setTaskFailure(&invalidAPIKey)

	// The open stream still delivers the change after the read is rejected.
	go func() {
		ts.awaitTaskReads(t, 1)
		ts.emit(t, map[string]any{"uid": 29, "status": "succeeded", "type": "documentAdditionOrUpdate"})
	}()
	change, err := waitWithin(t, dispatcher, 29, 3*time.Second)

	require.NoError(t, err)
	assert.Equal(t, meilisearchgo.TaskStatusSucceeded, change.Status)
}

func TestWaitPollingRetriesTransientResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		failure streamFailure
	}{
		{name: "internal error", failure: streamFailure{statusCode: http.StatusInternalServerError, body: `{"message":"boom","code":"internal","type":"internal"}`}},
		{name: "too many requests", failure: streamFailure{statusCode: http.StatusTooManyRequests, body: `{"message":"slow down","code":"too_many_search_requests","type":"system"}`}},
	}

	for _, tt := range tests {
		t.Run(tt.name+" is retried until the task is observed", func(t *testing.T) {
			t.Parallel()

			ts := newTaskServer(t, &featureNotEnabled)
			dispatcher := newTestDispatcher(t, ts)
			require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
			// The queued task is unaffected by the failing reads and completes
			// normally.
			ts.setTaskStatus(47, "succeeded")
			failure := tt.failure
			ts.setTaskFailure(&failure)
			go func() {
				ts.awaitTaskReads(t, 3)
				ts.setTaskFailure(nil)
			}()

			change, err := waitWithin(t, dispatcher, 47, 3*time.Second)

			require.NoError(t, err)
			assert.Equal(t, meilisearchgo.TaskStatusSucceeded, change.Status)
		})

		t.Run(tt.name+" until the deadline is a wait timeout", func(t *testing.T) {
			t.Parallel()

			ts := newTaskServer(t, &featureNotEnabled)
			dispatcher := newTestDispatcher(t, ts)
			require.NoError(t, dispatcher.EnsureStreaming(t.Context()))
			failure := tt.failure
			ts.setTaskFailure(&failure)

			_, err := dispatcher.Wait(t.Context(), 47, 100*time.Millisecond)

			require.ErrorIs(t, err, ErrWaitTimeout, "the outcome of the queued task is unknown, not failed")
			assert.GreaterOrEqual(t, ts.taskReads.Load(), int32(2))
		})
	}
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
	rejected := &meilisearchgo.Error{StatusCode: http.StatusNotFound, ErrCode: meilisearchgo.MeilisearchApiError}
	rejected.MeilisearchApiError.Code = "index_not_found"
	rejected.MeilisearchApiError.Message = "index books not found"
	rejected.MeilisearchApiError.Type = "invalid_request"
	// The task was accepted, but its description could not be decoded.
	undecodable := &meilisearchgo.Error{StatusCode: http.StatusAccepted, ErrCode: meilisearchgo.ErrCodeResponseUnmarshalBody, OriginError: errors.New("unexpected end of JSON input")}
	// A gateway in front of Meilisearch answered, not Meilisearch.
	gateway := &meilisearchgo.Error{StatusCode: http.StatusGatewayTimeout, ErrCode: meilisearchgo.MeilisearchApiErrorWithoutMessage}

	tests := []struct {
		name          string
		streamFailure *streamFailure
		taskStatus    string
		opts          search.IndexingOptions
		enqueueErr    error
		canceled      bool
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
			name:         "a canceled enqueue keeps the context cancellation code and has an unknown outcome",
			opts:         search.IndexingOptions{},
			enqueueErr:   context.Canceled,
			canceled:     true,
			wantCode:     codes.Canceled,
			wantReason:   search.ReasonIndexingOutcomeUnknown,
			wantEnqueued: true,
		},
		{
			name:         "a rejection is reported even when the context has ended",
			opts:         search.IndexingOptions{},
			enqueueErr:   rejected,
			canceled:     true,
			wantCode:     codes.NotFound,
			wantEnqueued: true,
		},
		{
			name:         "an accepted enqueue whose response cannot be decoded has an unknown outcome",
			opts:         search.IndexingOptions{},
			enqueueErr:   undecodable,
			wantCode:     codes.Internal,
			wantReason:   search.ReasonIndexingOutcomeUnknown,
			wantEnqueued: true,
		},
		{
			name:         "an error from a gateway has an unknown outcome",
			opts:         search.IndexingOptions{},
			enqueueErr:   gateway,
			wantCode:     codes.Unavailable,
			wantReason:   search.ReasonIndexingOutcomeUnknown,
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
			ctx := t.Context()
			if tt.canceled {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			ack, err := EnqueueWrite(ctx, dispatcher, tt.opts, "write to books", func(ctx context.Context) (*meilisearchgo.TaskInfo, error) {
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
