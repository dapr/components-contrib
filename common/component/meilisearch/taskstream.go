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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	meilisearchgo "github.com/meilisearch/meilisearch-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/search"
	"github.com/dapr/kit/logger"
)

// The task dispatcher implements the wait-for-completion mechanisms described
// in the Search and Vector building blocks proposal.
//
// Task status polling is the guaranteed path: it works against every
// supported Meilisearch version. When the experimental `tasksStreamingRoute`
// feature is enabled, one long-lived `GET /tasks/stream` connection is shared
// by every request of an initialized component as an optimisation. Requests
// never open their own stream: they register a waiter keyed by task ID and
// are woken by the dispatcher.
//
// The stream is a notification channel, not the source of truth. Waiters
// reconcile their task with the task status API after registration, after a
// reconnect and immediately before applying a wait-timeout action, so a
// dropped or missed event can never produce a false timeout. A time-bounded
// cache of terminal changes covers the race between the enqueue returning a
// task ID and the waiter being registered.
//
// When Meilisearch reports that the stream route is unavailable, the
// dispatcher falls back to polling for that and every subsequent wait. The
// choice is invisible to callers: both mechanisms honour the same
// IndexingOptions semantics.
const (
	// TasksStreamPath is the experimental Meilisearch task-change stream. It
	// requires the `tasksStreamingRoute` experimental feature and an API key
	// with the `tasks.get` permission.
	TasksStreamPath = "/tasks/stream"

	defaultStreamInitialBackoff = 100 * time.Millisecond
	defaultStreamMaxBackoff     = 5 * time.Second
	// defaultStreamLiveness forces a reconnect and reconciliation when a
	// connection carrying waiters goes silent or half-open.
	defaultStreamLiveness = 30 * time.Second
	// defaultTerminalCacheTTL bounds how long a terminal change is kept for a
	// waiter that has not registered yet.
	defaultTerminalCacheTTL = time.Minute
	// defaultPollInitialInterval is the first task status polling interval;
	// it grows exponentially up to defaultPollMaxInterval.
	defaultPollInitialInterval = 50 * time.Millisecond
	defaultPollMaxInterval     = time.Second
)

// ErrWaitTimeout is returned by TaskDispatcher.Wait when the requested wait
// timeout expires and reconciliation confirms the task is still not terminal.
var ErrWaitTimeout = errors.New("timed out waiting for a terminal meilisearch task change")

// streamUnavailableError reports that Meilisearch does not serve the
// task-change stream: the experimental feature is disabled or the route does
// not exist in this version. It is never returned to callers; the dispatcher
// falls back to polling instead.
type streamUnavailableError struct {
	reason string
}

func (e *streamUnavailableError) Error() string {
	return "the meilisearch task-change stream is unavailable: " + e.reason
}

// TaskChange is a single task change delivered by the task-change stream or
// read back from the task status API.
type TaskChange struct {
	UID      int64                    `json:"uid"`
	Status   meilisearchgo.TaskStatus `json:"status"`
	Type     meilisearchgo.TaskType   `json:"type"`
	IndexUID string                   `json:"indexUid"`
	Error    APIError                 `json:"error"`
}

// Terminal reports whether the task has reached a final status.
func (c TaskChange) Terminal() bool {
	switch c.Status {
	case meilisearchgo.TaskStatusSucceeded, meilisearchgo.TaskStatusFailed, meilisearchgo.TaskStatusCanceled:
		return true
	default:
		return false
	}
}

// TaskStatusError maps a terminal, non-successful task to a gRPC status error.
// A canceled provider task is ABORTED: CANCELLED is reserved for cancellation
// of the Dapr RPC itself.
func (c TaskChange) TaskStatusError() error {
	switch c.Status {
	case meilisearchgo.TaskStatusSucceeded:
		return nil
	case meilisearchgo.TaskStatusCanceled:
		return status.Errorf(codes.Aborted, "meilisearch task %d was canceled", c.UID)
	case meilisearchgo.TaskStatusFailed:
		return TaskError(c.UID, c.Error)
	default:
		return status.Errorf(codes.Internal, "meilisearch task %d is not terminal (%s)", c.UID, c.Status)
	}
}

type cachedChange struct {
	change TaskChange
	at     time.Time
}

type waiter struct {
	ch chan TaskChange
}

// TaskDispatcher owns the shared task-change stream of one initialized
// component, dispatches terminal changes to the requests waiting for them and
// polls the task status API when the stream is unavailable.
type TaskDispatcher struct {
	host       string
	apiKey     string
	httpClient *http.Client
	tasks      meilisearchgo.TaskReader
	log        logger.Logger

	// Tunables, exposed for tests.
	InitialBackoff      time.Duration
	MaxBackoff          time.Duration
	LivenessTimeout     time.Duration
	TerminalCacheTTL    time.Duration
	PollInitialInterval time.Duration
	PollMaxInterval     time.Duration

	startMu sync.Mutex

	mu           sync.Mutex
	waiters      map[int64]map[*waiter]struct{}
	cache        map[int64]cachedChange
	reconnected  chan struct{}
	running      bool
	closed       bool
	lastActivity time.Time
	// pollOnly is set once the stream is known to be unavailable; pollOnlyCh
	// is closed at the same time so stream waiters switch to polling.
	pollOnly   bool
	pollOnlyCh chan struct{}

	cancel context.CancelFunc
	done   chan struct{}
	body   io.Closer
}

// NewTaskDispatcher creates the task dispatcher of a component. The dispatcher
// shares the component's host and API key and uses tasks for the task status
// reads that reconcile the stream and back the polling fallback.
func NewTaskDispatcher(md MeilisearchMetadata, tasks meilisearchgo.TaskReader, log logger.Logger) *TaskDispatcher {
	client := &http.Client{}
	if md.Timeout != nil {
		// The stream is long-lived, so only the connection setup is bounded.
		client.Transport = &http.Transport{ResponseHeaderTimeout: *md.Timeout}
	}
	return &TaskDispatcher{
		host:                strings.TrimRight(md.Host, "/"),
		apiKey:              md.APIKey,
		httpClient:          client,
		tasks:               tasks,
		log:                 log,
		InitialBackoff:      defaultStreamInitialBackoff,
		MaxBackoff:          defaultStreamMaxBackoff,
		LivenessTimeout:     defaultStreamLiveness,
		TerminalCacheTTL:    defaultTerminalCacheTTL,
		PollInitialInterval: defaultPollInitialInterval,
		PollMaxInterval:     defaultPollMaxInterval,
		waiters:             map[int64]map[*waiter]struct{}{},
		cache:               map[int64]cachedChange{},
		reconnected:         make(chan struct{}),
		pollOnlyCh:          make(chan struct{}),
	}
}

// PollingOnly reports whether the dispatcher has fallen back to polling the
// task status API because the task-change stream is unavailable.
func (d *TaskDispatcher) PollingOnly() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.pollOnly
}

// EnsureStreaming makes sure the shared task-change stream is connected, or
// that the dispatcher has settled on polling. It is called before a
// wait-for-completion write enqueues anything so that a credential problem is
// reported before records are written. An unavailable stream (disabled
// experimental feature or missing route) is not an error: the dispatcher
// remembers it and serves that and every later wait by polling.
func (d *TaskDispatcher) EnsureStreaming(ctx context.Context) error {
	d.startMu.Lock()
	defer d.startMu.Unlock()

	d.mu.Lock()
	closed := d.closed
	settled := d.running || d.pollOnly
	d.mu.Unlock()
	if closed {
		return status.Error(codes.FailedPrecondition, "the meilisearch component is closed")
	}
	if settled {
		return nil
	}

	resp, err := d.dial(ctx)
	if err != nil {
		var unavailable *streamUnavailableError
		if errors.As(err, &unavailable) {
			d.switchToPolling(unavailable.reason)
			return nil
		}
		return err
	}
	defer func() {
		if resp != nil {
			_ = resp.Body.Close()
		}
	}()

	streamCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	d.mu.Lock()
	d.running = true
	d.cancel = cancel
	d.done = make(chan struct{})
	d.lastActivity = time.Now()
	done := d.done
	d.mu.Unlock()

	go d.run(streamCtx, resp, done)
	resp = nil
	return nil
}

// Close stops the shared stream and releases its connection.
func (d *TaskDispatcher) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	cancel := d.cancel
	body := d.body
	done := d.done
	d.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if body != nil {
		_ = body.Close()
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	}
	return nil
}

// Wait blocks until the task reaches a terminal status, the wait timeout
// expires or the request context ends. A zero timeout waits indefinitely.
//
// When the stream is available the wait registers a waiter and is woken by
// the dispatcher; otherwise it polls the task status API with exponential
// backoff. A wait that times out returns ErrWaitTimeout after one final
// reconciliation; the caller then applies the requested wait-timeout action.
// Cancellation of ctx always wins over the wait timeout and removes the
// waiter; provider-side work may continue.
func (d *TaskDispatcher) Wait(ctx context.Context, taskUID int64, timeout time.Duration) (TaskChange, error) {
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	if d.PollingOnly() {
		return d.poll(ctx, taskUID, deadline)
	}

	w, cached, reconnected, pollOnly, err := d.register(taskUID)
	if err != nil {
		return TaskChange{}, err
	}
	defer d.unregister(taskUID, w)
	if cached != nil {
		return *cached, nil
	}

	// Reconciliation after registration: the task may already be terminal,
	// or its change may have been delivered while the stream was down.
	if change, ok := d.reconcile(ctx, taskUID); ok {
		return change, nil
	}

	var timerC <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		timerC = timer.C
	}

	for {
		select {
		case change := <-w.ch:
			return change, nil
		case <-ctx.Done():
			return TaskChange{}, ContextStatusError(ctx)
		case <-pollOnly:
			// The stream went away for good while this request was waiting
			// on it; the remaining wait is served by polling.
			return d.poll(ctx, taskUID, deadline)
		case <-reconnected:
			// Reconciliation after a reconnect: changes emitted while the
			// connection was down are not replayed.
			if change, ok := d.reconcile(ctx, taskUID); ok {
				return change, nil
			}
			reconnected = d.reconnectedChan()
		case <-timerC:
			// Reconciliation immediately before the timeout action, so a
			// missed stream event cannot produce a false timeout.
			if change, ok := d.reconcile(ctx, taskUID); ok {
				return change, nil
			}
			return TaskChange{}, ErrWaitTimeout
		}
	}
}

// poll reads the task status with exponential backoff until the task is
// terminal, the deadline passes or ctx ends. A zero deadline polls until the
// task is terminal. The last read before the deadline is the reconciliation
// performed immediately before the timeout action.
func (d *TaskDispatcher) poll(ctx context.Context, taskUID int64, deadline time.Time) (TaskChange, error) {
	if d.tasks == nil {
		return TaskChange{}, status.Error(codes.Internal, "the meilisearch task status reader is not configured")
	}
	interval := d.PollInitialInterval
	if interval <= 0 {
		interval = defaultPollInitialInterval
	}
	for {
		change, err := d.taskStatus(ctx, taskUID)
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return TaskChange{}, ContextStatusError(ctx)
			}
			if ResponseReceived(err) {
				// Meilisearch answered: the task cannot be observed with these
				// credentials or does not exist. Retrying would not help.
				return TaskChange{}, StatusError(err, fmt.Sprintf("read the status of meilisearch task %d", taskUID))
			}
			if d.log != nil {
				d.log.Debugf("meilisearch: polling task %d failed, retrying: %v", taskUID, err)
			}
		case change.Terminal():
			return change, nil
		}

		wait := jitter(interval)
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return TaskChange{}, ErrWaitTimeout
			}
			if wait > remaining {
				wait = remaining
			}
		}
		if !sleepCtx(ctx, wait) {
			return TaskChange{}, ContextStatusError(ctx)
		}
		interval = nextBackoff(interval, d.PollMaxInterval)
	}
}

// WaitForWrite waits for the terminal change of an enqueued write task and
// translates it into an acknowledgement, applying the requested wait-timeout
// action when the wait expires.
func WaitForWrite(ctx context.Context, dispatcher *TaskDispatcher, taskUID int64, opts search.IndexingOptions) (search.IndexAck, error) {
	change, err := dispatcher.Wait(ctx, taskUID, opts.WaitTimeout)
	switch {
	case err == nil:
		// Meilisearch task completion is atomic: a succeeded task has no
		// eventual item failures and a failed task fails the whole request.
		if statusErr := change.TaskStatusError(); statusErr != nil {
			return search.IndexAckUnspecified, statusErr
		}
		return search.IndexAckCompleted, nil
	case errors.Is(err, ErrWaitTimeout):
		if opts.OnWaitTimeout == search.IndexingWaitTimeoutActionContinueAsync {
			return search.IndexAckQueued, nil
		}
		return search.IndexAckUnspecified, status.Errorf(codes.DeadlineExceeded,
			"the meilisearch task did not complete within %s; provider-side work is not guaranteed to be canceled", opts.WaitTimeout)
	default:
		return search.IndexAckUnspecified, err
	}
}

// EnqueueWrite runs a document write or deletion and returns the
// acknowledgement selected by opts, which must already be validated. A
// wait-for-completion write makes sure the wait mechanism is settled before
// anything is enqueued, then waits for the task with WaitForWrite. Any other
// mode returns IndexAckQueued as soon as Meilisearch accepts the task. msg
// describes the operation for error messages.
func EnqueueWrite(ctx context.Context, dispatcher *TaskDispatcher, opts search.IndexingOptions, msg string,
	enqueue func(ctx context.Context) (*meilisearchgo.TaskInfo, error),
) (search.IndexAck, error) {
	waitForCompletion := opts.Mode == search.IndexingModeWaitForCompletion
	if waitForCompletion {
		if err := dispatcher.EnsureStreaming(ctx); err != nil {
			return search.IndexAckUnspecified, err
		}
	}
	task, err := enqueue(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return search.IndexAckUnspecified, ContextStatusError(ctx)
		}
		return search.IndexAckUnspecified, EnqueueError(err, msg)
	}
	if task == nil {
		return search.IndexAckUnspecified, status.Errorf(codes.Internal, "%s: meilisearch accepted the request without a task", msg)
	}
	if !waitForCompletion {
		return search.IndexAckQueued, nil
	}
	return WaitForWrite(ctx, dispatcher, task.TaskUID, opts)
}

// EnqueueError reports a failed write enqueue. A request that received an HTTP
// response from Meilisearch was rejected without writing anything; a request
// that failed without a response has an indeterminate outcome and carries the
// INDEXING_OUTCOME_UNKNOWN reason.
func EnqueueError(err error, msg string) error {
	statusErr := StatusError(err, msg)
	if ResponseReceived(err) {
		return statusErr
	}
	st, _ := status.FromError(statusErr)
	return search.IndexingOutcomeUnknownError(st.Code(), st.Message())
}

// ContextStatusError converts an ended request context into a status error.
func ContextStatusError(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "the request deadline expired while waiting for meilisearch")
	}
	return status.Error(codes.Canceled, "the request was canceled while waiting for meilisearch")
}

func (d *TaskDispatcher) register(taskUID int64) (*waiter, *TaskChange, <-chan struct{}, <-chan struct{}, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, nil, nil, nil, status.Error(codes.FailedPrecondition, "the meilisearch component is closed")
	}
	w := &waiter{ch: make(chan TaskChange, 1)}
	if d.waiters[taskUID] == nil {
		d.waiters[taskUID] = map[*waiter]struct{}{}
	}
	d.waiters[taskUID][w] = struct{}{}

	// Enqueue-to-registration race: a terminal change that arrived before the
	// waiter existed is still in the time-bounded cache.
	if cached, ok := d.cache[taskUID]; ok && time.Since(cached.at) <= d.TerminalCacheTTL {
		change := cached.change
		return w, &change, d.reconnected, d.pollOnlyCh, nil
	}
	return w, nil, d.reconnected, d.pollOnlyCh, nil
}

func (d *TaskDispatcher) unregister(taskUID int64, w *waiter) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if set, ok := d.waiters[taskUID]; ok {
		delete(set, w)
		if len(set) == 0 {
			delete(d.waiters, taskUID)
		}
	}
}

func (d *TaskDispatcher) reconnectedChan() <-chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.reconnected
}

// switchToPolling records that the stream is unavailable and wakes the
// waiters registered on it so they continue by polling.
func (d *TaskDispatcher) switchToPolling(reason string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.pollOnly {
		return
	}
	d.pollOnly = true
	close(d.pollOnlyCh)
	if d.log != nil {
		d.log.Infof("meilisearch: the task-change stream is unavailable (%s); waiting for task completion by polling the task status API", reason)
	}
}

// taskStatus performs a single task status read.
func (d *TaskDispatcher) taskStatus(ctx context.Context, taskUID int64) (TaskChange, error) {
	task, err := d.tasks.GetTaskWithContext(ctx, taskUID)
	if err != nil {
		return TaskChange{}, err
	}
	change := TaskChange{UID: taskUID, Status: task.Status, Type: task.Type, IndexUID: task.IndexUID}
	if task.Status == meilisearchgo.TaskStatusFailed {
		change.Error = taskAPIError(task)
	}
	return change, nil
}

// reconcile performs a single point-in-time task status read for a stream
// waiter. It is a recovery check, not a polling loop, so a failed read is
// only logged.
func (d *TaskDispatcher) reconcile(ctx context.Context, taskUID int64) (TaskChange, bool) {
	if d.tasks == nil {
		return TaskChange{}, false
	}
	change, err := d.taskStatus(ctx, taskUID)
	if err != nil {
		if ctx.Err() == nil && d.log != nil {
			d.log.Debugf("meilisearch: reconciling task %d failed: %v", taskUID, err)
		}
		return TaskChange{}, false
	}
	if !change.Terminal() {
		return TaskChange{}, false
	}
	return change, true
}

// taskAPIError extracts the provider error of a failed task. The client's
// error payload type is unexported, so it is re-marshalled through JSON.
func taskAPIError(task *meilisearchgo.Task) APIError {
	raw, err := json.Marshal(task)
	if err != nil {
		return APIError{}
	}
	var decoded struct {
		Error APIError `json:"error"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return APIError{}
	}
	return decoded.Error
}

func (d *TaskDispatcher) publish(change TaskChange) {
	d.mu.Lock()
	now := time.Now()
	d.lastActivity = now
	if !change.Terminal() {
		d.mu.Unlock()
		return
	}
	d.cache[change.UID] = cachedChange{change: change, at: now}
	for uid, cached := range d.cache {
		if now.Sub(cached.at) > d.TerminalCacheTTL {
			delete(d.cache, uid)
		}
	}
	targets := make([]*waiter, 0, len(d.waiters[change.UID]))
	for w := range d.waiters[change.UID] {
		targets = append(targets, w)
	}
	d.mu.Unlock()

	for _, w := range targets {
		select {
		case w.ch <- change:
		default:
		}
	}
}

func (d *TaskDispatcher) touch() {
	d.mu.Lock()
	d.lastActivity = time.Now()
	d.mu.Unlock()
}

func (d *TaskDispatcher) markConnected(body io.Closer) {
	d.mu.Lock()
	previous := d.reconnected
	d.reconnected = make(chan struct{})
	d.lastActivity = time.Now()
	d.body = body
	d.mu.Unlock()
	close(previous)
}

// stale reports whether a connection carrying waiters has gone silent.
func (d *TaskDispatcher) stale() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.waiters) == 0 {
		return false
	}
	return time.Since(d.lastActivity) > d.LivenessTimeout
}

func (d *TaskDispatcher) run(ctx context.Context, resp *http.Response, done chan struct{}) {
	defer close(done)
	defer func() {
		if resp != nil {
			_ = resp.Body.Close()
		}
	}()
	defer func() {
		d.mu.Lock()
		d.running = false
		d.body = nil
		d.mu.Unlock()
	}()

	backoff := d.InitialBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		if resp == nil {
			next, err := d.dial(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				var unavailable *streamUnavailableError
				if errors.As(err, &unavailable) {
					// The route disappeared under a running stream, for
					// example after a Meilisearch upgrade or a feature
					// toggle. Waiters continue by polling.
					d.switchToPolling(unavailable.reason)
					return
				}
				if d.log != nil {
					d.log.Debugf("meilisearch: reconnecting to the task-change stream failed: %v", err)
				}
				if !sleepCtx(ctx, jitter(backoff)) {
					return
				}
				backoff = nextBackoff(backoff, d.MaxBackoff)
				continue
			}
			resp = next
		}
		backoff = d.InitialBackoff
		d.consume(ctx, resp)
		resp = nil
		if ctx.Err() != nil {
			return
		}
		if !sleepCtx(ctx, jitter(d.InitialBackoff)) {
			return
		}
	}
}

// consume reads one connection until it ends, the liveness watchdog closes it
// or the dispatcher is stopped.
func (d *TaskDispatcher) consume(ctx context.Context, resp *http.Response) {
	d.markConnected(resp.Body)
	defer func() { _ = resp.Body.Close() }()

	watchdogDone := make(chan struct{})
	defer close(watchdogDone)
	go func() {
		interval := d.LivenessTimeout / 2
		if interval <= 0 {
			interval = time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-watchdogDone:
				return
			case <-ctx.Done():
				_ = resp.Body.Close()
				return
			case <-ticker.C:
				if d.stale() {
					if d.log != nil {
						d.log.Warnf("meilisearch: the task-change stream went silent for more than %s, reconnecting", d.LivenessTimeout)
					}
					_ = resp.Body.Close()
					return
				}
			}
		}
	}()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for scanner.Scan() {
		// Any traffic, including keep-alive comments, proves liveness.
		d.touch()
		payload, ok := streamPayload(scanner.Text())
		if !ok {
			continue
		}
		var change TaskChange
		if err := json.Unmarshal(payload, &change); err != nil {
			if d.log != nil {
				d.log.Debugf("meilisearch: discarding an unparsable task-change event: %v", err)
			}
			continue
		}
		d.publish(change)
	}
}

// streamPayload extracts the JSON payload of one line of the task-change
// stream. Meilisearch documents the route as an SSE stream, and its response
// is also served as newline-delimited JSON, so both framings are accepted.
func streamPayload(line string) ([]byte, bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, ":") {
		return nil, false
	}
	if data, ok := strings.CutPrefix(trimmed, "data:"); ok {
		trimmed = strings.TrimSpace(data)
	} else if strings.HasPrefix(trimmed, "event:") || strings.HasPrefix(trimmed, "id:") || strings.HasPrefix(trimmed, "retry:") {
		return nil, false
	}
	if !strings.HasPrefix(trimmed, "{") {
		return nil, false
	}
	return []byte(trimmed), true
}

// StreamTaskTypes are the task types the shared stream is filtered to: the
// document addition/update tasks backing document and vector writes (vectors
// are stored in a document's `_vectors` field) and the deletion tasks backing
// deletes by ID.
func StreamTaskTypes() []meilisearchgo.TaskType {
	return []meilisearchgo.TaskType{
		meilisearchgo.TaskTypeDocumentAdditionOrUpdate,
		meilisearchgo.TaskTypeDocumentDeletion,
	}
}

// dial opens the shared task-change stream, filtered to the write task types
// in a terminal status.
func (d *TaskDispatcher) dial(ctx context.Context) (*http.Response, error) {
	types := make([]string, 0, 2)
	for _, taskType := range StreamTaskTypes() {
		types = append(types, string(taskType))
	}
	query := url.Values{}
	query.Set("types", strings.Join(types, ","))
	query.Set("statuses", strings.Join([]string{
		string(meilisearchgo.TaskStatusSucceeded),
		string(meilisearchgo.TaskStatusFailed),
		string(meilisearchgo.TaskStatusCanceled),
	}, ","))

	endpoint := d.host + TasksStreamPath + "?" + query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "build the meilisearch task-change stream request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	if d.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+d.apiKey)
	}

	// #nosec G704 -- endpoint is assembled from the component's configured host and fixed stream path.
	resp, err := d.httpClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ContextStatusError(ctx)
		}
		return nil, status.Errorf(codes.Unavailable, "connect to the meilisearch task-change stream: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		apiErr := readAPIError(resp.Body)
		if streamingDisabled(resp.StatusCode, apiErr) {
			return nil, &streamUnavailableError{reason: apiErrorMessage(apiErr, resp.StatusCode)}
		}
		code := CodeFromAPIError(apiErr)
		if code == codes.Unknown {
			code = CodeFromHTTPStatus(resp.StatusCode)
		}
		return nil, status.Errorf(code, "connect to the meilisearch task-change stream: %s", apiErrorMessage(apiErr, resp.StatusCode))
	}
	return resp, nil
}

// streamingDisabled reports whether the response means that Meilisearch does
// not serve the stream: the `tasksStreamingRoute` experimental feature is
// disabled (feature_not_enabled, HTTP 400) or this version has no such route
// (HTTP 404).
func streamingDisabled(statusCode int, apiErr APIError) bool {
	if apiErr.Code == "feature_not_enabled" {
		return true
	}
	return statusCode == http.StatusNotFound || statusCode == http.StatusBadRequest
}

func readAPIError(body io.Reader) APIError {
	var apiErr APIError
	raw, err := io.ReadAll(io.LimitReader(body, 8<<10))
	if err != nil {
		return apiErr
	}
	_ = json.Unmarshal(raw, &apiErr)
	return apiErr
}

func apiErrorMessage(apiErr APIError, statusCode int) string {
	if apiErr.Message != "" {
		return apiErr.Message
	}
	return fmt.Sprintf("meilisearch returned HTTP %d", statusCode)
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	next := current * 2
	if maximum > 0 && next > maximum {
		return maximum
	}
	return next
}

func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	// #nosec G404 -- reconnect jitter does not require cryptographic randomness.
	return d/2 + time.Duration(rand.Int64N(int64(d)))
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
