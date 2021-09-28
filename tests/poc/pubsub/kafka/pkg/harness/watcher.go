package harness

import (
	"errors"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Watcher struct {
	expected     []interface{}
	observedMu   sync.Mutex
	observed     []interface{}
	remainingMu  sync.Mutex
	remaining    map[interface{}]struct{}
	finished     chan struct{}
	finishedOnce sync.Once
}

// TestingT is an interface wrapper around *testing.T
type TestingT interface {
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	FailNow()
}

var ErrTimeout = errors.New("timeout")

func NewWatcher() *Watcher {
	return &Watcher{
		expected:  make([]interface{}, 0, 1000),
		observed:  make([]interface{}, 0, 1000),
		remaining: make(map[interface{}]struct{}, 1000),
		finished:  make(chan struct{}),
	}
}

func (w *Watcher) Expect(data ...interface{}) {
	w.expected = append(w.expected, data...)
}

func (w *Watcher) Observe(data ...interface{}) {
	w.observedMu.Lock()
	w.observed = append(w.observed, data...)
	w.observedMu.Unlock()

	w.remainingMu.Lock()
	for _, item := range data {
		delete(w.remaining, item)
	}
	if len(w.remaining) == 0 {
		w.finishedOnce.Do(func() {
			close(w.finished)
		})
	}
	w.remainingMu.Unlock()
}

func (w *Watcher) WaitForResult(duration time.Duration) error {
	select {
	case <-time.After(duration):
		return ErrTimeout
	case <-w.finished:
		w.remainingMu.Lock()
		diff := cmp.Diff(w.expected, w.observed)
		w.remainingMu.Unlock()
		if len(diff) > 0 {
			return errors.New(diff)
		}
	}

	return nil
}

func (w *Watcher) AssertResult(t TestingT, duration time.Duration) bool {
	select {
	case <-time.After(duration):
		t.Error(ErrTimeout)

		return false
	case <-w.finished:
		w.remainingMu.Lock()

		return assert.Equal(t, w.expected, w.observed)
	}
}

func (w *Watcher) RequireResult(t TestingT, duration time.Duration) {
	select {
	case <-time.After(duration):
		t.Error(ErrTimeout)

		require.FailNow(t, "timeout")
	case <-w.finished:
		w.remainingMu.Lock()

		require.Equal(t, w.expected, w.observed)
	}
}
