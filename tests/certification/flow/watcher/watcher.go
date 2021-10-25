// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package watcher

import (
	"errors"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Watcher struct {
	mu           sync.Mutex
	expected     []interface{}
	observed     []interface{}
	remaining    map[interface{}]struct{}
	finished     chan struct{}
	finishedOnce sync.Once
	verifyOrder  bool
}

// TestingT is an interface wrapper around *testing.T
type TestingT interface {
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Fail()
	FailNow()
}

var ErrTimeout = errors.New("timeout")

func NewOrdered() *Watcher {
	return New(true)
}

func NewUnordered() *Watcher {
	return New(false)
}

func New(verifyOrder bool) *Watcher {
	return &Watcher{
		expected:    make([]interface{}, 0, 1000),
		observed:    make([]interface{}, 0, 1000),
		remaining:   make(map[interface{}]struct{}, 1000),
		finished:    make(chan struct{}, 1),
		verifyOrder: verifyOrder,
	}
}

func (w *Watcher) Expect(data ...interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.expected = append(w.expected, data...)
	for _, item := range data {
		w.remaining[item] = struct{}{}
	}
}

func (w *Watcher) ExpectStrings(data ...string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

func (w *Watcher) ExpectInts(data ...int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

func (w *Watcher) ExpectI64s(data ...int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

func (w *Watcher) ExpectI32s(data ...int32) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

func (w *Watcher) ExpectI16s(data ...int16) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

func (w *Watcher) ExpectI8s(data ...int8) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

func (w *Watcher) ExpectUInts(data ...uint) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

func (w *Watcher) ExpectU64s(data ...uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

func (w *Watcher) ExpectU32s(data ...uint32) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

func (w *Watcher) ExpectU16s(data ...uint16) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

func (w *Watcher) ExpectBytes(data ...byte) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

func (w *Watcher) ExpectRunes(data ...rune) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

func (w *Watcher) Observe(data ...interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		if _, ok := w.remaining[item]; ok {
			w.observed = append(w.observed, item)
			delete(w.remaining, item)
		}
	}

	if len(w.remaining) == 0 {
		w.finishedOnce.Do(func() {
			close(w.finished)
		})
	}
}

func (w *Watcher) WaitForResult(duration time.Duration) error {
	select {
	case <-time.After(duration):
		return ErrTimeout
	case <-w.finished:
		w.mu.Lock()
		diff := cmp.Diff(w.expected, w.observed)
		w.mu.Unlock()
		if len(diff) > 0 {
			return errors.New(diff)
		}
	}

	return nil
}

func (w *Watcher) Result(t TestingT, duration time.Duration) (TestingT, interface{}, interface{}) {
	select {
	case <-time.After(duration):
		w.mu.Lock()
		remainingCount := len(w.remaining)
		w.mu.Unlock()

		t.Errorf("Timed out with %d items remaining", remainingCount)
		t.Fail()

		return t, nil, nil
	case <-w.finished:
		w.mu.Lock()
		defer w.mu.Unlock()

		return t, w.expected, w.observed
	}
}

func (w *Watcher) Assert(t TestingT, duration time.Duration) bool {
	select {
	case <-time.After(duration):
		w.mu.Lock()
		remainingCount := len(w.remaining)
		w.mu.Unlock()

		t.Errorf("Timed out with %d items remaining", remainingCount)

		return false
	case <-w.finished:
		w.mu.Lock()
		defer w.mu.Unlock()

		if w.verifyOrder {
			return assert.Equal(t, w.expected, w.observed)
		}

		return assert.ElementsMatch(t, w.expected, w.observed)
	}
}

func (w *Watcher) Require(t TestingT, duration time.Duration) {
	select {
	case <-time.After(duration):
		w.mu.Lock()
		remainingCount := len(w.remaining)
		w.mu.Unlock()

		t.Errorf("Timed out with %d items remaining", remainingCount)

		require.FailNow(t, "timeout")
	case <-w.finished:
		w.mu.Lock()
		defer w.mu.Unlock()

		if w.verifyOrder {
			require.Equal(t, w.expected, w.observed)
		} else {
			require.ElementsMatch(t, w.expected, w.observed)
		}
	}
}
