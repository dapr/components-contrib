/*
Copyright 2021 The Dapr Authors
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

package watcher

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Watcher struct {
	mu sync.Mutex

	// Expected and observed data slices.
	// Calling Add/Expect* adds data to expected.
	// Calling Observe adds data to observed.
	expected []interface{}
	observed []interface{}

	// Expected data that is yet to be observed.
	// Calling Prepare/Expect adds data and
	// calling Observe removes it.
	remaining map[interface{}]struct{}

	// When the watcher begins waiting for expected data
	// to be observed, closable set to true.
	closable bool
	// When closable is true and all remaining data is
	// observed, this channel is closed to signal completion.
	finished     chan struct{}
	finishedOnce sync.Once

	// If true, tests that the observed data is in the exact
	// order of the expected data.
	verifyOrder bool
}

// TestingT is an interface wrapper around *testing.T
type TestingT interface {
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Fail()
	FailNow()
}

// ErrTimeout denotes that the Watcher timed out
// waiting for remaining data to be observed.
var ErrTimeout = errors.New("timeout")

// NewOrdered creates a Watcher that expects
// observed data to match the ordering of the
// expected data.
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

// Reset clears all the underlying state and returns
// the watcher to a initial state.
func (w *Watcher) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.expected = make([]interface{}, 0, 1000)
	w.observed = make([]interface{}, 0, 1000)
	w.remaining = make(map[interface{}]struct{}, 1000)
	w.closable = false
	w.finished = make(chan struct{}, 1)
	w.finishedOnce = sync.Once{}
}

// Prepare is called before a network operation
// is called to add expected `data` to the `remaining` map.
// This is so that Observe can verify the data is expected
// and  add it to the `observed` slice.
// Use Prepare and Add together when created expected data
// while a separate goroutine that calls Observer is running.
func (w *Watcher) Prepare(data ...interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.remaining[item] = struct{}{}
	}
}

// Add is called after the network operation completes
// successfully and adds `data` to the `expected` slice
// so that it can be compared to the `observed` data
// at the end of the test scenario.
// Use Prepare and Add together when created expected data
// while a separate goroutine that calls Observer is running.
func (w *Watcher) Add(data ...interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.expected = append(w.expected, data...)
}

// Remove is called if the network operation fails
// and removes `data` from the `remaining` map added
// during `Prepare`. This is so that if the `Publish` '
// operation fails, `data` added for tracking could be
// removed afterwards.
func (w *Watcher) Remove(data ...interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		delete(w.remaining, item)
	}
}

// Expect adds data to both the `remaining` map
// add the expected slice in a single call.
// Use this only when a test scenario can prepare
// the expected data prior to an Observe calls.
func (w *Watcher) Expect(data ...interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.remaining[item] = struct{}{}
	}
	w.expected = append(w.expected, data...)
}

// ExpectStrings provides a simple function to
// add expected strings.
func (w *Watcher) ExpectStrings(data ...string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

// ExpectInts provides a simple function to
// add expected integers.
func (w *Watcher) ExpectInts(data ...int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

// ExpectI64s provides a simple function to
// add expected int64s.
func (w *Watcher) ExpectI64s(data ...int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

// ExpectI32s provides a simple function to
// add expected int32s.
func (w *Watcher) ExpectI32s(data ...int32) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

// ExpectI16s provides a simple function to
// add expected int16s.
func (w *Watcher) ExpectI16s(data ...int16) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

// ExpectI8s provides a simple function to
// add expected int8s.
func (w *Watcher) ExpectI8s(data ...int8) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

// ExpectUInts provides a simple function to
// add expected unsigned integers.
func (w *Watcher) ExpectUInts(data ...uint) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

// ExpectU64s provides a simple function to
// add expected uint64s.
func (w *Watcher) ExpectU64s(data ...uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

// ExpectU32s provides a simple function to
// add expected uint32s.
func (w *Watcher) ExpectU32s(data ...uint32) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

// ExpectU16s provides a simple function to
// add expected uint16s.
func (w *Watcher) ExpectU16s(data ...uint16) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

// ExpectBytes provides a simple function to
// add expected bytes.
func (w *Watcher) ExpectBytes(data ...byte) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

// ExpectRunes provides a simple function to
// add expected runes.
func (w *Watcher) ExpectRunes(data ...rune) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		w.expected = append(w.expected, item)
		w.remaining[item] = struct{}{}
	}
}

// Observe adds any data that is in `remaining` to
// the `observed` slice. If the the watcher is closable
// (all expected data captured) and there is no more
// remaining data to observe, then the finish channel
// is closed.
func (w *Watcher) Observe(data ...interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		if _, ok := w.remaining[item]; ok {
			w.observed = append(w.observed, item)
			delete(w.remaining, item)
		}
	}

	if w.closable && len(w.remaining) == 0 {
		w.finish()
	}
}

// WaitForResult waits for up to `timeout` for all
// expected data to be observed and returns an error
// if expected and observed data differ.
func (w *Watcher) WaitForResult(timeout time.Duration) error {
	w.checkClosable()

	select {
	case <-time.After(timeout):
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

func (w *Watcher) FailIfNotExpected(t TestingT, data ...interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range data {
		_, ok := w.remaining[item]
		if !ok {
			assert.Fail(t, fmt.Sprintf("Encountered an unexpected item: %v", item), item)
		}
	}
}

// Result waits for up to `timeout` for all
// expected data to be observed and returns
// the expected and observed slices.
func (w *Watcher) Result(t TestingT, timeout time.Duration) (TestingT, interface{}, interface{}) {
	w.checkClosable()

	select {
	case <-time.After(timeout):
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

// Assert waits for up to `timeout` for all
// expected data to be observed and asserts
// the expected and observed data are either
// equal (in order) or have matching elemenets
// (out of order is acceptable).
func (w *Watcher) Assert(t TestingT, timeout time.Duration) bool {
	w.checkClosable()

	select {
	case <-time.After(timeout):
		w.mu.Lock()
		defer w.mu.Unlock()

		t.Errorf("Timed out with %d items remaining: %v", len(w.remaining), w.remaining)

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

// Assert waits for up to `timeout` for all
// expected data to be observed and requires
// the expected and observed data are either
// equal (in order) or have matching elemenets
// (out of order is acceptable).
func (w *Watcher) Require(t TestingT, timeout time.Duration) {
	w.checkClosable()

	select {
	case <-time.After(timeout):
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

func (w *Watcher) checkClosable() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.closable = true

	// Close the finished channel if observations
	// are already complete.
	if len(w.remaining) == 0 {
		w.finish()
	}
}

func (w *Watcher) finish() {
	w.finishedOnce.Do(func() {
		close(w.finished)
	})
}
