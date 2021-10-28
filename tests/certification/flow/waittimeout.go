package flow

import (
	"sync"
	"time"
)

// WaitTimeout waits for `wg` for the specified `timeout`.
// Returns true if waiting timed out.
func WaitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
