package pubsub

import (
	"time"

	"github.com/cenkalti/backoff/v4"
)

// RetryNotifyRecover is a wrapper around backoff.RetryNotify that adds another callback for when an operation
// previously failed but has since recovered. The main purpose of this wrapper is to call `notify` only when
// the operations fails the first time and `recovered` when it finally succeeds. This can be helpful in limiting
// log messages to only the events that operators need to be alerted on.
func RetryNotifyRecover(operation backoff.Operation, b backoff.BackOff, notify backoff.Notify, recovered func()) error {
	var notified bool
	return backoff.RetryNotify(func() error {
		err := operation()

		if err == nil && notified {
			notified = false
			recovered()
		}

		return err
	}, b, func(err error, d time.Duration) {
		if !notified {
			notify(err, d)
			notified = true
		}
	})
}
