package snssqs

import (
	"testing"
)

func TestTopicsLockManager_Lock(t *testing.T) {
	lm := NewLockManager()

	topic := "topic"

	go func() {
		lm.Lock(topic)
		lm.Unlock(topic)
	}()

	go func() {
		lm.Lock(topic)
		lm.Unlock(topic)
	}()

}
