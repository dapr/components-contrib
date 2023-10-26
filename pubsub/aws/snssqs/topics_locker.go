package snssqs

import (
	"sync"

	"github.com/puzpuzpuz/xsync/v3"
)

var (
	lockManager     *TopicsLockManager
	lockManagerOnce sync.Once
)

// TopicsLockManager is a singleton for fine-grained locking, to prevent the component r/w operations
// from locking the entire component out when performing operations on different topics.
type TopicsLockManager struct {
	xLockMap *xsync.MapOf[string, *sync.Mutex]
}

func GetLockManager() *TopicsLockManager {
	lockManagerOnce.Do(func() {
		lockManager = &TopicsLockManager{xLockMap: xsync.NewMapOf[string, *sync.Mutex]()}
	})

	return lockManager
}

func (lm *TopicsLockManager) Lock(key string) *sync.Mutex {
	lock, _ := lm.xLockMap.LoadOrCompute(key, func() *sync.Mutex {
		l := &sync.Mutex{}
		l.Lock()

		return l
	})

	return lock
}

func (lm *TopicsLockManager) Unlock(key string) {
	lm.xLockMap.Compute(key, func(oldValue *sync.Mutex, exists bool) (newValue *sync.Mutex, delete bool) {
		// if exists then the mutex must be locked, and we unlock it
		if exists {
			oldValue.Unlock()
		}

		return oldValue, false
	})
}
