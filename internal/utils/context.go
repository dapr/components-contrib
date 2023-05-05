/*
Copyright 2023 The Dapr Authors
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

package utils

import (
	"context"
	"sync"
)

// ContextPool is a pool of contexts whereby the callee context is only
// cancelled when all callers in the pool are done. Contexts added after the
// pool is created are also tracked, but will be ignored if the pool is already
// cancelled.
type ContextPool struct {
	context.Context
	closed chan struct{}
	once   sync.Once
	pool   []<-chan struct{}
	lock   sync.RWMutex
}

// NewContextPool creates a new context pool with the given contexts. The
// returned context is cancelled when all contexts in the pool are done. Added
// contexts are ignored if the pool is already cancelled or if all current
// contexts in the pool are done.
func NewContextPool(ctx ...context.Context) *ContextPool {
	callee, cancel := context.WithCancel(context.Background())
	p := &ContextPool{
		Context: callee,
		pool:    make([]<-chan struct{}, 0, len(ctx)),
		closed:  make(chan struct{}),
	}

	for i := range ctx {
		select {
		case <-ctx[i].Done():
		default:
			p.pool = append(p.pool, ctx[i].Done())
		}
	}

	p.lock.RLock()
	go func() {
		defer cancel()
		defer p.lock.RUnlock()
		for i := 0; i < len(p.pool); i++ {
			ch := p.pool[i]
			p.lock.RUnlock()
			select {
			case <-ch:
			case <-p.closed:
			}
			p.lock.RLock()
		}
	}()

	return p
}

// Add adds a context to the pool. The context is ignored if the pool is
// already cancelled or if all current contexts in the pool are done.
func (p *ContextPool) Add(ctx context.Context) *ContextPool {
	select {
	case <-p.Done():
	case <-p.closed:
	default:
		p.lock.Lock()
		p.pool = append(p.pool, ctx.Done())
		p.lock.Unlock()
	}
	return p
}

// Cancel cancels the pool. Removes all contexts from the pool.
func (p *ContextPool) Cancel() {
	p.once.Do(func() {
		p.lock.Lock()
		defer p.lock.Unlock()
		close(p.closed)
		p.pool = nil
	})
}

// Size returns the number of contexts in the pool.
func (p *ContextPool) Size() int {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return len(p.pool)
}
