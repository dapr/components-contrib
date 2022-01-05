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

package flow

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"
)

type Runnable func(ctx Context) error

func Do(fn func() error) Runnable {
	return func(_ Context) error {
		return fn()
	}
}

func MustDo(fn func()) Runnable {
	return func(_ Context) error {
		fn()

		return nil
	}
}

func Sleep(t time.Duration) Runnable {
	return func(_ Context) error {
		time.Sleep(t)

		return nil
	}
}

type Resetable interface {
	Reset()
}

func Reset(reset ...Resetable) Runnable {
	return func(_ Context) error {
		for _, r := range reset {
			r.Reset()
		}

		return nil
	}
}

type AsyncTask struct {
	Context
	cancelOnce   sync.Once
	cancel       context.CancelFunc
	completeOnce sync.Once
	complete     chan struct{}
}

func (t *AsyncTask) Cancel() {
	t.cancelOnce.Do(func() {
		t.cancel()
	})
}

func (t *AsyncTask) Complete() {
	t.completeOnce.Do(func() {
		close(t.complete)
	})
}

func (t *AsyncTask) Wait() {
	<-t.complete
}

func (t *AsyncTask) CancelAndWait() {
	t.Cancel()
	t.Wait()
}

func Async(task *AsyncTask, runnable Runnable, cleanup ...Runnable) (Runnable, Runnable) {
	var cleanupFn Runnable
	if len(cleanup) == 1 {
		cleanupFn = cleanup[0]
	}
	return func(ctx Context) error {
			cctx, cancel := ctx.WithCancel()
			*task = AsyncTask{
				Context:      cctx,
				cancelOnce:   sync.Once{},
				cancel:       cancel,
				completeOnce: sync.Once{},
				complete:     make(chan struct{}, 1),
			}

			go func() {
				defer func() {
					task.Cancel()
					task.Complete()
				}()

				runnable(ctx)
			}()

			return nil
		},
		func(ctx Context) error {
			task.CancelAndWait()
			if cleanupFn != nil {
				cleanupFn(ctx)
			}

			return nil
		}
}

type Flow struct {
	t           *testing.T
	ctx         context.Context
	name        string
	varsMu      sync.RWMutex
	variables   map[string]interface{}
	tasks       []namedRunnable
	cleanup     []string
	uncalledMap map[string]Runnable
	cleanupMap  map[string]Runnable
}

type namedRunnable struct {
	name     string
	runnable Runnable
}

func New(t *testing.T, name string) *Flow {
	return &Flow{
		t:           t,
		ctx:         context.Background(),
		name:        name,
		variables:   make(map[string]interface{}, 25),
		tasks:       make([]namedRunnable, 0, 25),
		cleanup:     make([]string, 0, 25),
		uncalledMap: make(map[string]Runnable, 10),
		cleanupMap:  make(map[string]Runnable, 10),
	}
}

func (f *Flow) Name() string {
	return f.name
}

func as(source, target interface{}) bool {
	if target == nil {
		return false
	}
	val := reflect.ValueOf(target)
	typ := val.Type()
	if typ.Kind() != reflect.Ptr || val.IsNil() {
		return false
	}

	targetType := typ.Elem()
	if reflect.TypeOf(source).AssignableTo(targetType) {
		val.Elem().Set(reflect.ValueOf(source))
		return true
	}

	return false
}

func (f *Flow) Step(name string, runnable Runnable, cleanup ...Runnable) *Flow {
	if runnable != nil {
		f.tasks = append(f.tasks, namedRunnable{name, runnable})
	}
	if len(cleanup) == 1 {
		f.cleanup = append(f.cleanup, name)
		f.uncalledMap[name] = cleanup[0]
	}

	return f
}

func (f *Flow) StepAsync(name string, task *AsyncTask, runnable Runnable, cleanup ...Runnable) *Flow {
	r, c := Async(task, runnable, cleanup...)
	return f.Step(name, r, c)
}

func (f *Flow) Run() {
	f.t.Run(f.name, func(t *testing.T) {
		defer func() {
			for i := len(f.cleanup) - 1; i >= 0; i-- {
				name := f.cleanup[i]
				ctx := Context{
					name:    name,
					Context: f.ctx,
					T:       t,
					Flow:    f,
				}
				if cleanup, ok := f.cleanupMap[name]; ok {
					cleanup(ctx)
				}
			}
		}()

		for _, r := range f.tasks {
			if c, ok := f.uncalledMap[r.name]; ok {
				f.cleanupMap[r.name] = c
				delete(f.uncalledMap, r.name)
			}

			t.Logf("Running step: %s", r.name)
			ctx := Context{
				name:    r.name,
				Context: f.ctx,
				T:       t,
				Flow:    f,
			}
			err := r.runnable(ctx)
			t.Logf("Completed step: %s", r.name)
			if err != nil {
				t.Fatal(err)

				return
			}
		}
	})
}
