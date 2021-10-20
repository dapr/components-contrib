// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package flow

import (
	"context"
	"reflect"
	"sync"
	"testing"
)

type Runnable func(ctx Context) error

func Do(fn func() error) Runnable {
	return func(_ Context) error {
		return fn()
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

			if !t.Run(r.name, func(t *testing.T) {
				ctx := Context{
					name:    r.name,
					Context: f.ctx,
					T:       t,
					Flow:    f,
				}
				if err := r.runnable(ctx); err != nil {
					t.Fatal(err)

					return
				}
			}) {
				break
			}
		}
	})
}
