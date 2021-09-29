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
	t          *testing.T
	ctx        context.Context
	name       string
	varsMu     sync.RWMutex
	variables  map[string]interface{}
	tasks      []namedRunnable
	cleanup    []string
	cleanupMap map[string]Runnable
}

type namedRunnable struct {
	name     string
	runnable Runnable
}

func New(t *testing.T, name string) *Flow {
	return &Flow{
		t:          t,
		ctx:        context.Background(),
		name:       name,
		variables:  make(map[string]interface{}, 25),
		tasks:      make([]namedRunnable, 0, 25),
		cleanup:    make([]string, 0, 25),
		cleanupMap: make(map[string]Runnable, 10),
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

func (f *Flow) Task(name string, runnable Runnable) *Flow {
	f.tasks = append(f.tasks, namedRunnable{name, runnable})
	return f
}

func (f *Flow) Service(name string, start Runnable, stop Runnable) *Flow {
	f.tasks = append(f.tasks, namedRunnable{name, start})
	f.cleanup = append(f.cleanup, name)
	f.cleanupMap[name] = stop
	return f
}

func (f *Flow) Stop(name string) *Flow {
	ctx := Context{
		name:    name,
		Context: f.ctx,
		T:       f.t,
		Flow:    f,
	}
	if cleanup, ok := f.cleanupMap[name]; ok {
		delete(f.cleanupMap, name)
		cleanup(ctx)
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
