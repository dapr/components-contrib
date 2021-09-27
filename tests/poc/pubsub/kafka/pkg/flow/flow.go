package flow

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

type Flow struct {
	*testing.T
	ctx        context.Context
	name       string
	varsMu     sync.RWMutex
	variables  map[string]interface{}
	tasks      []Runnable
	cleanup    []string
	cleanupMap map[string]Runnable
}

type Context struct {
	context.Context
	*testing.T
	*Flow
}

func (c Context) Deadline() (deadline time.Time, ok bool) {
	return c.Context.Deadline()
}
func (c Context) Done() <-chan struct{} {
	return c.Context.Done()
}
func (c Context) Err() error {
	return c.Context.Err()
}
func (c Context) Value(key interface{}) interface{} {
	return c.Context.Value(key)
}

type Runnable interface {
	Initialize(flow *Flow, name string)
	Name() string
	Prepare(ctx context.Context, tt *testing.T)
	Run() error
}

func New(t *testing.T, name string) *Flow {
	return &Flow{
		T:          t,
		ctx:        context.Background(),
		name:       name,
		variables:  make(map[string]interface{}, 25),
		tasks:      make([]Runnable, 0, 25),
		cleanup:    make([]string, 0, 25),
		cleanupMap: make(map[string]Runnable, 10),
	}
}

func (f *Flow) Name() string {
	return f.name
}

func (f *Flow) MustGet(args ...interface{}) {
	if len(args)%2 != 0 {
		f.Fatal("invalid number of arguments passed to Get")
	}

	for i := 0; i < len(args); i += 2 {
		varName, ok := args[i].(string)
		if !ok {
			f.Fatalf("argument %d is not a string", i)
		}

		f.varsMu.RLock()
		variable, ok := f.variables[varName]
		f.varsMu.RUnlock()
		if !ok {
			f.Fatalf("could not find variable %q", varName)
		}
		if !as(variable, args[i+1]) {
			f.Fatalf("could not resolve variable %q", varName)
		}
	}
}

func (f *Flow) Get(args ...interface{}) bool {
	if len(args)%2 != 0 {
		f.Fatal("invalid number of arguments passed to Get")
	}

	for i := 0; i < len(args); i += 2 {
		varName, ok := args[i].(string)
		if !ok {
			f.Fatalf("argument %d is not a string", i)
		}

		f.varsMu.RLock()
		variable, ok := f.variables[varName]
		f.varsMu.RUnlock()
		if !ok {
			return false
		}
		if !as(variable, args[i+1]) {
			f.Fatalf("could not resolve variable %q", varName)
		}
	}

	return true
}

func (f *Flow) Set(varName string, value interface{}) {
	f.varsMu.Lock()
	defer f.varsMu.Unlock()

	f.variables[varName] = value
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
	runnable.Initialize(f, name)
	f.tasks = append(f.tasks, runnable)
	return f
}

func (f *Flow) Service(name string, start Runnable, stop Runnable) *Flow {
	start.Initialize(f, name+" start")
	stop.Initialize(f, name+" stop")
	f.tasks = append(f.tasks, start)
	f.cleanup = append(f.cleanup, name)
	f.cleanupMap[name] = stop
	return f
}

func (f *Flow) Stop(name string) *Flow {
	if cleanup, ok := f.cleanupMap[name]; ok {
		delete(f.cleanupMap, name)
		cleanup.Run()
	}

	return f
}

func (f *Flow) Func(name string, fn func(Context) error) *Flow {
	fi := funcInternal{
		fn: fn,
	}
	fi.Initialize(f, name)
	f.tasks = append(f.tasks, &fi)
	return f
}

func (f *Flow) Run() {
	f.T.Run(f.name, func(t *testing.T) {
		defer func() {
			for i := len(f.cleanup) - 1; i >= 0; i-- {
				name := f.cleanup[i]
				if cleanup, ok := f.cleanupMap[name]; ok {
					cleanup.Run()
				}
			}
		}()

		for _, r := range f.tasks {
			t.Run(r.Name(), func(t *testing.T) {
				r.Prepare(f.ctx, t)
				if err := r.Run(); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
}

var _ = Runnable((*Task)((nil)))

type Task struct {
	*testing.T
	ctx  context.Context
	flow *Flow
	name string
}

func (t *Task) Initialize(flow *Flow, name string) {
	t.flow = flow
	t.name = name
}

func (t *Task) Prepare(ctx context.Context, tt *testing.T) {
	t.ctx = ctx
	t.T = tt
}

func (t *Task) Name() string {
	return t.name
}

func (t *Task) MustGet(args ...interface{}) {
	t.flow.MustGet(args...)
}

func (t *Task) Get(args ...interface{}) bool {
	return t.flow.Get(args...)
}

func (t *Task) Set(varName string, value interface{}) {
	t.flow.Set(varName, value)
}

func (t *Task) GetContext() Context {
	return Context{
		Context: t.ctx,
		T:       t.T,
		Flow:    t.flow,
	}
}

func (t *Task) Run() error {
	return fmt.Errorf("Run for task %q is not implemented", t.name)
}

var _ = Runnable((*funcInternal)((nil)))

type funcInternal struct {
	Task
	fn func(Context) error
}

func (f *funcInternal) Run() error {
	return f.fn(Context{
		Context: f.ctx,
		T:       f.T,
		Flow:    f.flow,
	})
}
