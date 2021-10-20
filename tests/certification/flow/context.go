// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package flow

import (
	"context"
	"testing"
	"time"
)

type Context struct {
	name string
	context.Context
	*testing.T
	*Flow
}

func (c Context) Name() string {
	return c.name
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
func (c Context) MustGet(args ...interface{}) {
	if len(args)%2 != 0 {
		c.Fatal("invalid number of arguments passed to Get")
	}

	for i := 0; i < len(args); i += 2 {
		varName, ok := args[i].(string)
		if !ok {
			c.Fatalf("argument %d is not a string", i)
		}

		c.varsMu.RLock()
		variable, ok := c.variables[varName]
		c.varsMu.RUnlock()
		if !ok {
			c.Fatalf("could not find variable %q", varName)
		}
		if !as(variable, args[i+1]) {
			c.Fatalf("could not resolve variable %q", varName)
		}
	}
}

func (c Context) Get(args ...interface{}) bool {
	if len(args)%2 != 0 {
		c.Fatal("invalid number of arguments passed to Get")
	}

	for i := 0; i < len(args); i += 2 {
		varName, ok := args[i].(string)
		if !ok {
			c.Fatalf("argument %d is not a string", i)
		}

		c.varsMu.RLock()
		variable, ok := c.variables[varName]
		c.varsMu.RUnlock()
		if !ok {
			return false
		}
		if !as(variable, args[i+1]) {
			c.Fatalf("could not resolve variable %q", varName)
		}
	}

	return true
}

func (c Context) Set(varName string, value interface{}) {
	c.varsMu.Lock()
	defer c.varsMu.Unlock()

	c.variables[varName] = value
}
