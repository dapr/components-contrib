/*
Copyright 2022 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
This package has been forked from https://github.com/robfig/cron available under the MIT license.
You can check the original license at:
		https://github.com/robfig/cron/blob/master/LICENSE
*/

//nolint
package cron

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
)

// JobWrapper decorates the given Job with some behavior.
type JobWrapper func(Job) Job

// Chain is a sequence of JobWrappers that decorates submitted jobs with
// cross-cutting behaviors like logging or synchronization.
type Chain struct {
	wrappers []JobWrapper
}

// NewChain returns a Chain consisting of the given JobWrappers.
func NewChain(c ...JobWrapper) Chain {
	return Chain{c}
}

// Then decorates the given job with all JobWrappers in the chain.
//
// This:
//
//	NewChain(m1, m2, m3).Then(job)
//
// is equivalent to:
//
//	m1(m2(m3(job)))
func (c Chain) Then(j Job) Job {
	for i := range c.wrappers {
		j = c.wrappers[len(c.wrappers)-i-1](j)
	}
	return j
}

// Recover panics in wrapped jobs and log them with the provided logger.
func Recover(logger Logger) JobWrapper {
	return func(j Job) Job {
		return FuncJob(func() {
			defer func() {
				if r := recover(); r != nil {
					const size = 64 << 10
					buf := make([]byte, size)
					buf = buf[:runtime.Stack(buf, false)]
					err, ok := r.(error)
					if !ok {
						err = fmt.Errorf("%v", r)
					}
					logger.Error(err, "panic", "stack", "...\n"+string(buf))
				}
			}()
			j.Run()
		})
	}
}

// DelayIfStillRunning serializes jobs, delaying subsequent runs until the
// previous one is complete. Jobs running after a delay of more than a minute
// have the delay logged at Info.
func DelayIfStillRunning(logger Logger) JobWrapper {
	return DelayIfStillRunningWithClock(logger, clock.New())
}

// DelayIfStillRunningWithClock behaves identically to DelayIfStillRunning but
// uses the provided Clock for measuring the delay, for use in testing.
func DelayIfStillRunningWithClock(logger Logger, clk clock.Clock) JobWrapper {
	return func(j Job) Job {
		var mu sync.Mutex
		return FuncJob(func() {
			start := clk.Now()
			mu.Lock()
			defer mu.Unlock()
			if dur := clk.Since(start); dur > time.Minute {
				logger.Info("delay", "duration", dur)
			}
			j.Run()
		})
	}
}

// SkipIfStillRunning skips an invocation of the Job if a previous invocation is
// still running. It logs skips to the given logger at Info level.
func SkipIfStillRunning(logger Logger) JobWrapper {
	return func(j Job) Job {
		ch := make(chan struct{}, 1)
		ch <- struct{}{}
		return FuncJob(func() {
			select {
			case v := <-ch:
				j.Run()
				ch <- v
			default:
				logger.Info("skip")
			}
		})
	}
}
