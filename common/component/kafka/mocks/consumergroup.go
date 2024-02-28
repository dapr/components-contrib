/*
Copyright 2024 The Dapr Authors
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

package mocks

import (
	"context"

	"github.com/IBM/sarama"
)

type FakeConsumerGroup struct {
	consumerFn  func(context.Context, []string, sarama.ConsumerGroupHandler) error
	errorsFn    func() <-chan error
	closeFn     func() error
	pauseFn     func(map[string][]int32)
	resumeFn    func(map[string][]int32)
	pauseAllFn  func()
	resumeAllFn func()
}

func NewConsumerGroup() *FakeConsumerGroup {
	return &FakeConsumerGroup{
		consumerFn: func(context.Context, []string, sarama.ConsumerGroupHandler) error {
			return nil
		},
		errorsFn: func() <-chan error {
			return nil
		},
		closeFn: func() error {
			return nil
		},
		pauseFn: func(map[string][]int32) {
		},
		resumeFn: func(map[string][]int32) {
		},
		pauseAllFn: func() {
		},
		resumeAllFn: func() {
		},
	}
}

func (f *FakeConsumerGroup) WithConsumeFn(fn func(context.Context, []string, sarama.ConsumerGroupHandler) error) *FakeConsumerGroup {
	f.consumerFn = fn
	return f
}

func (f *FakeConsumerGroup) WithErrorsFn(fn func() <-chan error) *FakeConsumerGroup {
	f.errorsFn = fn
	return f
}

func (f *FakeConsumerGroup) WithCloseFn(fn func() error) *FakeConsumerGroup {
	f.closeFn = fn
	return f
}

func (f *FakeConsumerGroup) WithPauseFn(fn func(map[string][]int32)) *FakeConsumerGroup {
	f.pauseFn = fn
	return f
}

func (f *FakeConsumerGroup) WithResumeFn(fn func(map[string][]int32)) *FakeConsumerGroup {
	f.resumeFn = fn
	return f
}

func (f *FakeConsumerGroup) WithPauseAllFn(fn func()) *FakeConsumerGroup {
	f.pauseAllFn = fn
	return f
}

func (f *FakeConsumerGroup) WithResumeAllFn(fn func()) *FakeConsumerGroup {
	f.resumeAllFn = fn
	return f
}

func (f *FakeConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	return f.consumerFn(ctx, topics, handler)
}

func (f *FakeConsumerGroup) Errors() <-chan error {
	return f.errorsFn()
}

func (f *FakeConsumerGroup) Close() error {
	return f.closeFn()
}

func (f *FakeConsumerGroup) Pause(partitions map[string][]int32) {
	f.pauseFn(partitions)
}

func (f *FakeConsumerGroup) Resume(partitions map[string][]int32) {
	f.resumeFn(partitions)
}

func (f *FakeConsumerGroup) PauseAll() {
	f.pauseAllFn()
}

func (f *FakeConsumerGroup) ResumeAll() {
	f.resumeAllFn()
}
