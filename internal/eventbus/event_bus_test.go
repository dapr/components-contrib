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

/*
The MIT License (MIT)
Copyright (c) 2014 Alex Saskevich
*/

//nolint:errcheck
package eventbus

import (
	"testing"
	"time"

	"go.uber.org/atomic"
)

func TestNew(t *testing.T) {
	bus := New(false)
	if bus == nil {
		t.Log("New EventBus not created!")
		t.Fail()
	}
}

func TestGetCallbacks(t *testing.T) {
	t.Run("no wildcards", func(t *testing.T) {
		bus := New(false).(*EventBus)
		bus.Subscribe("topic", func() {})
		bus.Subscribe("topic*", func() {})
		if cbs := bus.getCallbacks("my_topic"); len(cbs) > 0 {
			t.Fail()
		}
		if cbs := bus.getCallbacks("topic"); len(cbs) != 1 {
			t.Fail()
		}
		if cbs := bus.getCallbacks("topic*"); len(cbs) != 1 {
			t.Fail()
		}
		if cbs := bus.getCallbacks("topic1"); len(cbs) > 0 {
			t.Fail()
		}
	})

	t.Run("with wildcards", func(t *testing.T) {
		bus := New(true).(*EventBus)
		bus.Subscribe("topic", func() {})
		bus.Subscribe("topic*", func() {})
		bus.Subscribe("topi*", func() {})
		if cbs := bus.getCallbacks("my_topic"); len(cbs) > 0 {
			t.Fail()
		}
		if cbs := bus.getCallbacks("topic"); len(cbs) != 2 {
			t.Fail()
		}
		if cbs := bus.getCallbacks("topic1"); len(cbs) != 2 {
			t.Fail()
		}
		if cbs := bus.getCallbacks("topicfoobar"); len(cbs) != 2 {
			t.Fail()
		}
	})
}

func TestSubscribe(t *testing.T) {
	bus := New(false)
	if bus.Subscribe("topic", func() {}) != nil {
		t.Fail()
	}
	if bus.Subscribe("topic", "String") == nil {
		t.Fail()
	}
}

func TestUnsubscribe(t *testing.T) {
	bus := New(false)
	handler := func() {}
	bus.Subscribe("topic*", handler)
	if bus.Unsubscribe("topic*", handler) != nil {
		t.Fail()
	}
	if bus.Unsubscribe("topic*", handler) == nil {
		t.Fail()
	}
}

type handler struct {
	val int
}

func (h *handler) Handle() {
	h.val++
}

func TestUnsubscribeMethod(t *testing.T) {
	bus := New(false)
	h := &handler{val: 0}

	bus.Subscribe("topic", h.Handle)
	bus.Publish("topic")
	if bus.Unsubscribe("topic", h.Handle) != nil {
		t.Fail()
	}
	if bus.Unsubscribe("topic", h.Handle) == nil {
		t.Fail()
	}
	bus.Publish("topic")
	bus.WaitAsync()

	if h.val != 1 {
		t.Fail()
	}
}

func TestPublish(t *testing.T) {
	bus := New(false)
	bus.Subscribe("topic", func(a int, err error) {
		if a != 10 {
			t.Fail()
		}

		if err != nil {
			t.Fail()
		}
	})
	bus.Publish("topic", 10, nil)
}

func TestSubscribeAsyncTransactional(t *testing.T) {
	results := make([]int, 0)

	bus := New(false)
	bus.SubscribeAsync("topic", func(a int, out *[]int, dur string) {
		sleep, _ := time.ParseDuration(dur)
		time.Sleep(sleep)
		*out = append(*out, a)
	}, true)

	bus.Publish("topic", 1, &results, "1s")
	bus.Publish("topic", 2, &results, "0s")

	bus.WaitAsync()

	if len(results) != 2 {
		t.Fail()
	}

	if results[0] != 1 || results[1] != 2 {
		t.Fail()
	}
}

func TestSubscribeAsync(t *testing.T) {
	results := make(chan int)

	bus := New(false)
	bus.SubscribeAsync("topic", func(a int, out chan<- int) {
		out <- a
	}, false)

	numResults := atomic.NewInt32(0)

	go func() {
		for range results {
			numResults.Inc()
		}
	}()

	bus.Publish("topic", 1, results)
	bus.Publish("topic", 2, results)

	bus.WaitAsync()

	time.Sleep(100 * time.Millisecond)

	if numResults.Load() != 2 {
		t.Fail()
	}
}

func TestWildcards(t *testing.T) {
	results := make(chan int)

	bus := New(true)
	bus.SubscribeAsync("topic/*", func(a int, out chan<- int) {
		out <- a
	}, false)

	numResults := atomic.NewInt32(0)
	go func() {
		for range results {
			numResults.Inc()
		}
	}()

	bus.Publish("topic", 1, results)
	bus.Publish("topic", 2, results)
	bus.Publish("topic/1", 3, results)
	bus.Publish("topic/2", 4, results)

	bus.WaitAsync()

	time.Sleep(100 * time.Millisecond)

	if numResults.Load() != 2 {
		t.Fail()
	}
}
