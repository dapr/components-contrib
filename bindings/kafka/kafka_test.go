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

package kafka

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/internal/component/kafka"
)

func TestBindingAdapter(t *testing.T) {
	// bindingAdapter is used to adapter bindings handler to kafka.EventHandler
	// step1: prepare a new kafka event
	ct := "text/plain"
	event := &kafka.NewEvent{
		Topic:       "topic1",
		Data:        []byte("abcdefg"),
		Metadata:    map[string]string{"k1": "v1"},
		ContentType: &ct,
	}
	ctx := context.Background()

	a := testAdapter{ctx: ctx, event: event, t: t}

	// step2: call this adapter method to mock the new kafka event is triggered from kafka topic
	err := newBindingAdapter(a.testHandler).adapter(ctx, event)
	assert.NoError(t, err)
}

type testAdapter struct {
	ctx   context.Context
	event *kafka.NewEvent
	t     *testing.T
}

func (a *testAdapter) testHandler(ctx context.Context, msg *bindings.ReadResponse) ([]byte, error) {
	// step3: the binding handler should be called with the adapted binding event with the same content of the kafka event
	assert.Equal(a.t, ctx, a.ctx)
	assert.Equal(a.t, msg.Data, a.event.Data)
	assert.Equal(a.t, msg.Metadata, a.event.Metadata)
	assert.Equal(a.t, msg.ContentType, a.event.ContentType)

	return nil, nil
}
