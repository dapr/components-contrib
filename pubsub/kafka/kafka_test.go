/*
Copyright 2026 The Dapr Authors
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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/pubsub"
)

// Compile-time assertion that PubSub implements the optional
// PausableSubscriber capability.
var _ pubsub.PausableSubscriber = (*PubSub)(nil)

// TestPublishWhenClosedIsTerminal verifies that publishing through a closed
// component returns a terminal (codes.FailedPrecondition) error so the runtime
// does not retry it.
func TestPublishWhenClosedIsTerminal(t *testing.T) {
	p := &PubSub{closeCh: make(chan struct{})}
	p.closed.Store(true)

	t.Run("Publish", func(t *testing.T) {
		err := p.Publish(t.Context(), &pubsub.PublishRequest{Topic: "topic"})
		require.Error(t, err)
		s, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.FailedPrecondition, s.Code())
	})

	t.Run("BulkPublish", func(t *testing.T) {
		_, err := p.BulkPublish(t.Context(), &pubsub.BulkPublishRequest{Topic: "topic"})
		require.Error(t, err)
		s, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.FailedPrecondition, s.Code())
	})
}
