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

package queues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

// Publishing while the component is closed is a lifecycle error and must be
// classified as terminal (codes.FailedPrecondition) so the runtime stops retrying.
func TestPublishClosedIsTerminal(t *testing.T) {
	a := &azureServiceBus{logger: logger.NewLogger("test")}
	a.closed.Store(true)

	err := a.Publish(context.Background(), &pubsub.PublishRequest{Topic: "topic"})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())

	_, berr := a.BulkPublish(context.Background(), &pubsub.BulkPublishRequest{Topic: "topic"})
	require.Error(t, berr)
	bst, bok := status.FromError(berr)
	require.True(t, bok)
	require.Equal(t, codes.FailedPrecondition, bst.Code())
}
