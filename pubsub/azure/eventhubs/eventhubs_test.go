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

package eventhubs

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/pubsub"
)

// A missing topic is a request-validation error and must be classified as
// terminal (codes.FailedPrecondition) so the runtime stops retrying. This path
// returns before any broker interaction, so no live Event Hubs is required.
func TestPublishMissingTopicIsTerminal(t *testing.T) {
	aeh := &AzureEventHubs{}

	err := aeh.Publish(t.Context(), &pubsub.PublishRequest{Topic: ""})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestBulkPublishMissingTopicIsTerminal(t *testing.T) {
	aeh := &AzureEventHubs{}

	_, err := aeh.BulkPublish(t.Context(), &pubsub.BulkPublishRequest{Topic: ""})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}
