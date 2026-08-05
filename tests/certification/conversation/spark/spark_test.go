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

package spark_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/spark"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func TestSparkConversation(t *testing.T) {
	key := os.Getenv("IFLYTEK_API_KEY")
	require.NotEmpty(t, key, "IFLYTEK_API_KEY must be set for certification tests")

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	component := spark.NewSpark(logger.NewLogger("dapr.components.conversation.spark.certification"))
	t.Cleanup(func() {
		require.NoError(t, component.Close())
	})

	err := component.Init(ctx, conversation.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"key": key,
			},
		},
	})
	require.NoError(t, err)

	messages := []llms.MessageContent{
		llms.TextParts(llms.ChatMessageTypeHuman, "Reply with the single word: pong"),
	}
	response, err := component.Converse(ctx, &conversation.Request{Message: &messages})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, "4.0Ultra", response.Model)
	require.NotEmpty(t, response.Outputs)
	require.NotEmpty(t, response.Outputs[0].Choices)
	require.NotEmpty(t, response.Outputs[0].Choices[0].Message.Content)
}
