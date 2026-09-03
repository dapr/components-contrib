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

package ollama_test

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	conversation_ollama "github.com/dapr/components-contrib/conversation/ollama"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	conversation_loader "github.com/dapr/dapr/pkg/components/conversation"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

const (
	componentName     = "ollama"
	sidecarName       = "conversation-ollama-sidecar"
	dockerProjectName = "conversation-ollama"
	dockerComposeYAML = "../../../../.github/infrastructure/docker-compose-ollama.yml"
	modelName         = "qwen2.5:0.5b"
)

func TestOllama(t *testing.T) {
	t.Setenv("OLLAMA_MODEL", modelName)

	ports, err := dapr_testing.GetFreePorts(2)
	require.NoError(t, err)

	pullModel := func(ctx flow.Context) error {
		pullCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()

		out, err := exec.CommandContext(
			pullCtx,
			"docker", "compose",
			"-p", dockerProjectName,
			"-f", dockerComposeYAML,
			"exec", "-T", "ollama",
			"ollama", "pull", modelName,
		).CombinedOutput()
		ctx.Log(string(out))
		if err != nil {
			return fmt.Errorf("failed to pull Ollama model %s: %w", modelName, err)
		}
		return nil
	}

	testConverse := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName)
		requestCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()

		resp, err := client.ConverseAlpha2(requestCtx, dapr.ConversationRequestAlpha2{
			Name: componentName,
			Inputs: []*dapr.ConversationInputAlpha2{
				{
					Messages: []*dapr.ConversationMessageAlpha2{
						{
							ConversationMessageOfUser: &dapr.ConversationMessageOfUserAlpha2{
								Content: []*dapr.ConversationMessageContentAlpha2{
									{Text: ptr.Of("Reply with a short greeting.")},
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.Outputs, 1)
		require.Len(t, resp.Outputs[0].Choices, 1)
		require.NotEmpty(t, resp.Outputs[0].Choices[0].FinishReason)
		require.NotEmpty(t, resp.Outputs[0].Choices[0].Message.Content)
		return nil
	}

	flow.New(t, "ollama conversation certification").
		Step(dockercompose.Run(dockerProjectName, dockerComposeYAML)).
		Step("pull model", pullModel).
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithResourcesPath("./components"),
			embedded.WithDaprGRPCPort(strconv.Itoa(ports[0])),
			embedded.WithDaprHTTPPort(strconv.Itoa(ports[1])),
			embedded.WithConversations(newConversationRegistry()),
		)).
		Step("converse", testConverse).
		Run()
}

func newConversationRegistry() *conversation_loader.Registry {
	registry := conversation_loader.NewRegistry()
	registry.Logger = logger.NewLogger("dapr.components")
	registry.RegisterComponent(conversation_ollama.NewOllama, componentName)
	return registry
}
