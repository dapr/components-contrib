package dingtalk

import (
	"encoding/json"
	"fmt"
	"testing"

	dapr "github.com/dapr/go-sdk/client"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/dapr/pkg/runtime"

	"github.com/dapr/components-contrib/secretstores"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/alicloud/dingtalk/webhook"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	"github.com/dapr/kit/logger"

	dapr_testing "github.com/dapr/dapr/pkg/testing"

	"github.com/dapr/components-contrib/tests/certification/flow"
)

type Msg struct {
	Msgtype string            `json:"msgtype"`
	Text    map[string]string `json:"text"`
}

func TestDingTalkBind(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	outComponent := bindings_loader.NewOutput("alicloud.dingtalk", func() bindings.OutputBinding {
		return webhook.NewDingTalkWebhook(log)
	})
	secretsComponents := secretstores_loader.New("local.env", func() secretstores.SecretStore {
		return secretstore_env.NewEnvSecretStore(log)
	})

	ports, _ := dapr_testing.GetFreePorts(1)
	grpcPort := ports[0]

	sendMessage := func(ctx flow.Context) error {
		client, err := dapr.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "dapr init failed")

		data, _ := json.Marshal(Msg{
			Msgtype: "text",
			Text: map[string]string{
				"content": "hello",
			},
		})
		err = client.InvokeOutputBinding(
			ctx, &dapr.InvokeBindingRequest{
				Name:      "alicloud-dingtalk-binding",
				Operation: string(bindings.CreateOperation),
				Data:      data,
			})
		require.NoError(ctx, err, "error publishing message")

		return nil
	}

	flow.New(t, "send message via dingtalk output").
		Step(sidecar.Run("sidecar",
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components"),
			runtime.WithSecretStores(secretsComponents),
			runtime.WithOutputBindings(outComponent),
		)).
		Step("send and wait", sendMessage).
		Run()
}
