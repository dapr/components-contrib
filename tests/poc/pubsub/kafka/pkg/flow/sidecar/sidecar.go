package sidecar

import (
	"time"

	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/dapr/pkg/runtime/embedded"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow"

	rtembedded "github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/embedded"
	// Go SDK
	dapr "github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/client"
)

type Client struct {
	dapr.Client
	embedded.ComponentRegistry
	rt *runtime.DaprRuntime
}

func Start(appID string, options ...interface{}) flow.Runnable {
	return func(ctx flow.Context) error {
		logContrib := logger.NewLogger("dapr.contrib")

		rtoptions := make([]rtembedded.Option, 0, 20)
		opts := []runtime.Option{}
		opts = append(opts, rtembedded.CommonComponents(logContrib)...)

		for _, o := range options {
			if rto, ok := o.(rtembedded.Option); ok {
				rtoptions = append(rtoptions, rto)
			} else if rto, ok := o.(runtime.Option); ok {
				opts = append(opts, rto)
			}
		}

		rt, err := rtembedded.NewRuntime(appID, rtoptions...)
		if err != nil {
			return err
		}

		client := Client{
			rt: rt,
		}

		opts = append(opts, runtime.WithComponentsCallback(func(reg embedded.ComponentRegistry) error {
			client.ComponentRegistry = reg

			return nil
		}))

		if err = rt.Run(opts...); err != nil {
			return err
		}

		daprClient, err := dapr.NewClient()
		if err != nil {
			return err
		}

		client.Client = daprClient

		ctx.Set(appID, &client)

		return nil
	}
}

func Stop(appID string) flow.Runnable {
	return func(ctx flow.Context) error {
		var client *Client
		if ctx.Get(appID, &client) {
			client.rt.Shutdown(2 * time.Second)

			return client.rt.WaitUntilShutdown()
		}

		return nil
	}
}
