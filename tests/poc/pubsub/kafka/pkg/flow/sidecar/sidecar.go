package sidecar

import (
	"time"

	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/dapr/pkg/runtime/embedded"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/client"
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow"

	rtembedded "github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/embedded"
	// Go SDK
	dapr "github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/client"
)

type sidecar struct {
	flow.Task
	appID   string
	options []interface{}
}

type stop struct {
	flow.Task
	appID string
}

type Client struct {
	client.Client
	embedded.ComponentRegistry
	rt *runtime.DaprRuntime
}

func Start(appID string, options ...interface{}) flow.Runnable {
	return &sidecar{
		appID:   appID,
		options: options,
	}
}

func Stop(appID string) flow.Runnable {
	return &stop{
		appID: appID,
	}
}

func (d *sidecar) Run() error {
	logContrib := logger.NewLogger("dapr.contrib")

	rtoptions := make([]rtembedded.Option, 0, 20)
	opts := []runtime.Option{}
	opts = append(opts, rtembedded.CommonComponents(logContrib)...)

	for _, o := range d.options {
		if rto, ok := o.(rtembedded.Option); ok {
			rtoptions = append(rtoptions, rto)
		} else if rto, ok := o.(runtime.Option); ok {
			opts = append(opts, rto)
		}
	}

	rt, err := rtembedded.NewRuntime(d.appID, rtoptions...)
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

	d.Set(d.appID, &client)

	return nil
}

func (d *stop) Run() error {
	var client *Client
	if d.Get(d.appID, &client) {
		client.rt.Shutdown(2 * time.Second)
		client.rt.WaitUntilShutdown()
	}

	return nil
}
