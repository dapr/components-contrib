// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package sidecar

import (
	"fmt"
	"time"

	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/flow"

	rtembedded "github.com/dapr/components-contrib/tests/certification/embedded"
	// Go SDK
	dapr "github.com/dapr/go-sdk/client"
)

type (
	Client struct {
		dapr.Client
		runtime.ComponentRegistry
		rt *runtime.DaprRuntime
	}

	Sidecar struct {
		appID   string
		options []interface{}
	}

	ClientCallback func(client *Client)

	options struct {
		clientCallback ClientCallback
	}

	Option func(o *options)
)

func WithClientCallback(callback ClientCallback) Option {
	return func(o *options) {
		o.clientCallback = callback
	}
}

func GetClient(ctx flow.Context, sidecarName string) *Client {
	var client *Client
	ctx.MustGet(sidecarName, &client)
	return client
}

func Run(appID string, options ...interface{}) (string, flow.Runnable, flow.Runnable) {
	return New(appID, options...).ToStep()
}

func New(appID string, options ...interface{}) Sidecar {
	return Sidecar{
		appID:   appID,
		options: options,
	}
}

func (s Sidecar) AppID() string {
	return s.appID
}

func (s Sidecar) ToStep() (string, flow.Runnable, flow.Runnable) {
	return s.appID, s.Start, s.Stop
}

func Start(appID string, options ...interface{}) flow.Runnable {
	return Sidecar{appID, options}.Start
}

func (s Sidecar) Start(ctx flow.Context) error {
	logContrib := logger.NewLogger("dapr.contrib")

	var options options
	rtoptions := make([]rtembedded.Option, 0, 20)
	opts := []runtime.Option{}
	opts = append(opts, rtembedded.CommonComponents(logContrib)...)

	for _, o := range s.options {
		if rto, ok := o.(rtembedded.Option); ok {
			rtoptions = append(rtoptions, rto)
		} else if rto, ok := o.(runtime.Option); ok {
			opts = append(opts, rto)
		} else if rto, ok := o.(Option); ok {
			rto(&options)
		}
	}

	rt, rtConf, err := rtembedded.NewRuntime(s.appID, rtoptions...)
	if err != nil {
		return err
	}

	client := Client{
		rt: rt,
	}

	opts = append(opts, runtime.WithComponentsCallback(func(reg runtime.ComponentRegistry) error {
		client.ComponentRegistry = reg

		return nil
	}))

	if err = rt.Run(opts...); err != nil {
		return err
	}

	daprClient, err := dapr.NewClientWithPort(fmt.Sprintf("%d", rtConf.APIGRPCPort))
	if err != nil {
		return err
	}

	client.Client = daprClient

	ctx.Set(s.appID, &client)

	if options.clientCallback != nil {
		options.clientCallback(&client)
	}

	return nil
}

func Stop(appID string) flow.Runnable {
	return Sidecar{appID: appID}.Stop
}

func (s Sidecar) Stop(ctx flow.Context) error {
	var client *Client
	if ctx.Get(s.appID, &client) {
		client.rt.Shutdown(2 * time.Second)

		return client.rt.WaitUntilShutdown()
	}

	return nil
}
