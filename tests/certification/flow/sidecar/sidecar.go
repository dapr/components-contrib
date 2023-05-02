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
		appID                    string
		options                  []interface{}
		gracefulShutdownDuration time.Duration
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
	return Sidecar{appID, options, 0}.Start
}

func (s Sidecar) Start(ctx flow.Context) error {
	logContrib := logger.NewLogger("dapr.contrib")

	var options options
	rtoptions := make([]rtembedded.Option, 0, 20)
	opts := []runtime.Option{}
	opts = append(opts, rtembedded.CommonComponents(logContrib)...)

	for _, o := range s.options {
		if rteo, ok := o.(rtembedded.Option); ok {
			rtoptions = append(rtoptions, rteo)
		} else if rto, ok := o.(runtime.Option); ok {
			opts = append(opts, rto)
		} else if rtos, ok := o.([]runtime.Option); ok {
			opts = append(opts, rtos...)
		} else if op, ok := o.(Option); ok {
			op(&options)
		}
	}

	rt, rtConf, err := rtembedded.NewRuntime(s.appID, rtoptions...)
	if err != nil {
		return err
	}
	s.gracefulShutdownDuration = rtConf.GracefulShutdownDuration

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
		client.rt.SetRunning(true)
		client.rt.Shutdown(s.gracefulShutdownDuration)

		return client.rt.WaitUntilShutdown()
	}

	return nil
}
