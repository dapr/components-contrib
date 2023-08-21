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
	"sync/atomic"
	"time"

	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/dapr/pkg/runtime/registry"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"

	rtembedded "github.com/dapr/components-contrib/tests/certification/embedded"
	// Go SDK
	dapr "github.com/dapr/go-sdk/client"
)

type (
	Client struct {
		dapr.Client
		registry.ComponentRegistry
		rt     *runtime.DaprRuntime
		errCh  chan error
		closed atomic.Bool
	}

	Sidecar struct {
		appID                    string
		options                  []embedded.Option
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

func Run(appID string, options ...embedded.Option) (string, flow.Runnable, flow.Runnable) {
	return New(appID, options...).ToStep()
}

func New(appID string, options ...embedded.Option) Sidecar {
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

func Start(appID string, options ...embedded.Option) flow.Runnable {
	return Sidecar{appID, options, 0}.Start
}

func (s Sidecar) Start(ctx flow.Context) error {
	logContrib := logger.NewLogger("dapr.contrib")

	var options options
	opts := append(rtembedded.CommonComponents(logContrib), s.options...)

	var client Client
	opts = append(opts, func(cfg *runtime.Config) {
		cfg.Registry = cfg.Registry.WithComponentsCallback(func(reg registry.ComponentRegistry) error {
			client.ComponentRegistry = reg
			return nil
		})
	})

	rt, rtConf, err := rtembedded.NewRuntime(ctx, s.appID, opts...)
	if err != nil {
		return err
	}
	s.gracefulShutdownDuration = time.Duration(rtConf.DaprGracefulShutdownSeconds) * time.Second

	client.rt = rt
	client.errCh = make(chan error)

	go func() {
		client.errCh <- rt.Run(ctx)
	}()

	ctx.Cleanup(func() {
		s.Stop(ctx)
	})

	daprClient, err := dapr.NewClientWithPort(fmt.Sprintf("%s", rtConf.DaprAPIGRPCPort))
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
		client.rt.ShutdownWithWait()
		if client.closed.CompareAndSwap(false, true) {
			if err := <-client.errCh; err != nil {
				logger.NewLogger("dapr.contrib").Errorf("error on shutdown: %s", err)
				return err
			}
		}
		return nil
	}

	return nil
}
