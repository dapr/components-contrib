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

package cron_test

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/cron"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"

	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/kit/logger"

	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/stretchr/testify/require"

	daprClient "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"

	// Certification testing runnables
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
)

func TestCronBinding(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	// Test Frequent Trigger
	application := func(ctx flow.Context, s common.Service) error {
		// For cron component with trigger @every 1s, check if the app is invoked 10 times within 10 seconds
		pending := 10
		cronContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		// Setup the input binding endpoint
		err := s.AddBindingInvocationHandler("cron", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
			ctx.Logf("Cron triggered at %s", time.Now().String())
			log.Printf("Cron triggered at %s", time.Now().String())
			pending--
			select {
			case <-cronContext.Done():
				if pending > 0 {
					require.NoError(t, cronContext.Err(), "Cron failed to trigger within deadline.")
					cancel()
					return nil, cronContext.Err()
				}
			default:
			}
			return []byte("{}"), nil
		})
		require.NoError(t, err)
		return nil
	}

	testDelete := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, "cron-sidecar")

		req := &daprClient.InvokeBindingRequest{Name: "cron", Operation: "delete"}
		res, err := client.InvokeBinding(ctx, req)

		require.Nil(t, res.Data, "Error cancelling cron schedule")
		require.NoError(t, err, "Error cancelling cron schedule")
		return nil
	}

	flow.New(t, "test cron trigger schedule @every 1s").
		Step(app.Run("cronapp", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("cron-sidecar",
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(),
		)).
		Step("wait for cron to trigger", flow.Sleep(time.Second*10)).
		Step("check schedule cancel", testDelete).
		Step("stop sidecar", sidecar.Stop("cron-sidecar")).
		Step("stop app", app.Stop("cronapp")).
		Run()

	// Test app restart
	pending := 3
	cronContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	application3s := func(ctx flow.Context, s common.Service) error {
		// For cron component with trigger @every 3s, check if the app is invoked correctly after app restart

		// Setup the input binding endpoint
		err := s.AddBindingInvocationHandler("cron3s", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
			ctx.Logf("Cron triggered at %s", time.Now().String())
			log.Printf("Cron triggered at %s", time.Now().String())
			pending--
			select {
			case <-cronContext.Done():
				if pending > 0 {
					require.NoError(t, cronContext.Err(), "Cron failed to trigger within deadline.")
					cancel()
					return nil, cronContext.Err()
				}
			default:
			}
			return []byte("{}"), nil
		})
		require.NoError(t, err)
		return nil
	}

	flow.New(t, "test cron trigger schedule @every 3s").
		Step(app.Run("cronapp3s", fmt.Sprintf(":%d", appPort), application3s)).
		Step(sidecar.Run("cron-sidecar3s",
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(),
		)).
		Step("start cron trigger", flow.Sleep(time.Second*4)).
		Step("stop app", app.Stop("cronapp3s")).
		Step("wait before app restart", flow.Sleep(time.Second*5)).
		Step(app.Run("cronapp3s", fmt.Sprintf(":%d", appPort), application3s)).
		Step("wait for cron trigger", flow.Sleep(time.Second*6)).
		Step("stop sidecar", sidecar.Stop("cron-sidecar3s")).
		Step("stop app", app.Stop("cronapp3s")).
		Run()

	// Test sidecar restart
	pending = 3
	cronContext, cancel = context.WithTimeout(context.Background(), 15*time.Second)
	application3s = func(ctx flow.Context, s common.Service) error {
		// For cron component with trigger @every 3s, check if the app is invoked correctly after sidecar restart

		// Setup the input binding endpoint
		err := s.AddBindingInvocationHandler("cron3s", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
			ctx.Logf("Cron triggered at %s", time.Now().String())
			log.Printf("Cron triggered at %s", time.Now().String())
			pending--
			select {
			case <-cronContext.Done():
				if pending > 0 {
					require.NoError(t, cronContext.Err(), "Cron failed to trigger within deadline.")
					cancel()
					return nil, cronContext.Err()
				}
			default:
			}
			return []byte("{}"), nil
		})
		require.NoError(t, err)
		return nil
	}

	flow.New(t, "test cron trigger schedule @every 3s with sidecar restart").
		Step(app.Run("cronapp3s", fmt.Sprintf(":%d", appPort), application3s)).
		Step(sidecar.Run("cron-sidecar3s",
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(),
		)).
		Step("wait for cron trigger", flow.Sleep(time.Second*4)).
		Step("stop sidecar", sidecar.Stop("cron-sidecar3s")).
		Step("wait before sidecar restart", flow.Sleep(time.Second*5)).
		Step(sidecar.Run("cron-sidecar3s",
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(),
		)).
		Step("wait for cron trigger", flow.Sleep(time.Second*6)).
		// Step("stop sidecar", sidecar.Stop("cron-sidecar3s")).
		Step("stop app", app.Stop("cronapp3s")).
		Run()
}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	bindingsRegistry := bindings_loader.NewRegistry()
	bindingsRegistry.Logger = log
	bindingsRegistry.RegisterInputBinding(func(l logger.Logger) bindings.InputBinding {
		return cron.NewCron(l)
	}, "cron")
	bindingsRegistry.RegisterOutputBinding(func(l logger.Logger) bindings.OutputBinding {
		return cron.NewCron(l)
	}, "cron")

	return []runtime.Option{
		runtime.WithBindings(bindingsRegistry),
	}
}
