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

package embedded

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dapr/dapr/pkg/components/bindings"
	"github.com/dapr/dapr/pkg/components/configuration"
	"github.com/dapr/dapr/pkg/components/middleware/http"
	"github.com/dapr/dapr/pkg/components/pubsub"
	"github.com/dapr/dapr/pkg/components/secretstores"
	"github.com/dapr/dapr/pkg/components/state"
	env "github.com/dapr/dapr/pkg/config/env"
	"github.com/dapr/dapr/pkg/config/protocol"
	"github.com/dapr/dapr/pkg/cors"
	"github.com/dapr/dapr/pkg/healthz"
	"github.com/dapr/dapr/pkg/metrics"
	"github.com/dapr/dapr/pkg/modes"
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/dapr/pkg/runtime/registry"
	"github.com/dapr/dapr/pkg/security/fake"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
	"github.com/phayes/freeport"
)

const (
	placementAddresses  = "127.0.0.1"
	controlPlaneAddress = ""
	allowedOrigins      = cors.DefaultAllowedOrigins
	mode                = modes.StandaloneMode
	config              = "config.yaml"
	componentsPath      = "./components"
	profilePort         = runtime.DefaultProfilePort
	enableProfiling     = true
	maxConcurrency      = -1
	enableMTLS          = false
	sentryAddress       = ""
	maxRequestBodySize  = 4 << 20

	daprHTTPPort     = runtime.DefaultDaprHTTPPort
	daprAPIGRPCPort  = runtime.DefaultDaprAPIGRPCPort
	daprInternalGRPC = 0 // use ephemeral port
	appPort          = 8000
)

var log = logger.NewLogger("dapr.runtime")

type Option func(config *runtime.Config)

func WithAppProtocol(protocol protocol.Protocol, port string) Option {
	return func(config *runtime.Config) {
		config.AppProtocol = string(protocol)
		config.ApplicationPort = port
	}
}

func WithoutApp() Option {
	return func(config *runtime.Config) {
		config.ApplicationPort = "0"
	}
}

func WithDaprHTTPPort(port string) Option {
	return func(config *runtime.Config) {
		config.DaprHTTPPort = port
	}
}

func WithDaprGRPCPort(port string) Option {
	return func(config *runtime.Config) {
		config.DaprAPIGRPCPort = port
	}
}

func WithDaprInternalGRPCPort(port string) Option {
	return func(config *runtime.Config) {
		config.DaprInternalGRPCPort = port
	}
}

func WithListenAddresses(addresses []string) Option {
	return func(config *runtime.Config) {
		config.DaprAPIListenAddresses = strings.Join(addresses, ",")
	}
}

func WithResourcesPath(path string) Option {
	return func(config *runtime.Config) {
		config.ResourcesPath = []string{path}
	}
}

// Deprecated: use WithResourcesPath.
func WithComponentsPath(path string) Option {
	return WithResourcesPath(path)
}

func WithProfilePort(port string) Option {
	return func(config *runtime.Config) {
		config.ProfilePort = port
	}
}

func WithGracefulShutdownDuration(d time.Duration) Option {
	return func(config *runtime.Config) {
		config.DaprGracefulShutdownSeconds = int(d.Seconds())
	}
}

func WithAPILoggingEnabled(enabled bool) Option {
	return func(config *runtime.Config) {
		config.EnableAPILogging = &enabled
	}
}

func WithProfilingEnabled(enabled bool) Option {
	return func(config *runtime.Config) {
		config.EnableProfiling = enabled
	}
}

func WithStates(reg *state.Registry) Option {
	return func(config *runtime.Config) {
		config.Registry = config.Registry.WithStateStores(reg)
	}
}

func WithSecretStores(reg *secretstores.Registry) Option {
	return func(config *runtime.Config) {
		config.Registry = config.Registry.WithSecretStores(reg)
	}
}

func WithPubSubs(reg *pubsub.Registry) Option {
	return func(config *runtime.Config) {
		config.Registry = config.Registry.WithPubSubs(reg)
	}
}

func WithConfigurations(reg *configuration.Registry) Option {
	return func(config *runtime.Config) {
		config.Registry = config.Registry.WithConfigurations(reg)
	}
}

func WithHTTPMiddlewares(reg *http.Registry) Option {
	return func(config *runtime.Config) {
		config.Registry = config.Registry.WithHTTPMiddlewares(reg)
	}
}

func WithBindings(reg *bindings.Registry) Option {
	return func(config *runtime.Config) {
		config.Registry = config.Registry.WithBindings(reg)
	}
}

func NewRuntime(ctx context.Context, appID string, opts ...Option) (*runtime.DaprRuntime, *runtime.Config, error) {
	var err error
	metricsOpts := metrics.DefaultFlagOptions().ToOptions(healthz.New())
	metricsOpts.Port = "0"
	metricsOpts.Log = log

	runtimeConfig := &runtime.Config{
		AppID:                        appID,
		DaprHTTPPort:                 strconv.Itoa(daprHTTPPort),
		DaprInternalGRPCPort:         strconv.Itoa(daprInternalGRPC),
		DaprAPIGRPCPort:              strconv.Itoa(daprAPIGRPCPort),
		ApplicationPort:              strconv.Itoa(appPort),
		ProfilePort:                  strconv.Itoa(profilePort),
		DaprAPIListenAddresses:       "127.0.0.1",
		AppProtocol:                  string(protocol.HTTPProtocol),
		Mode:                         string(mode),
		ActorsService:                "",
		Healthz:                      healthz.New(),
		RemindersService:             "",
		AllowedOrigins:               allowedOrigins,
		ResourcesPath:                []string{componentsPath},
		EnableProfiling:              enableProfiling,
		AppMaxConcurrency:            maxConcurrency,
		EnableMTLS:                   enableMTLS,
		SentryAddress:                sentryAddress,
		MaxRequestSize:               maxRequestBodySize,
		ReadBufferSize:               runtime.DefaultReadBufferSize,
		DaprGracefulShutdownSeconds:  1,
		EnableAPILogging:             ptr.Of(true),
		DisableBuiltinK8sSecretStore: false,
		Registry:                     registry.NewOptions(),
		Config:                       []string{"config.yaml"},
		Metrics:                      metricsOpts,
		Security:                     fake.New(),
	}

	for _, opt := range opts {
		opt(runtimeConfig)
	}

	if runtimeConfig.DaprInternalGRPCPort == "0" {
		port, err := freeport.GetFreePort()
		if err != nil {
			return nil, nil, err
		}
		runtimeConfig.DaprInternalGRPCPort = strconv.Itoa(port)
	}

	variables := map[string]string{
		env.AppID:           runtimeConfig.AppID,
		env.AppPort:         runtimeConfig.ApplicationPort,
		env.HostAddress:     "127.0.0.1",
		env.DaprPort:        runtimeConfig.DaprInternalGRPCPort,
		env.DaprGRPCPort:    runtimeConfig.DaprAPIGRPCPort,
		env.DaprHTTPPort:    runtimeConfig.DaprHTTPPort,
		env.DaprProfilePort: runtimeConfig.ProfilePort,
	}

	for key, value := range variables {
		err := os.Setenv(key, value)
		if err != nil {
			return nil, nil, err
		}
	}

	rt, err := runtime.FromConfig(ctx, runtimeConfig)
	if err != nil {
		return nil, nil, err
	}

	return rt, runtimeConfig, nil
}
