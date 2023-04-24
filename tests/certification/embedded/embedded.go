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
	"fmt"
	"os"
	"time"

	"github.com/dapr/dapr/pkg/acl"
	global_config "github.com/dapr/dapr/pkg/config"
	env "github.com/dapr/dapr/pkg/config/env"
	"github.com/dapr/dapr/pkg/cors"
	"github.com/dapr/dapr/pkg/modes"
	"github.com/dapr/dapr/pkg/operator/client"
	"github.com/dapr/dapr/pkg/resiliency"
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/dapr/pkg/runtime/security"
	"github.com/dapr/kit/logger"
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
	maxRequestBodySize  = 4

	daprHTTPPort     = runtime.DefaultDaprHTTPPort
	daprAPIGRPCPort  = runtime.DefaultDaprAPIGRPCPort
	daprInternalGRPC = 0 // use ephemeral port
	appPort          = 8000
)

var log = logger.NewLogger("dapr.runtime")

type Option func(config *runtime.Config)

func WithAppProtocol(protocol runtime.Protocol, port int) Option {
	return func(config *runtime.Config) {
		config.ApplicationProtocol = protocol
		config.ApplicationPort = port
	}
}

func WithoutApp() Option {
	return func(config *runtime.Config) {
		config.ApplicationPort = 0
	}
}

func WithDaprHTTPPort(port int) Option {
	return func(config *runtime.Config) {
		config.HTTPPort = port
	}
}

func WithDaprGRPCPort(port int) Option {
	return func(config *runtime.Config) {
		config.APIGRPCPort = port
	}
}

func WithDaprInternalGRPCPort(port int) Option {
	return func(config *runtime.Config) {
		config.InternalGRPCPort = port
	}
}

func WithListenAddresses(addresses []string) Option {
	return func(config *runtime.Config) {
		config.APIListenAddresses = addresses
	}
}

func WithResourcesPath(path string) Option {
	return func(config *runtime.Config) {
		config.Standalone.ResourcesPath[0] = path
	}
}

// Deprecated: use WithResourcesPath.
func WithComponentsPath(path string) Option {
	return WithResourcesPath(path)
}

func WithProfilePort(port int) Option {
	return func(config *runtime.Config) {
		config.ProfilePort = port
	}
}

func WithGracefulShutdownDuration(d time.Duration) Option {
	return func(config *runtime.Config) {
		config.GracefulShutdownDuration = d
	}
}

func WithAPILoggingEnabled(enabled bool) Option {
	return func(config *runtime.Config) {
		config.EnableAPILogging = enabled
	}
}

func WithProfilingEnabled(enabled bool) Option {
	return func(config *runtime.Config) {
		config.EnableProfiling = enabled
	}
}

func NewRuntime(appID string, opts ...Option) (*runtime.DaprRuntime, *runtime.Config, error) {
	var err error
	runtimeConfig := runtime.NewRuntimeConfig(runtime.NewRuntimeConfigOpts{
		ID:                           appID,
		HTTPPort:                     daprHTTPPort,
		InternalGRPCPort:             daprInternalGRPC,
		APIGRPCPort:                  daprAPIGRPCPort,
		AppPort:                      appPort,
		ProfilePort:                  profilePort,
		APIListenAddresses:           []string{"127.0.0.1"},
		AppProtocol:                  string(runtime.HTTPProtocol),
		Mode:                         string(mode),
		PlacementAddresses:           []string{},
		AllowedOrigins:               allowedOrigins,
		ResourcesPath:                []string{componentsPath},
		EnableProfiling:              enableProfiling,
		MaxConcurrency:               maxConcurrency,
		MTLSEnabled:                  enableMTLS,
		SentryAddress:                sentryAddress,
		MaxRequestBodySize:           maxRequestBodySize,
		ReadBufferSize:               runtime.DefaultReadBufferSize,
		GracefulShutdownDuration:     time.Second,
		EnableAPILogging:             true,
		DisableBuiltinK8sSecretStore: false,
	})

	for _, opt := range opts {
		opt(runtimeConfig)
	}

	if runtimeConfig.InternalGRPCPort == 0 {
		if runtimeConfig.InternalGRPCPort, err = freeport.GetFreePort(); err != nil {
			return nil, nil, err
		}
	}

	variables := map[string]string{
		env.AppID:           runtimeConfig.ID,
		env.AppPort:         fmt.Sprintf("%d", runtimeConfig.ApplicationPort),
		env.HostAddress:     "127.0.0.1",
		env.DaprPort:        fmt.Sprintf("%d", runtimeConfig.InternalGRPCPort),
		env.DaprGRPCPort:    fmt.Sprintf("%d", runtimeConfig.APIGRPCPort),
		env.DaprHTTPPort:    fmt.Sprintf("%d", runtimeConfig.HTTPPort),
		env.DaprProfilePort: fmt.Sprintf("%d", runtimeConfig.ProfilePort),
	}

	for key, value := range variables {
		err := os.Setenv(key, value)
		if err != nil {
			return nil, nil, err
		}
	}

	var globalConfig *global_config.Configuration
	var configErr error

	if enableMTLS {
		if runtimeConfig.CertChain, err = security.GetCertChain(); err != nil {
			return nil, nil, err
		}
	}

	var accessControlList *global_config.AccessControlList
	var namespace string

	if config != "" {
		switch modes.DaprMode(mode) {
		case modes.KubernetesMode:
			client, conn, clientErr := client.GetOperatorClient(context.Background(), controlPlaneAddress, security.TLSServerName, runtimeConfig.CertChain)
			if clientErr != nil {
				return nil, nil, err
			}
			defer conn.Close()
			namespace = os.Getenv("NAMESPACE")
			podName := os.Getenv("POD_NAME")
			globalConfig, configErr = global_config.LoadKubernetesConfiguration(config, namespace, podName, client)
		case modes.StandaloneMode:
			globalConfig, _, configErr = global_config.LoadStandaloneConfiguration(config)
		}

		if configErr != nil {
			log.Debugf("Config error: %v", configErr)
		}
	}

	if configErr != nil {
		return nil, nil, fmt.Errorf("error loading configuration: %w", configErr)
	}
	if globalConfig == nil {
		log.Info("loading default configuration")
		globalConfig = global_config.LoadDefaultConfiguration()
	}

	accessControlList, err = acl.ParseAccessControlSpec(globalConfig.Spec.AccessControlSpec, true)
	if err != nil {
		return nil, nil, err
	}

	return runtime.NewDaprRuntime(runtimeConfig, globalConfig, accessControlList, &resiliency.NoOp{}), runtimeConfig, nil
}
