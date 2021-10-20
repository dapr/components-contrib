// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package embedded

import (
	"fmt"
	"os"

	"github.com/dapr/dapr/pkg/acl"
	global_config "github.com/dapr/dapr/pkg/config"
	env "github.com/dapr/dapr/pkg/config/env"
	"github.com/dapr/dapr/pkg/cors"
	"github.com/dapr/dapr/pkg/modes"
	"github.com/dapr/dapr/pkg/operator/client"
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/dapr/pkg/runtime/security"
	"github.com/dapr/kit/logger"
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
	appSSL              = false
	maxRequestBodySize  = 4

	daprHTTPPort     = runtime.DefaultDaprHTTPPort
	daprAPIGRPCPort  = runtime.DefaultDaprAPIGRPCPort
	daprInternalGRPC = runtime.DefaultDaprAPIGRPCPort + 1
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

func NewRuntime(appID string, opts ...Option) (*runtime.DaprRuntime, error) {
	var err error

	runtimeConfig := runtime.NewRuntimeConfig(
		appID, []string{}, controlPlaneAddress,
		allowedOrigins, config, componentsPath, string(runtime.HTTPProtocol), string(mode),
		daprHTTPPort, daprInternalGRPC, daprAPIGRPCPort, []string{"127.0.0.1"}, nil, appPort, profilePort,
		enableProfiling, maxConcurrency, enableMTLS, sentryAddress, appSSL, maxRequestBodySize, "",
		runtime.DefaultReadBufferSize, false)

	for _, opt := range opts {
		opt(runtimeConfig)
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
			return nil, err
		}
	}

	var globalConfig *global_config.Configuration
	var configErr error

	if enableMTLS {
		if runtimeConfig.CertChain, err = security.GetCertChain(); err != nil {
			return nil, err
		}
	}

	var accessControlList *global_config.AccessControlList
	var namespace string

	if config != "" {
		switch modes.DaprMode(mode) {
		case modes.KubernetesMode:
			client, conn, clientErr := client.GetOperatorClient(controlPlaneAddress, security.TLSServerName, runtimeConfig.CertChain)
			if clientErr != nil {
				return nil, err
			}
			defer conn.Close()
			namespace = os.Getenv("NAMESPACE")
			globalConfig, configErr = global_config.LoadKubernetesConfiguration(config, namespace, client)
		case modes.StandaloneMode:
			globalConfig, _, configErr = global_config.LoadStandaloneConfiguration(config)
		}

		if configErr != nil {
			log.Debugf("Config error: %v", configErr)
		}
	}

	if configErr != nil {
		return nil, fmt.Errorf("error loading configuration: %w", configErr)
	}
	if globalConfig == nil {
		log.Info("loading default configuration")
		globalConfig = global_config.LoadDefaultConfiguration()
	}

	accessControlList, err = acl.ParseAccessControlSpec(globalConfig.Spec.AccessControlSpec, string(runtimeConfig.ApplicationProtocol))
	if err != nil {
		return nil, err
	}

	return runtime.NewDaprRuntime(runtimeConfig, globalConfig, accessControlList), nil
}
