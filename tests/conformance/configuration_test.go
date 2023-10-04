//go:build conftests
// +build conftests

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

package conformance

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/configuration"
	c_postgres "github.com/dapr/components-contrib/configuration/postgres"
	c_redis "github.com/dapr/components-contrib/configuration/redis"
	conf_configuration "github.com/dapr/components-contrib/tests/conformance/configuration"
	"github.com/dapr/components-contrib/tests/utils/configupdater"
	cu_postgres "github.com/dapr/components-contrib/tests/utils/configupdater/postgres"
	cu_redis "github.com/dapr/components-contrib/tests/utils/configupdater/redis"
)

func TestConfigurationConformance(t *testing.T) {
	const configPath = "../config/configuration/"
	tc, err := NewTestConfiguration(filepath.Join(configPath, "tests.yml"))
	require.NoError(t, err)
	require.NotNil(t, tc)

	tc.TestFn = func(comp *TestComponent) func(t *testing.T) {
		return func(t *testing.T) {
			ParseConfigurationMap(t, comp.Config)

			componentConfigPath := convertComponentNameToPath(comp.Component, comp.Profile)
			props, err := loadComponentsAndProperties(t, filepath.Join(configPath, componentConfigPath))
			require.NoErrorf(t, err, "error running conformance test for component %s", comp.Component)

			store, updater := loadConfigurationStore(comp.Component)
			require.NotNil(t, store, "error running conformance test for component %s", comp.Component)
			require.NotNil(t, updater, "error running conformance test for component %s", comp.Component)

			configurationConfig := conf_configuration.NewTestConfig(comp.Component, comp.Operations, comp.Config)
			conf_configuration.ConformanceTests(t, props, store, updater, configurationConfig, comp.Component)
		}
	}
}

func loadConfigurationStore(name string) (configuration.Store, configupdater.Updater) {
	switch name {
	case "redis.v6", "redis.v7":
		return c_redis.NewRedisConfigurationStore(testLogger),
			cu_redis.NewRedisConfigUpdater(testLogger)
	case "postgresql.docker", "postgresql.azure":
		return c_postgres.NewPostgresConfigurationStore(testLogger),
			cu_postgres.NewPostgresConfigUpdater(testLogger)
	default:
		return nil, nil
	}
}
