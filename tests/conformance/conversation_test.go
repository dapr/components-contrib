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

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/echo"
	conf_conversation "github.com/dapr/components-contrib/tests/conformance/conversation"
)

func TestConversationConformance(t *testing.T) {
	const configPath = "../config/conversation/"
	tc, err := NewTestConfiguration(filepath.Join(configPath, "tests.yml"))
	require.NoError(t, err)
	require.NotNil(t, tc)

	tc.TestFn = func(comp *TestComponent) func(t *testing.T) {
		return func(t *testing.T) {
			ParseConfigurationMap(t, comp.Config)

			componentConfigPath := convertComponentNameToPath(comp.Component, comp.Profile)
			props, err := loadComponentsAndProperties(t, filepath.Join(configPath, componentConfigPath))
			require.NoErrorf(t, err, "error running conformance test for component %s", comp.Component)

			conv := loadConversationComponent(comp.Component)
			require.NotNil(t, conv, "error running conformance test for component %s", comp.Component)

			conf_conversation.ConformanceTests(t, props, conv, comp.Component)
		}
	}

	tc.Run(t)
}

func loadConversationComponent(name string) conversation.Conversation {
	switch name {
	case "echo":
		return echo.NewEcho(testLogger)
	default:
		return nil
	}
}
