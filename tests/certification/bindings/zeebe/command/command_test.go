/*
Copyright 2023 The Dapr Authors
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

package command_test

import (
	"github.com/dapr/components-contrib/bindings"
	bindings_zeebe_command "github.com/dapr/components-contrib/bindings/zeebe/command"
	bindings_zeebe_jobworker "github.com/dapr/components-contrib/bindings/zeebe/jobworker"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	"github.com/dapr/kit/logger"
)

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	bindingsRegistry := bindings_loader.NewRegistry()
	bindingsRegistry.Logger = log
	bindingsRegistry.RegisterInputBinding(func(l logger.Logger) bindings.InputBinding {
		return bindings_zeebe_jobworker.NewZeebeJobWorker(l)
	}, "zeebe.jobworker")
	bindingsRegistry.RegisterOutputBinding(func(l logger.Logger) bindings.OutputBinding {
		return bindings_zeebe_command.NewZeebeCommand(l)
	}, "zeebe.command")

	return []embedded.Option{
		embedded.WithBindings(bindingsRegistry),
	}
}
