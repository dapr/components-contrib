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
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/kit/logger"

	// Name resolutions.
	nr "github.com/dapr/components-contrib/nameresolution"
	nr_consul "github.com/dapr/components-contrib/nameresolution/consul"
	nr_kubernetes "github.com/dapr/components-contrib/nameresolution/kubernetes"
	nr_mdns "github.com/dapr/components-contrib/nameresolution/mdns"

	nr_loader "github.com/dapr/dapr/pkg/components/nameresolution"
)

func CommonComponents(log logger.Logger) []runtime.Option {
	return []runtime.Option{
		runtime.WithNameResolutions(
			nr_loader.New("mdns", func() nr.Resolver {
				return nr_mdns.NewResolver(log)
			}),
			nr_loader.New("kubernetes", func() nr.Resolver {
				return nr_kubernetes.NewResolver(log)
			}),
			nr_loader.New("consul", func() nr.Resolver {
				return nr_consul.NewResolver(log)
			}),
		),
	}
}
