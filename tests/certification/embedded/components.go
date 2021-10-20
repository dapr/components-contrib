// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

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
