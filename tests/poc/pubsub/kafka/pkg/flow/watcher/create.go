package watcher

import (
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow"
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/harness"
)

func Create(names ...string) flow.Runnable {
	return func(ctx flow.Context) error {
		for _, name := range names {
			watcher := harness.NewWatcher()
			ctx.Set(name, watcher)
		}

		return nil
	}
}
