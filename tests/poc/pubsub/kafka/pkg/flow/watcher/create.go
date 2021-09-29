package watcher

import (
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow"
)

func Create(names ...string) flow.Runnable {
	return func(ctx flow.Context) error {
		for _, name := range names {
			watcher := New()
			ctx.Set(name, watcher)
		}

		return nil
	}
}
