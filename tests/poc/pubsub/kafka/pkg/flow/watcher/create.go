package watcher

import (
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow"
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/harness"
)

type create struct {
	flow.Task
	names []string
}

func Create(names ...string) *create {
	return &create{
		names: names,
	}
}

func (c *create) Run() error {
	for _, name := range c.names {
		watcher := harness.NewWatcher()
		c.Set(name, watcher)
	}

	return nil
}
