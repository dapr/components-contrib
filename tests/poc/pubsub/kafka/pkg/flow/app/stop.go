package app

import (
	"github.com/dapr/go-sdk/service/common"

	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow"
)

type stop struct {
	flow.Task
	appName string
}

func Stop(appName string) *stop {
	return &stop{
		appName: appName,
	}
}

func (c *stop) Run() error {
	var s common.Service
	c.MustGet(c.appName, &s)

	return s.Stop()
}
