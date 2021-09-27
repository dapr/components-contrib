package dockercompose

import (
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow"
)

type up struct {
	flow.Task
	filename string
}

func Up(filename string) flow.Runnable {
	return &up{
		filename: filename,
	}
}

func (d *up) Run() error {
	return nil
}
