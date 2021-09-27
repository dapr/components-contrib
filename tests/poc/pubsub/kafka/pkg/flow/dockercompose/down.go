package dockercompose

import (
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow"
)

type down struct {
	flow.Task
	filename string
}

func Down(filename string) flow.Runnable {
	return &down{
		filename: filename,
	}
}

func (d *down) Run() error {
	return nil
}
