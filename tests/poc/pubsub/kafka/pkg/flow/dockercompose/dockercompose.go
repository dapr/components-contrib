package dockercompose

import (
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow"
)

func Up(filename string) flow.Runnable {
	return func(ctx flow.Context) error {
		return nil
	}
}

func Down(filename string) flow.Runnable {
	return func(ctx flow.Context) error {
		return nil
	}
}
