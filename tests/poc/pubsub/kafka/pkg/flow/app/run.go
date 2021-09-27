package app

import (
	"log"
	"net/http"

	"github.com/dapr/go-sdk/service/common"

	daprd "github.com/dapr/go-sdk/service/http"

	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow"
)

type run struct {
	flow.Task
	appName string
	address string
	setup   func(flow.Context, common.Service) error
}

func Run(appName, address string, setup func(flow.Context, common.Service) error) *run {
	return &run{
		appName: appName,
		address: address,
		setup:   setup,
	}
}

func (c *run) Run() error {
	s := daprd.NewService(":8000")

	if err := c.setup(c.GetContext(), s); err != nil {
		return err
	}

	go func() {
		if err := s.Start(); err != nil && err != http.ErrServerClosed {
			log.Printf("error listenning: %v", err)
		}
	}()

	c.Set(c.appName, s)

	return nil
}
