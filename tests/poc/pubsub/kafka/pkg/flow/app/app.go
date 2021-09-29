package app

import (
	"log"
	"net/http"

	"github.com/dapr/go-sdk/service/common"

	daprd "github.com/dapr/go-sdk/service/http"

	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow"
)

func Start(appName, address string, setup func(flow.Context, common.Service) error) flow.Runnable {
	return func(ctx flow.Context) error {
		s := daprd.NewService(":8000")

		if err := setup(ctx, s); err != nil {
			return err
		}

		go func() {
			if err := s.Start(); err != nil && err != http.ErrServerClosed {
				log.Printf("error listenning: %v", err)
			}
		}()

		ctx.Set(appName, s)

		return nil
	}
}

func Stop(appName string) flow.Runnable {
	return func(ctx flow.Context) error {
		var s common.Service
		if ctx.Get(appName, &s) {
			return s.Stop()
		}

		return nil
	}
}
