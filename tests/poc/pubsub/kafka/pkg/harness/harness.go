package harness

import (
	"context"
	"net/http"
	"time"

	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/dapr/pkg/runtime/embedded"
	"github.com/dapr/kit/logger"

	// Go SDK
	dapr "github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/client"
	"github.com/dapr/go-sdk/service/common"
	daprd "github.com/dapr/go-sdk/service/http"

	rtembedded "github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/embedded"
)

type Test interface {
	RegisterComponents(components embedded.ComponentRegistry) error
	SetLogger(log logger.Logger)
	Options(log logger.Logger) []runtime.Option
	SetService(service common.Service)
	SetClient(client dapr.Client)
	Setup(ctx context.Context) error
	Run(ctx context.Context) error
}

type Base struct {
	logger.Logger
	embedded.ComponentRegistry
	common.Service
	dapr.Client
}

func (b *Base) RegisterComponents(components embedded.ComponentRegistry) error {
	b.ComponentRegistry = components
	return nil
}

func (b *Base) Options() []runtime.Option {
	return []runtime.Option{}
}

func (b *Base) SetLogger(log logger.Logger) {
	b.Logger = log
}

func (b *Base) SetService(service common.Service) {
	b.Service = service
}

func (b *Base) SetClient(client dapr.Client) {
	b.Client = client
}

func (b *Base) Setup(ctx context.Context) error {
	return nil
}

func Run(test Test) {
	log := logger.NewLogger("test")
	logContrib := logger.NewLogger("dapr.contrib")

	test.SetLogger(log)
	rt, err := rtembedded.NewRuntime("kafka-test")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	go func() {
		s := daprd.NewService(":8000")

		test.SetService(s)
		test.Setup(ctx)

		if err := s.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("error listenning: %v", err)
		}
	}()

	opts := []runtime.Option{}
	opts = append(opts, rtembedded.CommonComponents(logContrib)...)
	opts = append(opts, test.Options(logContrib)...)
	opts = append(opts, runtime.WithComponentsCallback(test.RegisterComponents))

	if err = rt.Run(opts...); err != nil {
		log.Fatal(err)
	}

	client, err := dapr.NewClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	test.SetClient(client)

	if err := test.Run(ctx); err != nil {
		log.Error(err)
	} else {
		log.Info("Test passed!")
	}

	log.Info("Shutting down...")
	rt.Shutdown(2 * time.Second)
	rt.WaitUntilShutdown()
}
