package redis_native

import (
	"context"
	"fmt"
	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/kit/logger"
	"os"
	"testing"
)

var store = NewRedisConfigurationStore(logger.NewLogger("redis.logger"))

func TestMain(m *testing.M) {
	meta := configuration.Metadata{
		Name: "test",
		Properties: map[string]string{
			"redisHost":     "localhost:6379",
			"redisPassword": "",
		},
	}
	err := store.Init(meta)
	if err != nil {
		fmt.Printf("fail to init redis store: %s", err.Error())
		os.Exit(-1)
	}

	code := m.Run()
	os.Exit(code)
}

func TestSubscribe(t *testing.T) {
	if err := store.Subscribe(context.TODO(), &configuration.SubscribeRequest{
		Keys:     []string{"mykey"},
		Metadata: make(map[string]string),
	}, func(ctx context.Context, e *configuration.UpdateEvent) error {
		for _, v := range e.Items {
			fmt.Printf("event is %+v\n", *v)
		}
		return nil
	}); err != nil {
		panic(err)
	}
	select {}
}
