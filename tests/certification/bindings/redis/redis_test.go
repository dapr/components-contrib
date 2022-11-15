package redisbinding_test

import (
	"fmt"
	"github.com/dapr/components-contrib/bindings"
	bindingRedis "github.com/dapr/components-contrib/bindings/redis"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	bindingsLoader "github.com/dapr/dapr/pkg/components/bindings"
	"github.com/dapr/dapr/pkg/runtime"
	daprTesting "github.com/dapr/dapr/pkg/testing"
	daprClient "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const (
	sidecarName       = "redisBindingSidecar"
	bindingName       = "redisBinding"
	dockerComposeYAML = "docker-compose.yml"
	dataInserted      = "Hello: World!"
	keyName           = "customKey"
)

func TestRedisBinding(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	ports, _ := daprTesting.GetFreePorts(2)
	grpcPort := ports[0]
	httpPort := ports[1]

	testInvokeCreate := func(ctx flow.Context) error {
		client, clientErr := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		invokeRequest := &daprClient.InvokeBindingRequest{
			Name:      bindingName,
			Operation: string(bindings.CreateOperation),
			Data:      []byte(dataInserted),
			Metadata:  map[string]string{"key": keyName},
		}

		err := client.InvokeOutputBinding(ctx, invokeRequest)
		assert.NoError(t, err)

		rdb := redis.NewClient(&redis.Options{
			Addr:     "localhost:6379", // host:port of the redis server
			Password: "",               // no password set
			DB:       0,                // use default DB
		})

		val, err := rdb.Get(ctx, keyName).Result()
		assert.NoError(t, err)
		assert.Equal(t, dataInserted, val)

		err = rdb.Close()
		if err != nil {
			return err
		}
		return nil
	}

	checkRedisConnection := func(ctx flow.Context) error {
		rdb := redis.NewClient(&redis.Options{
			Addr:     "localhost:6379", // host:port of the redis server
			Password: "",               // no password set
			DB:       0,                // use default DB
		})

		if err := rdb.Ping(ctx).Err(); err != nil {
			return nil
		} else {
			log.Info("Setup for Redis done")
		}

		err := rdb.Close()
		if err != nil {
			return err
		}
		return nil
	}

	testCheckInsertedData := func(ctx flow.Context) error {
		rdb := redis.NewClient(&redis.Options{
			Addr:     "localhost:6379", // host:port of the redis server
			Password: "",               // no password set
			DB:       0,                // use default DB
		})

		val, err := rdb.Get(ctx, keyName).Result()
		assert.NoError(t, err)
		assert.Equal(t, dataInserted, val)

		err = rdb.Close()
		if err != nil {
			return err
		}
		return nil
	}

	flow.New(t, "Test Redis Output Binding CREATE operation").
		Step(dockercompose.Run("redis", dockerComposeYAML)).
		Step("Waiting for Redis Readiness...", retry.Do(time.Second*3, 10, checkRedisConnection)).
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("components/standard"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(),
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Insert data into the redis data store and check if the insertion was successful", testInvokeCreate).
		Run()

	flow.New(t, "Test Redis Output Binding after reconnecting to network").
		Step(dockercompose.Run("redis", dockerComposeYAML)).
		Step("Waiting for Redis Readiness...", retry.Do(time.Second*3, 10, checkRedisConnection)).
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("components/standard"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(),
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Insert data into the redis data store before network interruption", testInvokeCreate).
		Step("Interrupt network", network.InterruptNetwork(5*time.Second, nil, nil, "6379:6379")).
		Step("Wait for the network to come back up", flow.Sleep(5*time.Second)).
		Step("Check if the data is accessible after the network is up again", testCheckInsertedData).
		Step("Stop Redis server", dockercompose.Stop("redis", dockerComposeYAML)).
		Step("Start Redis server", dockercompose.Start("redis", dockerComposeYAML)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Check if the data is accessible after server is up again", testCheckInsertedData).
		Run()

	flow.New(t, "Test Redis Output Binding with retryOptions after restarting Redis").
		Step(dockercompose.Run("redis", dockerComposeYAML)).
		Step("Waiting for Redis Readiness...", retry.Do(time.Second*3, 10, checkRedisConnection)).
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("components/retryOptions"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(),
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Stop Redis server", dockercompose.Stop("redis", dockerComposeYAML)).
		Step("Start Redis server", dockercompose.Start("redis", dockerComposeYAML)).
		//After restarting Redis, it usually takes a couple of seconds for the container to start but
		//since we have retry strategies and connection timeouts configured, the client will retry if it is
		//not able to establish a connection to the Redis server
		Step("Insert data into the redis data store during the server restart", testInvokeCreate).
		Step("Check if the data is accessible after the server is up again", testCheckInsertedData).
		Run()
}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	bindingsRegistry := bindingsLoader.NewRegistry()
	bindingsRegistry.Logger = log
	bindingsRegistry.RegisterOutputBinding(func(l logger.Logger) bindings.OutputBinding {
		return bindingRedis.NewRedis(l)
	}, "redis")

	return []runtime.Option{
		runtime.WithBindings(bindingsRegistry),
	}
}
