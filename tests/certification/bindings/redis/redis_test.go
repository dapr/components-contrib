package redisbinding_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	bindingRedis "github.com/dapr/components-contrib/bindings/redis"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	bindingsLoader "github.com/dapr/dapr/pkg/components/bindings"
	daprTesting "github.com/dapr/dapr/pkg/testing"
	daprClient "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
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
			Addr:     "localhost:6399", // host:port of the redis server
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

	testInvokeCreateIncr := func(ctx flow.Context) error {
		client, clientErr := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		invokeRequest := &daprClient.InvokeBindingRequest{
			Name:      bindingName,
			Operation: string(bindings.CreateOperation),
			Data:      []byte("hello"),
			Metadata:  map[string]string{"key": "expireKey", "ttlInSeconds": "2"},
		}

		err := client.InvokeOutputBinding(ctx, invokeRequest)
		assert.NoError(t, err)

		invokeRequest = &daprClient.InvokeBindingRequest{
			Name:      bindingName,
			Operation: string(bindings.CreateOperation),
			Data:      []byte("41"),
			Metadata:  map[string]string{"key": "incKey"},
		}

		err = client.InvokeOutputBinding(ctx, invokeRequest)
		assert.NoError(t, err)

		invokeRequest = &daprClient.InvokeBindingRequest{
			Name:      bindingName,
			Operation: "increment",
			Metadata:  map[string]string{"key": "incKey", "ttlInSeconds": "2"},
		}

		err = client.InvokeOutputBinding(ctx, invokeRequest)
		assert.NoError(t, err)

		invokeRequest = &daprClient.InvokeBindingRequest{
			Name:      bindingName,
			Operation: string(bindings.GetOperation),
			Metadata:  map[string]string{"key": "incKey"},
		}

		out, err2 := client.InvokeBinding(ctx, invokeRequest)
		assert.NoError(t, err2)
		assert.Equal(t, "42", string(out.Data))

		time.Sleep(3 * time.Second)

		// all keys should be expired now

		invokeRequest = &daprClient.InvokeBindingRequest{
			Name:      bindingName,
			Operation: string(bindings.GetOperation),
			Metadata:  map[string]string{"key": "incKey"},
		}

		out, err2 = client.InvokeBinding(ctx, invokeRequest)
		assert.NoError(t, err2)
		assert.Equal(t, []byte(nil), out.Data)

		invokeRequest = &daprClient.InvokeBindingRequest{
			Name:      bindingName,
			Operation: string(bindings.GetOperation),
			Metadata:  map[string]string{"key": "expireKey"},
		}

		out, err2 = client.InvokeBinding(ctx, invokeRequest)
		assert.NoError(t, err2)
		assert.Equal(t, []byte(nil), out.Data)

		invokeRequest = &daprClient.InvokeBindingRequest{
			Name:      bindingName,
			Operation: string(bindings.CreateOperation),
			Data:      []byte("43"),
			Metadata:  map[string]string{"key": "getDelKey"},
		}

		err = client.InvokeOutputBinding(ctx, invokeRequest)
		assert.NoError(t, err)

		invokeRequest = &daprClient.InvokeBindingRequest{
			Name:      bindingName,
			Operation: string(bindings.GetOperation),
			Metadata:  map[string]string{"key": "getDelKey", "delete": "true"},
		}
		out, err2 = client.InvokeBinding(ctx, invokeRequest)
		assert.NoError(t, err2)
		assert.Equal(t, "43", string(out.Data))

		invokeRequest = &daprClient.InvokeBindingRequest{
			Name:      bindingName,
			Operation: string(bindings.GetOperation),
			Metadata:  map[string]string{"key": "getDelKey"},
		}

		out, err2 = client.InvokeBinding(ctx, invokeRequest)
		assert.NoError(t, err2)
		assert.Equal(t, []byte(nil), out.Data)

		return nil
	}

	checkRedisConnection := func(ctx flow.Context) error {
		rdb := redis.NewClient(&redis.Options{
			Addr:     "localhost:6399", // host:port of the redis server
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
			Addr:     "localhost:6399", // host:port of the redis server
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
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithResourcesPath("components/standard"),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Insert data into the redis data store and check if the insertion was successful", testInvokeCreate).
		Run()

	flow.New(t, "Test Redis Output Binding after reconnecting to network").
		Step(dockercompose.Run("redis", dockerComposeYAML)).
		Step("Waiting for Redis Readiness...", retry.Do(time.Second*3, 10, checkRedisConnection)).
		Step(sidecar.Run(sidecarName,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithResourcesPath("components/standard"),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
			)...,
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
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithResourcesPath("components/retryOptions"),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Stop Redis server", dockercompose.Stop("redis", dockerComposeYAML)).
		Step("Start Redis server", dockercompose.Start("redis", dockerComposeYAML)).
		// After restarting Redis, it usually takes a couple of seconds for the container to start but
		// since we have retry strategies and connection timeouts configured, the client will retry if it is
		// not able to establish a connection to the Redis server
		Step("Insert data into the redis data store during the server restart", testInvokeCreate).
		Step("Check if the data is accessible after the server is up again", testCheckInsertedData).
		Run()

	flow.New(t, "Test Redis Output Binding CREATE and INCREMENT operation with EXPIRE").
		Step(dockercompose.Run("redis", dockerComposeYAML)).
		Step("Waiting for Redis Readiness...", retry.Do(time.Second*3, 10, checkRedisConnection)).
		Step(sidecar.Run(sidecarName,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithResourcesPath("components/standard"),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Insert data and increment data, with expiration options.", testInvokeCreateIncr).
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	bindingsRegistry := bindingsLoader.NewRegistry()
	bindingsRegistry.Logger = log
	bindingsRegistry.RegisterOutputBinding(func(l logger.Logger) bindings.OutputBinding {
		return bindingRedis.NewRedis(l)
	}, "redis")

	return []embedded.Option{
		embedded.WithBindings(bindingsRegistry),
	}
}
