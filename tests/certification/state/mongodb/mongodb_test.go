package mongodb_test

import (
	"fmt"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/go-sdk/client"
	"testing"
	"time"

	"github.com/dapr/components-contrib/state"
	stateMongoDB "github.com/dapr/components-contrib/state/mongodb"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	stateLoader "github.com/dapr/dapr/pkg/components/state"
	"github.com/dapr/dapr/pkg/runtime"
	daprTesting "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

const (
	sidecarNamePrefix        = "mongodb-sidecar-"
	dockerComposeClusterYAML = "docker-compose-cluster.yml"
	dockerComposeSingleYAML  = "docker-compose-single.yml"
	stateStoreName           = "statestore"
	certificationTestPrefix  = "stable-certification-"
)

func TestMongoDB(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	stateStore := stateMongoDB.NewMongoDB(log).(*stateMongoDB.MongoDB)
	ports, err := daprTesting.GetFreePorts(2)
	assert.NoError(t, err)

	stateRegistry := stateLoader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(func(l logger.Logger) state.Store {
		return stateStore
	}, "mongodb")

	// var rdb redis.Client
	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	basicTest := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("mongodbCert"), nil)
		assert.NoError(t, err)

		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key2", []byte("mongodbCert2"), nil)
		assert.NoError(t, err)

		// get state
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)
		assert.Equal(t, "mongodbCert", string(item.Value))

		errUpdate := client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("mongodbCertUpdate"), nil)
		assert.NoError(t, errUpdate)
		item, errUpdatedGet := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(t, errUpdatedGet)
		assert.Equal(t, "mongodbCertUpdate", string(item.Value))

		// delete state
		err = client.DeleteState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)

		return nil
	}

	testGetAfterMongoDBRestart := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		// get state
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key2", nil)
		assert.NoError(t, err)
		assert.Equal(t, "mongodbCert2", string(item.Value))

		return nil
	}

	flow.New(t, "Connecting MongoDB And Verifying majority of the tests for a replica set here").
		Step(dockercompose.Run("mongodb", dockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(10*time.Second)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerClusterDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("components/docker/default"),
			runtime.WithStates(stateRegistry))).
		Step("Waiting for component to load...", flow.Sleep(10*time.Second)).
		Step("Run basic test", basicTest).
		Step("Interrupt network",
			network.InterruptNetwork(5*time.Second, nil, nil, "27017:27017")).
		// Component should recover at this point.
		Step("Wait", flow.Sleep(10*time.Second)).
		Step("Run basic test again to verify reconnection occurred", basicTest).
		Step("Stop MongoDB server", dockercompose.Stop("mongodb", dockerComposeClusterYAML)).
		Step("Start MongoDB server", dockercompose.Start("mongodb", dockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(10*time.Second)).
		Step("Get Values Saved Earlier And Not Expired, after MongoDB restart", testGetAfterMongoDBRestart).
		Run()

	flow.New(t, "Connecting MongoDB And Verifying majority of the tests for a replica set "+
		"here with valid read, write concerns and operation timeout").
		Step(dockercompose.Run("mongodb", dockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(10*time.Second)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerClusterValidReadWriteConcernAndTimeout",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("components/docker/validReadWriteConcernAndTimeout"),
			runtime.WithStates(stateRegistry))).
		Step("Waiting for component to load...", flow.Sleep(10*time.Second)).
		Step("Run basic test", basicTest).
		Step("Interrupt network",
			network.InterruptNetwork(5*time.Second, nil, nil, "27017:27017")).
		// Component should recover at this point.
		Step("Wait", flow.Sleep(10*time.Second)).
		Step("Run basic test again to verify reconnection occurred", basicTest).
		Step("Stop MongoDB server", dockercompose.Stop("mongodb", dockerComposeClusterYAML)).
		Step("Start MongoDB server", dockercompose.Start("mongodb", dockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(10*time.Second)).
		Step("Get Values Saved Earlier And Not Expired, after MongoDB restart", testGetAfterMongoDBRestart).
		Run()

	flow.New(t, "Connecting MongoDB And Verifying majority of the tests here for a single node with valid read, "+
		"write concerns and operation timeout").
		Step(dockercompose.Run("mongodb", dockerComposeSingleYAML)).
		Step("Waiting for component to start...", flow.Sleep(10*time.Second)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerSingleNode",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("components/docker/singleNode"),
			runtime.WithStates(stateRegistry))).
		Step("Waiting for component to load...", flow.Sleep(10*time.Second)).
		Step("Run basic test", basicTest).
		Step("Interrupt network",
			network.InterruptNetwork(5*time.Second, nil, nil, "27017:27017")).
		// Component should recover at this point.
		Step("Wait", flow.Sleep(10*time.Second)).
		Step("Run basic test again to verify reconnection occurred", basicTest).
		Step("Stop MongoDB server", dockercompose.Stop("mongodb", dockerComposeSingleYAML)).
		Step("Start MongoDB server", dockercompose.Start("mongodb", dockerComposeSingleYAML)).
		Step("Waiting for component to start...", flow.Sleep(10*time.Second)).
		Step("Get Values Saved Earlier And Not Expired, after MongoDB restart", testGetAfterMongoDBRestart).
		Run()
}
