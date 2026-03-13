package mongodb_test

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/state"
	stateMongoDB "github.com/dapr/components-contrib/state/mongodb"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	stateLoader "github.com/dapr/dapr/pkg/components/state"
	daprTesting "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
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
	require.NoError(t, err)

	stateRegistry := stateLoader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(func(l logger.Logger) state.Store {
		return stateStore
	}, "mongodb")

	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	basicTest := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(strconv.Itoa(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("mongodbCert"), nil)
		require.NoError(t, err)

		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key2", []byte("mongodbCert2"), nil)
		require.NoError(t, err)

		// get state
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		require.NoError(t, err)
		assert.Equal(t, "mongodbCert", string(item.Value))
		assert.NotContains(t, item.Metadata, "ttlExpireTime")

		errUpdate := client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("mongodbCertUpdate"), nil)
		require.NoError(t, errUpdate)
		item, errUpdatedGet := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		require.NoError(t, errUpdatedGet)
		assert.Equal(t, "mongodbCertUpdate", string(item.Value))

		// delete state
		err = client.DeleteState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		require.NoError(t, err)

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
		require.NoError(t, err)
		assert.Equal(t, "mongodbCert2", string(item.Value))

		return nil
	}

	// Time-To-Live Test
	timeToLiveTest := func(sidecarname string) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
			require.NoError(t, err)
			defer client.Close()

			assert.Error(t, client.SaveState(ctx, stateStoreName, certificationTestPrefix+"ttl1", []byte("mongodbCert"), map[string]string{
				"ttlInSeconds": "mock value",
			}))
			require.NoError(t, client.SaveState(ctx, stateStoreName, certificationTestPrefix+"ttl2", []byte("mongodbCert2"), map[string]string{
				"ttlInSeconds": "-1",
			}))
			require.NoError(t, client.SaveState(ctx, stateStoreName, certificationTestPrefix+"ttl3", []byte("mongodbCert3"), map[string]string{
				"ttlInSeconds": "3",
			}))

			// Check we have the correct database ID for the TTL test.
			cl, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017/?directConnection=true"))
			require.NoError(t, err)
			resp := cl.Database("admin").
				Collection("daprCollection").
				FindOne(ctx, bson.M{"_id": sidecarname + "||stable-certification-ttl3"})
			require.NoError(t, resp.Err())

			// get state
			item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"ttl3", nil)
			require.NoError(t, err)
			assert.Equal(t, "mongodbCert3", string(item.Value))
			assert.Contains(t, item.Metadata, "ttlExpireTime")
			expireTime, err := time.Parse(time.RFC3339, item.Metadata["ttlExpireTime"])
			require.NoError(t, err)
			assert.InDelta(t, time.Now().Add(time.Second*3).Unix(), expireTime.Unix(), 2)

			assert.Eventually(t, func() bool {
				item, err = client.GetState(ctx, stateStoreName, certificationTestPrefix+"ttl3", nil)
				require.NoError(t, err)
				return len(item.Value) == 0
			}, time.Second*7, time.Millisecond*500)

			// MongoDB will delete a document after a maximum of 60 seconds.
			assert.Eventually(t, func() bool {
				resp := cl.Database("admin").
					Collection("daprCollection").
					FindOne(ctx, bson.M{"_id": sidecarname + "||stable-certification-ttl3"})
				return resp.Err() != nil && errors.Is(resp.Err(), mongo.ErrNoDocuments)
			}, time.Second*60, time.Millisecond*500)

			return nil
		}
	}

	keysLikeTest := func(sidecarName string) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			cl, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017/?directConnection=true"))
			require.NoError(t, err)
			defer cl.Disconnect(ctx)

			coll := cl.Database("admin").Collection("daprCollection")

			// Clean up any existing test keys first.
			_, err = coll.DeleteMany(ctx, bson.M{
				"_id": bson.M{"$regex": "^" + sidecarName + `\|\|keyslike-`},
			})
			require.NoError(t, err)

			// Insert test documents using the same key format Dapr uses.
			testKeys := []string{
				sidecarName + "||keyslike-workflow||instance1||metadata",
				sidecarName + "||keyslike-workflow||instance2||metadata",
				sidecarName + "||keyslike-workflow||instance3||metadata",
				sidecarName + "||keyslike-other||instance4||metadata",
			}
			for _, key := range testKeys {
				_, err = coll.InsertOne(ctx, bson.M{
					"_id":   key,
					"value": "test",
				})
				require.NoError(t, err)
			}

			// Use the state store's KeysLike method directly.
			store, ok := state.Store(stateStore).(state.KeysLiker)
			require.True(t, ok, "MongoDB must implement KeysLiker interface")

			// Query for workflow keys only.
			resp, err := store.KeysLike(ctx, &state.KeysLikeRequest{
				Pattern: sidecarName + "||keyslike-workflow||%||metadata",
			})
			require.NoError(t, err)
			require.Len(t, resp.Keys, 3)
			assert.ElementsMatch(t, []string{
				sidecarName + "||keyslike-workflow||instance1||metadata",
				sidecarName + "||keyslike-workflow||instance2||metadata",
				sidecarName + "||keyslike-workflow||instance3||metadata",
			}, resp.Keys)

			// Test pagination.
			resp, err = store.KeysLike(ctx, &state.KeysLikeRequest{
				Pattern:  sidecarName + "||keyslike-workflow||%||metadata",
				PageSize: ptr.Of(uint32(2)),
			})
			require.NoError(t, err)
			require.Len(t, resp.Keys, 2)
			require.NotNil(t, resp.ContinuationToken)

			resp2, err := store.KeysLike(ctx, &state.KeysLikeRequest{
				Pattern:           sidecarName + "||keyslike-workflow||%||metadata",
				ContinuationToken: resp.ContinuationToken,
			})
			require.NoError(t, err)
			require.Len(t, resp2.Keys, 1)
			assert.Nil(t, resp2.ContinuationToken)

			allKeys := append(resp.Keys, resp2.Keys...)
			assert.ElementsMatch(t, []string{
				sidecarName + "||keyslike-workflow||instance1||metadata",
				sidecarName + "||keyslike-workflow||instance2||metadata",
				sidecarName + "||keyslike-workflow||instance3||metadata",
			}, allKeys)

			// Clean up.
			_, err = coll.DeleteMany(ctx, bson.M{
				"_id": bson.M{"$regex": "^" + sidecarName + `\|\|keyslike-`},
			})
			require.NoError(t, err)

			return nil
		}
	}

	flow.New(t, "Connecting MongoDB And Verifying majority of the tests for a replica set here").
		Step(dockercompose.Run("mongodb", dockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(20*time.Second)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerClusterDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			embedded.WithComponentsPath("components/docker/default"),
			embedded.WithStates(stateRegistry))).
		Step("Waiting for component to load...", flow.Sleep(10*time.Second)).
		Step("Run basic test", basicTest).
		Step("Run time to live test", timeToLiveTest(sidecarNamePrefix+"dockerClusterDefault")).
		Step("Run KeysLike test", keysLikeTest(sidecarNamePrefix+"dockerClusterDefault")).
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

	flow.New(t, "Connecting MongoDB And Verifying majority of the tests for a replica set here with valid read, write concerns and operation timeout").
		Step(dockercompose.Run("mongodb", dockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(20*time.Second)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerClusterValidReadWriteConcernAndTimeout",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			embedded.WithComponentsPath("components/docker/validReadWriteConcernAndTimeout"),
			embedded.WithStates(stateRegistry))).
		Step("Waiting for component to load...", flow.Sleep(10*time.Second)).
		Step("Run basic test", basicTest).
		Step("Run time to live test", timeToLiveTest(sidecarNamePrefix+"dockerClusterValidReadWriteConcernAndTimeout")).
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

	flow.New(t, "Connecting MongoDB using connectionString and Verifying majority of the tests here for a single node with valid read, write concerns and operation timeout").
		Step(dockercompose.Run("mongodb", dockerComposeSingleYAML)).
		Step("Waiting for component to start...", flow.Sleep(20*time.Second)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerSingleNode",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			embedded.WithComponentsPath("components/docker/singleNode"),
			embedded.WithStates(stateRegistry))).
		Step("Waiting for component to load...", flow.Sleep(10*time.Second)).
		Step("Run basic test", basicTest).
		Step("Run time to live test", timeToLiveTest(sidecarNamePrefix+"dockerSingleNode")).
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
