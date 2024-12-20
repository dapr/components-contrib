package ravendb_test

import (
	"fmt"
	"github.com/dapr/components-contrib/state"
	stateRavenDB "github.com/dapr/components-contrib/state/ravendb"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	stateLoader "github.com/dapr/dapr/pkg/components/state"
	daprTesting "github.com/dapr/dapr/pkg/testing"
	daprClient "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"
)

const (
	sidecarNamePrefix       = "ravendb-sidecar-"
	stateStoreName          = "statestore"
	certificationTestPrefix = "stable-certification-"
	dockerComposeYAML       = "docker-compose.yaml"
)

func TestRavenDB(t *testing.T) {
	fmt.Printf("testing started:")
	log := logger.NewLogger("dapr.components")
	stateStore := stateRavenDB.NewRavenDB(log).(*stateRavenDB.RavenDB)
	ports, err := daprTesting.GetFreePorts(2)
	require.NoError(t, err)

	stateRegistry := stateLoader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(func(l logger.Logger) state.Store {
		return stateStore
	}, "ravenDb")

	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	basicTest := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(strconv.Itoa(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("ravenCert1"), nil)
		require.NoError(t, err)

		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key2", []byte("ravenCert2"), nil)
		require.NoError(t, err)

		// get state
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		require.NoError(t, err)
		assert.Equal(t, "ravenCert1", string(item.Value))

		errUpdate := client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("ravenCertUpdate"), nil)
		require.NoError(t, errUpdate)
		item, errUpdatedGet := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		require.NoError(t, errUpdatedGet)
		assert.Equal(t, "ravenCertUpdate", string(item.Value))

		// delete state
		err = client.DeleteState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		require.NoError(t, err)

		return nil
	}

	eTagTest := func() func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client, err := daprClient.NewClientWithPort(strconv.Itoa(currentGrpcPort))
			if err != nil {
				panic(err)
			}
			defer client.Close()

			err = client.SaveState(ctx, stateStoreName, "k", []byte("v1"), nil)
			require.NoError(t, err)

			resp1, err := client.GetState(ctx, stateStoreName, "k", nil)
			require.NoError(t, err)

			err = client.SaveStateWithETag(ctx, stateStoreName, "k", []byte("v2"), resp1.Etag, nil)
			require.NoError(t, err)

			resp2, err := client.GetState(ctx, stateStoreName, "k", nil)
			require.NoError(t, err)

			err = client.SaveStateWithETag(ctx, stateStoreName, "k", []byte("v3"), "900invalid", nil)
			require.Error(t, err)

			resp3, err := client.GetState(ctx, stateStoreName, "k", nil)
			require.NoError(t, err)
			assert.Equal(t, resp2.Etag, resp3.Etag)
			assert.Equal(t, "v2", string(resp3.Value))

			return nil
		}
	}

	timeToLiveTest := func() func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client, err := daprClient.NewClientWithPort(fmt.Sprint(currentGrpcPort))
			require.NoError(t, err)
			defer client.Close()

			assert.Error(t, client.SaveState(ctx, stateStoreName, certificationTestPrefix+"ttl1", []byte("revendbCert"), map[string]string{
				"ttlInSeconds": "mock value",
			}))
			require.NoError(t, client.SaveState(ctx, stateStoreName, certificationTestPrefix+"ttl2", []byte("revendbCert2"), map[string]string{
				"ttlInSeconds": "-1",
			}))
			require.NoError(t, client.SaveState(ctx, stateStoreName, certificationTestPrefix+"ttl3", []byte("revendbCert3"), map[string]string{
				"ttlInSeconds": "3",
			}))

			// get state
			item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"ttl3", nil)
			require.NoError(t, err)
			assert.Equal(t, "revendbCert3", string(item.Value))
			assert.Contains(t, item.Metadata, "ttlExpireTime")
			expireTime, err := time.Parse(time.RFC3339, item.Metadata["ttlExpireTime"])
			require.NoError(t, err)
			assert.InDelta(t, time.Now().Add(time.Second*3).Unix(), expireTime.Unix(), 2)

			assert.Eventually(t, func() bool {
				item, err = client.GetState(ctx, stateStoreName, certificationTestPrefix+"ttl3", nil)
				require.NoError(t, err)
				return len(item.Value) == 0
			}, time.Second*10, time.Second*1)

			return nil
		}
	}

	transactionsTest := func() func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client, err := daprClient.NewClientWithPort(strconv.Itoa(currentGrpcPort))
			if err != nil {
				panic(err)
			}
			defer client.Close()

			err = client.ExecuteStateTransaction(ctx, stateStoreName, nil, []*daprClient.StateOperation{
				{
					Type: daprClient.StateOperationTypeUpsert,
					Item: &daprClient.SetStateItem{
						Key:   "reqKey1",
						Value: []byte("reqVal1"),
						Metadata: map[string]string{
							"ttlInSeconds": "-1",
						},
					},
				},
				{
					Type: daprClient.StateOperationTypeUpsert,
					Item: &daprClient.SetStateItem{
						Key:   "reqKey2",
						Value: []byte("reqVal2"),
						Metadata: map[string]string{
							"ttlInSeconds": "222",
						},
					},
				},
				{
					Type: daprClient.StateOperationTypeUpsert,
					Item: &daprClient.SetStateItem{
						Key:   "reqKey3",
						Value: []byte("reqVal3"),
					},
				},
				{
					Type: daprClient.StateOperationTypeUpsert,
					Item: &daprClient.SetStateItem{
						Key:   "reqKey4",
						Value: []byte("reqVal101"),
						Metadata: map[string]string{
							"ttlInSeconds": "50",
						},
					},
				},
				{
					Type: daprClient.StateOperationTypeUpsert,
					Item: &daprClient.SetStateItem{
						Key:   "reqKey5",
						Value: []byte("reqVal103"),
						Metadata: map[string]string{
							"ttlInSeconds": "50",
						},
					},
				},
			})
			require.NoError(t, err)

			resp1, err := client.GetState(ctx, stateStoreName, "reqKey1", nil)
			require.NoError(t, err)
			assert.Equal(t, "reqVal1", string(resp1.Value))

			resp3, err := client.GetState(ctx, stateStoreName, "reqKey3", nil)
			require.NoError(t, err)
			assert.Equal(t, "reqVal3", string(resp3.Value))

			return nil
		}
	}

	testGetAfterRavenDBRestart := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		// get state
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key2", nil)
		require.NoError(t, err)
		assert.Equal(t, "ravenCert2", string(item.Value))

		return nil
	}

	flow.New(t, "Connecting RavenDB And Verifying majority of the tests.").
		Step(dockercompose.Run("ravendb", dockerComposeYAML)).
		Step("Waiting for component to start...", flow.Sleep(20*time.Second)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerClusterDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			embedded.WithResourcesPath("components/default"),
			embedded.WithStates(stateRegistry))).
		Step("Waiting for component to load...", flow.Sleep(10*time.Second)).
		Step("Run basic test", basicTest).
		Step("Run Etag test", eTagTest()).
		Step("Run transaction test", transactionsTest()).
		Step("Run time to live test", timeToLiveTest()).
		Step("Interrupt network",
			network.InterruptNetwork(5*time.Second, nil, nil, "27017:27017")).
		// Component should recover at this point.
		Step("Wait", flow.Sleep(10*time.Second)).
		Step("Run basic test again to verify reconnection occurred", basicTest).
		Step("Stop RavenDB server", dockercompose.Stop("ravendb", dockerComposeYAML)).
		Step("Start RavenDB server", dockercompose.Start("ravendb", dockerComposeYAML)).
		Step("Waiting for component to start...", flow.Sleep(10*time.Second)).
		Step("Get Values Saved Earlier And Not Expired, after RavenDB restart", testGetAfterRavenDBRestart).
		Step("Wait to check documents", flow.Sleep(10*time.Second)).
		Run()
}
