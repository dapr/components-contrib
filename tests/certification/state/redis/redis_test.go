/*
Copyright 2022 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package redis_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/internal/component/redis"
	"github.com/dapr/components-contrib/state"
	state_redis "github.com/dapr/components-contrib/state/redis"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
)

const (
	clusterName       = "rediscertification"
	dockerComposeYAML = "docker-compose.yml"
	redisURL          = "localhost:6379"
	storeName         = "statestore"
	sidecarName1      = "dapr-1"
	appID1            = "app-1"
	appPort           = 8000
)

var (
	records []string = []string{
		`{
    "person": {
      "org": "Dev Ops",
      "id": 1036
    },
    "city": "Seattle",
    "state": "WA"
  }`,
		`{
    "person": {
      "org": "Hardware",
      "id": 1028
    },
    "city": "Portland",
    "state": "OR"
  }`,
		`{
    "person": {
      "org": "Finance",
      "id": 1071
    },
    "city": "Sacramento",
    "state": "CA"
  }`,
		`{
    "person": {
      "org": "Dev Ops",
      "id": 1042
    },
    "city": "Spokane",
    "state": "WA"
  }`,
		`{
    "person": {
      "org": "Hardware",
      "id": 1007
    },
    "city": "Los Angeles",
    "state": "CA"
  }`,
		`{
    "person": {
      "org": "Finance",
      "id": 1094
    },
    "city": "Eugene",
    "state": "OR"
  }`,
		`{
    "person": {
      "org": "Dev Ops",
      "id": 1015
    },
    "city": "San Francisco",
    "state": "CA"
  }`,
		`{
    "person": {
      "org": "Hardware",
      "id": 1077
    },
    "city": "Redmond",
    "state": "WA"
  }`,
		`{
    "person": {
      "org": "Finance",
      "id": 1002
    },
    "city": "San Diego",
    "state": "CA"
  }`,
		`{
    "person": {
      "org": "Dev Ops",
      "id": 1054
    },
    "city": "New York",
    "state": "NY"
  }`}

	query string = `
  {
    "filter": {
        "OR": [
            {
                "EQ": { "person.org": "Dev Ops" }
            },
            {
                "AND": [
                    {
                        "EQ": { "person.org": "Finance" }
                    },
                    {
                        "IN": { "state": [ "CA", "WA" ] }
                    }
                ]
            }
        ]
    },
    "sort": [
      {
        "key": "person.id",
        "order": "DESC"
      }
    ],
    "page": {
        "limit": 3
    }
  }`
)

func redisReady(url string) flow.Runnable {
	return func(ctx flow.Context) error {
		meta := map[string]string{"redisHost": redisURL, "redisPassword": ""}
		client, _, err := redis.ParseClientFromProperties(meta, nil)
		if err != nil {
			return err
		}

		return client.Ping(context.Background()).Err()
	}
}

func TestRedis(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	log.SetOutputLevel(logger.DebugLevel)

	// Test logic that stores and retrieves data.
	test := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName1)

		meta := map[string]string{"contentType": "application/json", "queryIndexName": "orgIndx"}
		// save states
		for indx, value := range records {
			log.Infof("Saving state with key '%d'", indx)
			err := client.SaveState(ctx, storeName, strconv.Itoa(indx), []byte(value), meta)
			require.NoError(ctx, err, "error saving state")
		}

		// get state
		indx := 3
		key := fmt.Sprintf("%d", indx)
		log.Infof("Getting state with key '%s'", key)
		item, err := client.GetState(ctx, storeName, key, meta)
		require.NoError(ctx, err, "error getting state")
		require.Equal(t, key, item.Key)
		var vExpected, vActual interface{}
		err = jsoniter.Unmarshal([]byte(records[indx]), &vExpected)
		require.NoError(ctx, err, "parsing error")
		err = jsoniter.Unmarshal(item.Value, &vActual)
		require.NoError(ctx, err, "parsing error")
		require.Equal(t, vExpected, vActual)

		//query state
		log.Infof("Querying state")
		resp, err := client.QueryStateAlpha1(ctx, storeName, query, meta)
		require.NoError(ctx, err, "error querying state")
		require.Equal(t, "3", resp.Token)
		require.Equal(t, 3, len(resp.Results))
		for i, indx := range []int{2, 9, 3} {
			require.Equal(t, fmt.Sprintf("%d", indx), resp.Results[i].Key)
			var vExpected, vActual interface{}
			err = jsoniter.Unmarshal([]byte(records[indx]), &vExpected)
			require.NoError(ctx, err, "parsing error")
			err = jsoniter.Unmarshal(resp.Results[i].Value, &vActual)
			require.NoError(ctx, err, "parsing error")
			require.Equal(t, vExpected, vActual)
		}
		return nil
	}

	// Application logic.
	application := func() app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			return nil
		}
	}

	flow.New(t, "redis certification").
		// Run Redis using Docker Compose.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for redis readiness",
			retry.Do(time.Second, 30, redisReady(redisURL))).
		// Run the application logic above.
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort), application())).
		// Run the Dapr sidecar with the RabbitMQ component.
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			embedded.WithProfilePort(runtime.DefaultProfilePort),
			runtime.WithStates(
				state_loader.New("redis", func() state.Store {
					return state_redis.NewRedisStateStore(log)
				}),
			))).
		Step("test", test).
		Run()
}
