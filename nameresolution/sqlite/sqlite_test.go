/*
Copyright 2023 The Dapr Authors
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

package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

func TestSqliteNameResolver(t *testing.T) {
	nr := NewResolver(logger.NewLogger("test")).(*resolver)

	t.Run("Init", func(t *testing.T) {
		err := nr.Init(context.Background(), nameresolution.Metadata{
			Instance: nameresolution.Instance{
				Address:          "127.0.0.1",
				DaprInternalPort: 1234,
				AppID:            "myapp",
			},
			Configuration: map[string]string{
				"connectionString": ":memory:",
				"cleanupInterval":  "0",
				"updateInterval":   "120s",
			},
		})

		require.NoError(t, err)
	})

	require.False(t, t.Failed(), "Cannot continue if init step failed")

	t.Run("Populate test data", func(t *testing.T) {
		// Note updateInterval is 120s
		now := time.Now().Unix()
		rows := [][]any{
			{"2cb5f837", "1.1.1.1:1", "app-1", "", now},
			{"4d1e7b11", "1.1.1.1:2", "app-1", "", now},
			{"05add1fa", "1.1.1.1:3", "app-1", "", now},
			{"f1b24d4b", "2.2.2.2:1", "app-2", "", now},
			{"23fb164f", "2.2.2.2:2", "app-2", "", now - 200},
			{"db50a29e", "3.3.3.3:1", "app-3", "", now},
			{"eef793d4", "4.4.4.4:1", "app-4", "", now - 200},
			{"ef06eb49", "5.5.5.5:1", "app-5", "", now},
			{"b0e6cd89", "6.6.6.6:1", "app-6", "", now},
			{"36e99c68", "7.7.7.7:1", "app-7", "", now},
			{"f77ed318", "8.8.8.8:1", "app-8", "", now - 100},
		}
		for i, r := range rows {
			_, err := nr.db.Exec("INSERT INTO hosts VALUES (?, ?, ?, ?, ?)", r...)
			require.NoErrorf(t, err, "Failed to insert row %d", i)
		}
	})

	if t.Failed() {
		nr.Close()
		require.Fail(t, "Cannot continue if populate test data step failed")
	}

	t.Run("Resolve", func(t *testing.T) {
		type testCase struct {
			appID       string
			expectEmpty bool
			expectOne   string
			expectAny   []string
		}
		tt := map[string]testCase{
			"single host resolved 1": {appID: "app-5", expectOne: "5.5.5.5:1"},
			"single host resolved 2": {appID: "app-8", expectAny: []string{"8.8.8.8:1"}}, // Use expectAny to make the test run multiple times
			"not found":              {appID: "notfound", expectEmpty: true},
			"host expired":           {appID: "app-4", expectEmpty: true},
			"multiple hosts found":   {appID: "app-1", expectAny: []string{"1.1.1.1:1", "1.1.1.1:2", "1.1.1.1:3"}},
			"one host expired":       {appID: "app-2", expectAny: []string{"2.2.2.2:1"}}, // Use expectAny to make the test run multiple times
		}
		for name, tc := range tt {
			t.Run(name, func(t *testing.T) {
				if len(tc.expectAny) == 0 {
					res, err := nr.ResolveID(context.Background(), nameresolution.ResolveRequest{ID: tc.appID})

					if tc.expectEmpty {
						require.Error(t, err)
						require.ErrorIs(t, err, ErrNoHost)
						require.Empty(t, res)
					} else {
						require.NoError(t, err)
						require.Equal(t, tc.expectOne, res)
					}
				} else {
					for i := range 20 {
						res, err := nr.ResolveID(context.Background(), nameresolution.ResolveRequest{ID: tc.appID})
						require.NoErrorf(t, err, "Error on iteration %d", i)
						require.Contains(t, tc.expectAny, res)
					}
				}
			})
		}
	})

	// Simulate the ticker
	t.Run("Renew registration", func(t *testing.T) {
		t.Run("Succeess", func(t *testing.T) {
			const addr = "127.0.0.1:1234"

			// Get current last_update value
			var lastUpdate int
			err := nr.db.QueryRow("SELECT last_update FROM hosts WHERE address = ?", addr).Scan(&lastUpdate)
			require.NoError(t, err)

			// Must sleep for 1s
			time.Sleep(time.Second)

			// Renew
			err = nr.doRenewRegistration(context.Background(), addr)
			require.NoError(t, err)

			// Get updated last_update
			var newLastUpdate int
			err = nr.db.QueryRow("SELECT last_update FROM hosts WHERE address = ?", addr).Scan(&newLastUpdate)
			require.NoError(t, err)

			// Should have increased
			require.Greater(t, newLastUpdate, lastUpdate)
		})

		t.Run("Lost registration", func(t *testing.T) {
			// Renew
			err := nr.doRenewRegistration(context.Background(), "fail")
			require.Error(t, err)
			require.ErrorIs(t, err, errRegistrationLost)
		})
	})

	t.Run("Close", func(t *testing.T) {
		err := nr.Close()
		require.NoError(t, err)
	})
}
