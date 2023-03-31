/*
Copyright 2021 The Dapr Authors
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

package state

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/contenttype"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/tests/conformance/utils"
)

type ValueType struct {
	Message string `json:"message"`
}

type intValueType struct {
	Message int32 `json:"message"`
}

type scenario struct {
	key              string
	value            interface{}
	toBeDeleted      bool
	bulkOnly         bool
	transactionOnly  bool
	transactionGroup int
	contentType      string
}

type queryScenario struct {
	query   string
	results []state.QueryItem
}

type TestConfig struct {
	utils.CommonConfig
}

func NewTestConfig(component string, allOperations bool, operations []string, conf map[string]interface{}) TestConfig {
	tc := TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "state",
			ComponentName: component,
			AllOperations: allOperations,
			Operations:    utils.NewStringSet(operations...),
		},
	}

	return tc
}

// ConformanceTests runs conf tests for state store.
func ConformanceTests(t *testing.T, props map[string]string, statestore state.Store, config TestConfig) {
	// Test vars
	key := strings.ReplaceAll(uuid.New().String(), "-", "")
	t.Logf("Base key for test: %s", key)

	scenarios := []scenario{
		{
			key:   fmt.Sprintf("%s-int", key),
			value: 123,
		},
		{
			key:   fmt.Sprintf("%s-bool", key),
			value: true,
		},
		{
			key:   fmt.Sprintf("%s-bytes", key),
			value: []byte{0x1},
		},
		{
			key:   fmt.Sprintf("%s-string-with-json", key),
			value: `{"a":"b"}`,
		},
		{
			key:   fmt.Sprintf("%s-string", key),
			value: "hello world",
		},
		{
			key:   fmt.Sprintf("%s-empty-string", key),
			value: "",
		},
		{
			key:         fmt.Sprintf("%s-struct", key),
			value:       ValueType{Message: fmt.Sprintf("test%s", key)},
			contentType: contenttype.JSONContentType,
		},
		{
			key:         fmt.Sprintf("%s-struct-with-int", key),
			value:       intValueType{Message: 42},
			contentType: contenttype.JSONContentType,
		},
		{
			key:         fmt.Sprintf("%s-to-be-deleted", key),
			value:       "to be deleted",
			toBeDeleted: true,
		},
		{
			key:      fmt.Sprintf("%s-bulk-int", key),
			value:    123,
			bulkOnly: true,
		},
		{
			key:      fmt.Sprintf("%s-bulk-bool", key),
			value:    true,
			bulkOnly: true,
		},
		{
			key:      fmt.Sprintf("%s-bulk-bytes", key),
			value:    []byte{0x1},
			bulkOnly: true,
		},
		{
			key:      fmt.Sprintf("%s-bulk-string", key),
			value:    "hello world",
			bulkOnly: true,
		},
		{
			key:      fmt.Sprintf("%s-bulk-struct", key),
			value:    ValueType{Message: "test"},
			bulkOnly: true,
		},
		{
			key:         fmt.Sprintf("%s-bulk-to-be-deleted", key),
			value:       "to be deleted",
			toBeDeleted: true,
			bulkOnly:    true,
		},
		{
			key:         fmt.Sprintf("%s-bulk-to-be-deleted-too", key),
			value:       "to be deleted too",
			toBeDeleted: true,
			bulkOnly:    true,
		},
		{
			key:              fmt.Sprintf("%s-trx-int", key),
			value:            123,
			transactionOnly:  true,
			transactionGroup: 1,
		},
		{
			key:              fmt.Sprintf("%s-trx-bool", key),
			value:            true,
			transactionOnly:  true,
			transactionGroup: 1,
		},
		{
			key:              fmt.Sprintf("%s-trx-bytes", key),
			value:            []byte{0x1},
			transactionOnly:  true,
			transactionGroup: 1,
		},
		{
			key:              fmt.Sprintf("%s-trx-string", key),
			value:            "hello world",
			transactionOnly:  true,
			transactionGroup: 1,
		},
		{
			key:              fmt.Sprintf("%s-trx-struct", key),
			value:            ValueType{Message: "test"},
			transactionOnly:  true,
			transactionGroup: 2,
		},
		{
			key:              fmt.Sprintf("%s-trx-to-be-deleted", key),
			value:            "to be deleted",
			toBeDeleted:      true,
			transactionOnly:  true,
			transactionGroup: 1,
		},
		{
			key:              fmt.Sprintf("%s-trx-to-be-deleted-too", key),
			value:            "to be deleted too",
			toBeDeleted:      true,
			transactionOnly:  true,
			transactionGroup: 3,
		},
	}

	queryScenarios := []queryScenario{
		{
			query: `
			{
				"filter": {
					"OR": [
						{
							"EQ": {"message": "dummy"}
						},
						{
							"IN": {"message": ["test` + key + `", "dummy"]}
						}
					]
				}
			}
			`,
			results: []state.QueryItem{
				{
					Key:  fmt.Sprintf("%s-struct", key),
					Data: []byte(fmt.Sprintf(`{"message":"test%s"}`, key)),
				},
			},
		},
	}

	t.Run("init", func(t *testing.T) {
		err := statestore.Init(context.Background(), state.Metadata{
			Base: metadata.Base{Properties: props},
		})
		assert.Nil(t, err)
	})

	// Don't run more tests if init failed
	if t.Failed() {
		t.Fatal("Init test failed, stopping further tests")
	}

	t.Run("ping", func(t *testing.T) {
		err := state.Ping(context.Background(), statestore)
		// TODO: Ideally, all stable components should implenment ping function,
		// so will only assert assert.Nil(t, err) finally, i.e. when current implementation
		// implements ping in existing stable components
		if err != nil {
			assert.EqualError(t, err, "ping is not implemented by this state store")
		} else {
			assert.Nil(t, err)
		}
	})

	if config.HasOperation("set") {
		t.Run("set", func(t *testing.T) {
			for _, scenario := range scenarios {
				if !scenario.bulkOnly && !scenario.transactionOnly {
					t.Logf("Setting value for %s", scenario.key)
					req := &state.SetRequest{
						Key:   scenario.key,
						Value: scenario.value,
					}
					if len(scenario.contentType) != 0 {
						req.Metadata = map[string]string{metadata.ContentType: scenario.contentType}
					}
					err := statestore.Set(context.Background(), req)
					assert.Nil(t, err)
				}
			}
		})
	}

	if config.HasOperation("get") {
		t.Run("get", func(t *testing.T) {
			for _, scenario := range scenarios {
				if !scenario.bulkOnly && !scenario.transactionOnly {
					t.Logf("Checking value presence for %s", scenario.key)
					req := &state.GetRequest{
						Key: scenario.key,
					}
					if len(scenario.contentType) != 0 {
						req.Metadata = map[string]string{metadata.ContentType: scenario.contentType}
					}
					res, err := statestore.Get(context.Background(), req)
					require.NoError(t, err)
					assertEquals(t, scenario.value, res)
				}
			}
		})
	}

	if config.HasOperation("query") {
		t.Run("query", func(t *testing.T) {
			querier, ok := statestore.(state.Querier)
			assert.Truef(t, ok, "Querier interface is not implemented")
			for _, scenario := range queryScenarios {
				t.Logf("Querying value presence for %s", scenario.query)
				var req state.QueryRequest
				err := json.Unmarshal([]byte(scenario.query), &req.Query)
				require.NoError(t, err)
				req.Metadata = map[string]string{
					metadata.ContentType:    contenttype.JSONContentType,
					metadata.QueryIndexName: "qIndx",
				}
				resp, err := querier.Query(context.Background(), &req)
				require.NoError(t, err)
				assert.Equal(t, len(scenario.results), len(resp.Results))
				for i := range scenario.results {
					var expected, actual interface{}
					err = json.Unmarshal(scenario.results[i].Data, &expected)
					assert.NoError(t, err)
					err = json.Unmarshal(resp.Results[i].Data, &actual)
					assert.NoError(t, err)
					assert.Equal(t, scenario.results[i].Key, resp.Results[i].Key)
					assert.Equal(t, expected, actual)
				}
			}
		})
	}

	if config.HasOperation("delete") {
		t.Run("delete", func(t *testing.T) {
			for _, scenario := range scenarios {
				if !scenario.bulkOnly && scenario.toBeDeleted {
					// this also deletes two keys that were not inserted in the set operation
					t.Logf("Deleting %s", scenario.key)
					req := &state.DeleteRequest{
						Key: scenario.key,
					}
					if len(scenario.contentType) != 0 {
						req.Metadata = map[string]string{metadata.ContentType: scenario.contentType}
					}
					err := statestore.Delete(context.Background(), req)
					assert.NoError(t, err, "no error expected while deleting %s", scenario.key)

					t.Logf("Checking value absence for %s", scenario.key)
					res, err := statestore.Get(context.Background(), &state.GetRequest{
						Key: scenario.key,
					})
					assert.NoError(t, err, "no error expected while checking for absence for %s", scenario.key)
					assert.Nil(t, res.Data, "no data expected while checking for absence for %s", scenario.key)
				}
			}
		})
	}

	if config.HasOperation("bulkset") {
		t.Run("bulkset", func(t *testing.T) {
			var bulk []state.SetRequest
			for _, scenario := range scenarios {
				if scenario.bulkOnly {
					t.Logf("Adding set request to bulk for %s", scenario.key)
					bulk = append(bulk, state.SetRequest{
						Key:   scenario.key,
						Value: scenario.value,
					})
				}
			}
			err := statestore.BulkSet(context.Background(), bulk)
			require.NoError(t, err)

			for _, scenario := range scenarios {
				if scenario.bulkOnly {
					t.Logf("Checking value presence for %s", scenario.key)
					// Data should have been inserted at this point
					res, err := statestore.Get(context.Background(), &state.GetRequest{
						Key: scenario.key,
					})
					require.NoError(t, err)
					assertEquals(t, scenario.value, res)
				}
			}
		})
	}

	if config.HasOperation("bulkget") {
		t.Run("bulkget", func(t *testing.T) {
			var req []state.GetRequest
			expects := map[string]any{}
			for _, scenario := range scenarios {
				if scenario.bulkOnly {
					t.Logf("Adding get request to bulk for %s", scenario.key)
					req = append(req, state.GetRequest{
						Key: scenario.key,
					})
					expects[scenario.key] = scenario.value
				}
			}
			res, err := statestore.BulkGet(context.Background(), req)
			require.NoError(t, err)
			require.Len(t, res, len(expects))

			for _, r := range res {
				t.Logf("Checking value equality %s", r.Key)
				_, ok := expects[r.Key]
				if assert.Empty(t, r.Error) && assert.True(t, ok) {
					assertDataEquals(t, expects[r.Key], r.Data)
				}
			}
		})
	}

	if config.HasOperation("bulkdelete") {
		t.Run("bulkdelete", func(t *testing.T) {
			var bulk []state.DeleteRequest
			for _, scenario := range scenarios {
				if scenario.bulkOnly && scenario.toBeDeleted {
					t.Logf("Adding delete request to bulk for %s", scenario.key)
					bulk = append(bulk, state.DeleteRequest{
						Key: scenario.key,
					})
				}
			}
			err := statestore.BulkDelete(context.Background(), bulk)
			assert.Nil(t, err)

			for _, req := range bulk {
				t.Logf("Checking value absence for %s", req.Key)
				res, err := statestore.Get(context.Background(), &state.GetRequest{
					Key: req.Key,
				})
				assert.Nil(t, err)
				assert.Nil(t, res.Data)
			}
		})
	}

	//nolint:nestif
	if config.HasOperation("transaction") {
		t.Run("transaction", func(t *testing.T) {
			// Check if transactional feature is listed
			features := statestore.Features()
			assert.True(t, state.FeatureTransactional.IsPresent(features))

			var transactionGroups []int
			transactions := map[int][]state.TransactionalStateOperation{}
			for _, scenario := range scenarios {
				if scenario.transactionOnly {
					if transactions[scenario.transactionGroup] == nil {
						transactionGroups = append(transactionGroups, scenario.transactionGroup)
					}
					transactions[scenario.transactionGroup] = append(
						transactions[scenario.transactionGroup], state.TransactionalStateOperation{
							Operation: state.Upsert,
							Request: state.SetRequest{
								Key:   scenario.key,
								Value: scenario.value,
							},
						})

					// Deletion happens in the following transaction.
					if scenario.toBeDeleted {
						if transactions[scenario.transactionGroup+1] == nil {
							transactionGroups = append(transactionGroups, scenario.transactionGroup+1)
						}
						transactions[scenario.transactionGroup+1] = append(
							transactions[scenario.transactionGroup+1], state.TransactionalStateOperation{
								Operation: state.Delete,
								Request: state.DeleteRequest{
									Key: scenario.key,
								},
							})
					}
				}
			}

			transactionStore, ok := statestore.(state.TransactionalStore)
			assert.True(t, ok)
			sort.Ints(transactionGroups)
			for _, transactionGroup := range transactionGroups {
				t.Logf("Testing transaction #%d", transactionGroup)
				err := transactionStore.Multi(context.Background(), &state.TransactionalStateRequest{
					Operations: transactions[transactionGroup],
					// For CosmosDB
					Metadata: map[string]string{
						"partitionKey": "myPartition",
					},
				})
				require.NoError(t, err)
				for _, scenario := range scenarios {
					if scenario.transactionOnly {
						if scenario.transactionGroup == transactionGroup {
							t.Logf("Checking value presence for %s", scenario.key)
							// Data should have been inserted at this point
							res, err := statestore.Get(context.Background(), &state.GetRequest{
								Key: scenario.key,
								// For CosmosDB
								Metadata: map[string]string{
									"partitionKey": "myPartition",
								},
							})
							require.NoError(t, err)
							assertEquals(t, scenario.value, res)
						}

						if scenario.toBeDeleted && (scenario.transactionGroup == transactionGroup-1) {
							t.Logf("Checking value absence for %s", scenario.key)
							// Data should have been deleted at this point
							res, err := statestore.Get(context.Background(), &state.GetRequest{
								Key: scenario.key,
								// For CosmosDB
								Metadata: map[string]string{
									"partitionKey": "myPartition",
								},
							})
							require.NoError(t, err)
							assert.Nil(t, res.Data)
						}
					}
				}
			}
		})

		t.Run("transaction-order", func(t *testing.T) {
			// Arrange
			firstKey := "key1"
			firstValue := "value1"
			secondKey := "key2"
			secondValue := "value2"
			thirdKey := "key3"
			thirdValue := "value3"

			// for CosmosDB
			partitionMetadata := map[string]string{
				"partitionKey": "myPartition",
			}

			// prerequisite: key1 should be present
			err := statestore.Set(context.Background(), &state.SetRequest{
				Key:      firstKey,
				Value:    firstValue,
				Metadata: partitionMetadata,
			})
			assert.NoError(t, err, "set request should be successful")

			// prerequisite: key2 should not be present
			err = statestore.Delete(context.Background(), &state.DeleteRequest{
				Key:      secondKey,
				Metadata: partitionMetadata,
			})
			assert.NoError(t, err, "delete request should be successful")

			// prerequisite: key3 should not be present
			err = statestore.Delete(context.Background(), &state.DeleteRequest{
				Key:      thirdKey,
				Metadata: partitionMetadata,
			})
			assert.NoError(t, err, "delete request should be successful")

			operations := []state.TransactionalStateOperation{
				// delete an item that already exists
				{
					Operation: state.Delete,
					Request: state.DeleteRequest{
						Key: firstKey,
					},
				},
				// upsert a new item
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:   secondKey,
						Value: secondValue,
					},
				},
				// delete the item that was just upserted
				{
					Operation: state.Delete,
					Request: state.DeleteRequest{
						Key: secondKey,
					},
				},
				// upsert a new item
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:   thirdKey,
						Value: thirdValue,
					},
				},
			}

			expected := map[string][]byte{
				firstKey:  []byte(nil),
				secondKey: []byte(nil),
				thirdKey:  []byte(fmt.Sprintf("\"%s\"", thirdValue)),
			}

			// Act
			transactionStore, ok := statestore.(state.TransactionalStore)
			assert.True(t, ok)
			err = transactionStore.Multi(context.Background(), &state.TransactionalStateRequest{
				Operations: operations,
				Metadata:   partitionMetadata,
			})
			require.NoError(t, err)

			// Assert
			for k, v := range expected {
				res, err := statestore.Get(context.Background(), &state.GetRequest{
					Key:      k,
					Metadata: partitionMetadata,
				})
				require.NoError(t, err)
				assert.Equal(t, v, res.Data)
			}
		})
	} else {
		// Check if transactional feature is NOT listed
		features := statestore.Features()
		assert.False(t, state.FeatureTransactional.IsPresent(features))
	}

	// Supporting etags requires support for get, set, and delete so they are not checked individually
	if config.HasOperation("etag") {
		t.Run("etag", func(t *testing.T) {
			testKey := "etagTest"
			firstValue := []byte("testValue1")
			secondValue := []byte("testValue2")
			fakeEtag := "not-an-etag"

			// Check if eTag feature is listed
			features := statestore.Features()
			require.True(t, state.FeatureETag.IsPresent(features))

			// Delete any potential object, it's important to start from a clean slate.
			err := statestore.Delete(context.Background(), &state.DeleteRequest{
				Key: testKey,
			})
			require.NoError(t, err)

			// Set an object.
			err = statestore.Set(context.Background(), &state.SetRequest{
				Key:   testKey,
				Value: firstValue,
			})
			require.NoError(t, err)

			// Validate the set.
			res, err := statestore.Get(context.Background(), &state.GetRequest{
				Key: testKey,
			})
			require.NoError(t, err)
			assertEquals(t, firstValue, res)
			etag := res.ETag

			// Try and update with wrong ETag, expect failure.
			err = statestore.Set(context.Background(), &state.SetRequest{
				Key:   testKey,
				Value: secondValue,
				ETag:  &fakeEtag,
			})
			require.Error(t, err)

			// Try and update with corect ETag, expect success.
			err = statestore.Set(context.Background(), &state.SetRequest{
				Key:   testKey,
				Value: secondValue,
				ETag:  etag,
			})
			require.NoError(t, err)

			// Validate the set.
			res, err = statestore.Get(context.Background(), &state.GetRequest{
				Key: testKey,
			})
			require.NoError(t, err)
			assertEquals(t, secondValue, res)
			require.NotEqual(t, etag, res.ETag)
			etag = res.ETag

			// Try and delete with wrong ETag, expect failure.
			err = statestore.Delete(context.Background(), &state.DeleteRequest{
				Key:  testKey,
				ETag: &fakeEtag,
			})
			require.Error(t, err)

			// Try and delete with correct ETag, expect success.
			err = statestore.Delete(context.Background(), &state.DeleteRequest{
				Key:  testKey,
				ETag: etag,
			})
			require.NoError(t, err)
		})
	} else {
		// Check if eTag feature is NOT listed
		features := statestore.Features()
		require.False(t, state.FeatureETag.IsPresent(features))
	}

	if config.HasOperation("first-write") {
		t.Run("first-write without etag", func(t *testing.T) {
			testKey := "first-writeTest"
			firstValue := []byte("testValue1")
			secondValue := []byte("testValue2")
			emptyString := ""

			requestSets := [][2]*state.SetRequest{
				{
					{
						Key:   testKey,
						Value: firstValue,
						Options: state.SetStateOption{
							Concurrency: state.FirstWrite,
							Consistency: state.Strong,
						},
					},
					{
						Key:   testKey,
						Value: secondValue,
						Options: state.SetStateOption{
							Concurrency: state.FirstWrite,
							Consistency: state.Strong,
						},
					},
				},
				{
					{
						Key:   testKey,
						Value: firstValue,
						Options: state.SetStateOption{
							Concurrency: state.FirstWrite,
							Consistency: state.Strong,
						},
						ETag: &emptyString,
					},
					{
						Key:   testKey,
						Value: secondValue,
						Options: state.SetStateOption{
							Concurrency: state.FirstWrite,
							Consistency: state.Strong,
						},
						ETag: &emptyString,
					},
				},
			}

			for i, requestSet := range requestSets {
				t.Run(fmt.Sprintf("request set %d", i), func(t *testing.T) {
					// Delete any potential object, it's important to start from a clean slate.
					err := statestore.Delete(context.Background(), &state.DeleteRequest{
						Key: testKey,
					})
					require.NoError(t, err)

					err = statestore.Set(context.Background(), requestSet[0])
					require.NoError(t, err)

					// Validate the set.
					res, err := statestore.Get(context.Background(), &state.GetRequest{
						Key: testKey,
					})
					require.NoError(t, err)
					assertEquals(t, firstValue, res)

					// Second write expect fail
					err = statestore.Set(context.Background(), requestSet[1])
					require.Error(t, err)
				})
			}
		})

		t.Run("first-write with etag", func(t *testing.T) {
			testKey := "first-writeTest"
			firstValue := []byte("testValue1")
			secondValue := []byte("testValue2")

			request := &state.SetRequest{
				Key:   testKey,
				Value: firstValue,
			}

			// Delete any potential object, it's important to start from a clean slate.
			err := statestore.Delete(context.Background(), &state.DeleteRequest{
				Key: testKey,
			})
			require.NoError(t, err)

			err = statestore.Set(context.Background(), request)
			require.NoError(t, err)

			// Validate the set.
			res, err := statestore.Get(context.Background(), &state.GetRequest{
				Key: testKey,
			})
			require.NoError(t, err)
			assertEquals(t, firstValue, res)

			etag := res.ETag

			request = &state.SetRequest{
				Key:   testKey,
				Value: secondValue,
				ETag:  etag,
				Options: state.SetStateOption{
					Concurrency: state.FirstWrite,
					Consistency: state.Strong,
				},
			}
			err = statestore.Set(context.Background(), request)
			require.NoError(t, err)

			// Validate the set.
			res, err = statestore.Get(context.Background(), &state.GetRequest{
				Key: testKey,
			})
			require.NoError(t, err)
			require.NotEqual(t, etag, res.ETag)
			assertEquals(t, secondValue, res)

			request.ETag = etag

			// Second write expect fail
			err = statestore.Set(context.Background(), request)
			require.Error(t, err)
		})
	}

	if config.HasOperation("ttl") {
		t.Run("set and get with TTL", func(t *testing.T) {
			err := statestore.Set(context.Background(), &state.SetRequest{
				Key:   key + "-ttl",
				Value: "⏱️",
				Metadata: map[string]string{
					"ttlInSeconds": "2",
				},
			})
			require.NoError(t, err)

			// Request immediately
			res, err := statestore.Get(context.Background(), &state.GetRequest{
				Key: key + "-ttl",
			})
			require.NoError(t, err)
			assertEquals(t, "⏱️", res)

			// Wait for the object to expire and request again
			assert.Eventually(t, func() bool {
				res, err = statestore.Get(context.Background(), &state.GetRequest{
					Key: key + "-ttl",
				})
				require.NoError(t, err)
				return res.Data == nil
			}, time.Second*3, 200*time.Millisecond, "expected object to have been deleted in time")
		})
	}
}

func assertEquals(t *testing.T, value any, res *state.GetResponse) {
	t.Helper()
	assertDataEquals(t, value, res.Data)
}

func assertDataEquals(t *testing.T, expect any, actual []byte) {
	t.Helper()
	switch v := expect.(type) {
	case intValueType:
		// Custom type requires case mapping
		if err := json.Unmarshal(actual, &v); err != nil {
			assert.Failf(t, "unmarshal error", "error: %v, json: %s", err, string(actual))
		}
		assert.Equal(t, expect, v)
	case ValueType:
		// Custom type requires case mapping
		if err := json.Unmarshal(actual, &v); err != nil {
			assert.Failf(t, "unmarshal error", "error: %v, json: %s", err, string(actual))
		}
		assert.Equal(t, expect, v)
	case int:
		// json.Unmarshal to float64 by default, case mapping to int coerces to int type
		if err := json.Unmarshal(actual, &v); err != nil {
			assert.Failf(t, "unmarshal error", "error: %v, json: %s", err, string(actual))
		}
		assert.Equal(t, expect, v)
	case []byte:
		assert.Equal(t, expect, actual)
	default:
		// Other golang primitive types (string, bool ...)
		if err := json.Unmarshal(actual, &v); err != nil {
			assert.Failf(t, "unmarshal error", "error: %v, json: %s", err, string(actual))
		}
		assert.Equal(t, expect, v)
	}
}
