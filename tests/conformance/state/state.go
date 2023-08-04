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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/dapr/components-contrib/contenttype"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/dapr/kit/config"
	"github.com/dapr/kit/ptr"
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

	BadEtag string `mapstructure:"badEtag"`
}

func NewTestConfig(component string, operations []string, configMap map[string]interface{}) (TestConfig, error) {
	testConfig := TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "state",
			ComponentName: component,
			Operations:    utils.NewStringSet(operations...),
		},
		BadEtag: "bad-etag",
	}

	err := config.Decode(configMap, &testConfig)
	if err != nil {
		return testConfig, err
	}

	return testConfig, nil
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
		err := statestore.Init(context.Background(), state.Metadata{Base: metadata.Base{
			Properties: props,
		}})
		assert.NoError(t, err)
	})

	// Don't run more tests if init failed
	if t.Failed() {
		t.Fatal("Init failed, stopping further tests")
	}

	t.Run("ping", func(t *testing.T) {
		err := state.Ping(context.Background(), statestore)
		// TODO: Ideally, all stable components should implenment ping function,
		// so will only assert assert.NoError(t, err) finally, i.e. when current implementation
		// implements ping in existing stable components
		if err != nil {
			assert.EqualError(t, err, "ping is not implemented by this state store")
		} else {
			assert.NoError(t, err)
		}
	})

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
				assert.NoError(t, err)
			}
		}
	})

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

	if config.HasOperation("query") {
		t.Run("query", func(t *testing.T) {
			// Check if query feature is listed
			features := statestore.Features()
			require.True(t, state.FeatureQueryAPI.IsPresent(features))

			querier, ok := statestore.(state.Querier)
			assert.True(t, ok, "Querier interface is not implemented")
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
	} else {
		t.Run("query API feature not present", func(t *testing.T) {
			features := statestore.Features()
			assert.False(t, state.FeatureQueryAPI.IsPresent(features))
		})
	}

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
		err := statestore.BulkSet(context.Background(), bulk, state.BulkStoreOpts{})
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

	t.Run("bulkget", func(t *testing.T) {
		tests := []struct {
			name   string
			req    []state.GetRequest
			expect map[string]any
		}{
			{name: "scenario", req: []state.GetRequest{}, expect: map[string]any{}},
			{name: "include non-existent key", req: []state.GetRequest{{Key: "doesnotexist"}}, expect: map[string]any{"doesnotexist": nil}},
		}

		// Build test cases
		first := true
		for _, scenario := range scenarios {
			if scenario.bulkOnly {
				t.Logf("Adding get request to bulk for %s", scenario.key)
				tests[0].req = append(tests[0].req, state.GetRequest{
					Key: scenario.key,
				})
				tests[0].expect[scenario.key] = scenario.value

				if first {
					tests[1].req = append(tests[1].req, state.GetRequest{
						Key: scenario.key,
					})
					tests[1].expect[scenario.key] = scenario.value
					first = false
				}
			}
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				res, err := statestore.BulkGet(context.Background(), tt.req, state.BulkGetOpts{})
				require.NoError(t, err)
				require.Len(t, res, len(tt.expect))

				for _, r := range res {
					t.Logf("Checking value equality %s", r.Key)
					val, ok := tt.expect[r.Key]
					if assert.Empty(t, r.Error) && assert.True(t, ok) {
						assertDataEquals(t, val, r.Data)
					}
					delete(tt.expect, r.Key)
				}
			})
		}
	})

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
		err := statestore.BulkDelete(context.Background(), bulk, state.BulkStoreOpts{})
		assert.NoError(t, err)

		for _, req := range bulk {
			t.Logf("Checking value absence for %s", req.Key)
			res, err := statestore.Get(context.Background(), &state.GetRequest{
				Key: req.Key,
			})
			assert.NoError(t, err)
			assert.Nil(t, res.Data)
		}
	})

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
						transactions[scenario.transactionGroup],
						state.SetRequest{
							Key:   scenario.key,
							Value: scenario.value,
						},
					)

					// Deletion happens in the following transaction.
					if scenario.toBeDeleted {
						if transactions[scenario.transactionGroup+1] == nil {
							transactionGroups = append(transactionGroups, scenario.transactionGroup+1)
						}
						transactions[scenario.transactionGroup+1] = append(
							transactions[scenario.transactionGroup+1],
							state.DeleteRequest{
								Key: scenario.key,
							},
						)
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
			firstKey := key + "-key1"
			firstValue := "value1"
			secondKey := key + "-key2"
			secondValue := "value2"
			thirdKey := key + "-key3"
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
				state.DeleteRequest{
					Key: firstKey,
				},
				// upsert a new item
				state.SetRequest{
					Key:   secondKey,
					Value: secondValue,
				},
				// delete the item that was just upserted
				state.DeleteRequest{
					Key: secondKey,
				},
				// upsert a new item
				state.SetRequest{
					Key:   thirdKey,
					Value: thirdValue,
				},
			}

			expected := map[string][]byte{
				firstKey:  []byte(nil),
				secondKey: []byte(nil),
				thirdKey:  []byte(strconv.Quote(thirdValue)),
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
		t.Run("transactional feature not present", func(t *testing.T) {
			features := statestore.Features()
			assert.False(t, state.FeatureTransactional.IsPresent(features))
		})
	}

	// Supporting etags requires support for get, set, and delete so they are not checked individually
	if config.HasOperation("etag") {
		t.Run("etag", func(t *testing.T) {
			var (
				etagErr      *state.ETagError
				bulkStoreErr state.BulkStoreError
				testKeys     = [4]string{key + "-etag1", key + "-etag2", key + "-etag3", key + "-etag4"}
				etags        [4]string
			)
			const (
				firstValue  = "first-value"
				secondValue = "second-value"
				thirdValue  = "third-value"
			)

			// Check if eTag feature is listed
			features := statestore.Features()
			require.True(t, state.FeatureETag.IsPresent(features))

			// Set some objects (no etag as they are new)
			err := statestore.BulkSet(context.Background(), []state.SetRequest{
				{Key: testKeys[0], Value: firstValue},
				{Key: testKeys[1], Value: firstValue},
				{Key: testKeys[2], Value: firstValue},
				{Key: testKeys[3], Value: firstValue},
			}, state.BulkStoreOpts{})
			require.NoError(t, err)

			// Validate the set, using both regular Get and BulkGet
			res, err := statestore.Get(context.Background(), &state.GetRequest{
				Key: testKeys[0],
			})
			require.NoError(t, err)
			require.NotNil(t, res.ETag)
			require.NotEmpty(t, *res.ETag)
			assertEquals(t, firstValue, res)
			etags[0] = *res.ETag

			bulkRes, err := statestore.BulkGet(context.Background(), []state.GetRequest{
				{Key: testKeys[1]},
				{Key: testKeys[2]},
				{Key: testKeys[3]},
			}, state.BulkGetOpts{})
			require.NoError(t, err)
			require.Len(t, bulkRes, 3)
			for i := 0; i < 3; i++ {
				require.NotNil(t, bulkRes[i].ETag)
				require.NotEmpty(t, *bulkRes[i].ETag)
				assertDataEquals(t, firstValue, bulkRes[i].Data)
				switch bulkRes[i].Key {
				case testKeys[1]:
					etags[1] = *bulkRes[i].ETag
				case testKeys[2]:
					etags[2] = *bulkRes[i].ETag
				case testKeys[3]:
					etags[3] = *bulkRes[i].ETag
				}
			}

			// Try and update with wrong ETag, expect failure
			err = statestore.Set(context.Background(), &state.SetRequest{
				Key:   testKeys[0],
				Value: secondValue,
				ETag:  &config.BadEtag,
			})
			require.Error(t, err)
			require.ErrorAs(t, err, &etagErr)
			assert.Equal(t, state.ETagMismatch, etagErr.Kind())

			// Try and update with Set and corect ETag, expect success
			err = statestore.Set(context.Background(), &state.SetRequest{
				Key:   testKeys[0],
				Value: secondValue,
				ETag:  &etags[0],
			})
			require.NoError(t, err)

			// Validate the Set
			res, err = statestore.Get(context.Background(), &state.GetRequest{
				Key: testKeys[0],
			})
			require.NoError(t, err)
			assertEquals(t, secondValue, res)
			require.NotNil(t, res.ETag)
			require.NotEqual(t, etags[0], *res.ETag)
			etags[0] = *res.ETag

			// Try and update bulk with one ETag wrong, expect partial success
			err = statestore.BulkSet(context.Background(), []state.SetRequest{
				{Key: testKeys[1], Value: secondValue, ETag: &config.BadEtag},
				{Key: testKeys[2], Value: secondValue, ETag: &etags[2]},
			}, state.BulkStoreOpts{})
			require.Error(t, err)
			unwrapErr, ok := err.(interface{ Unwrap() []error })
			require.True(t, ok, "Returned error is not a joined error")
			errs := unwrapErr.Unwrap()
			require.Len(t, errs, 1)
			require.ErrorAs(t, errs[0], &bulkStoreErr)
			assert.Equal(t, testKeys[1], bulkStoreErr.Key())
			etagErr = bulkStoreErr.ETagError()
			require.NotNil(t, etagErr)
			assert.Equal(t, state.ETagMismatch, etagErr.Kind())

			// Validate: key 1 should be unchanged, and key 2 should be changed
			res, err = statestore.Get(context.Background(), &state.GetRequest{
				Key: testKeys[1],
			})
			require.NoError(t, err)
			assertEquals(t, firstValue, res)
			require.NotNil(t, res.ETag)
			require.Equal(t, etags[1], *res.ETag)

			res, err = statestore.Get(context.Background(), &state.GetRequest{
				Key: testKeys[2],
			})
			require.NoError(t, err)
			assertEquals(t, secondValue, res)
			require.NotNil(t, res.ETag)
			require.NotEqual(t, etags[2], *res.ETag)
			etags[2] = *res.ETag

			// Update bulk with valid etags
			err = statestore.BulkSet(context.Background(), []state.SetRequest{
				{Key: testKeys[1], Value: thirdValue, ETag: &etags[1]},
				{Key: testKeys[2], Value: thirdValue, ETag: &etags[2]},
			}, state.BulkStoreOpts{})
			require.NoError(t, err)

			// Validate
			bulkRes, err = statestore.BulkGet(context.Background(), []state.GetRequest{
				{Key: testKeys[1]},
				{Key: testKeys[2]},
			}, state.BulkGetOpts{})
			require.NoError(t, err)
			require.Len(t, bulkRes, 2)
			for i := 0; i < 2; i++ {
				require.NotNil(t, bulkRes[i].ETag)
				require.NotEmpty(t, *bulkRes[i].ETag)
				assertDataEquals(t, thirdValue, bulkRes[i].Data)
				switch bulkRes[i].Key {
				case testKeys[1]:
					etags[1] = *bulkRes[i].ETag
				case testKeys[2]:
					etags[2] = *bulkRes[i].ETag
				}
			}

			// Try and delete with wrong ETag, expect failure
			err = statestore.Delete(context.Background(), &state.DeleteRequest{
				Key:  testKeys[0],
				ETag: &config.BadEtag,
			})
			require.Error(t, err)
			require.ErrorAs(t, err, &etagErr)
			assert.NotEmpty(t, etagErr.Kind())

			// Try and delete with correct ETag, expect success
			err = statestore.Delete(context.Background(), &state.DeleteRequest{
				Key:  testKeys[0],
				ETag: &etags[0],
			})
			require.NoError(t, err)

			// Validate missing
			res, err = statestore.Get(context.Background(), &state.GetRequest{
				Key: testKeys[0],
			})
			require.NoError(t, err)
			require.Empty(t, res.Data)
			require.Empty(t, res.ETag)

			// Try and delete bulk with two ETag's wrong, expect partial success
			err = statestore.BulkDelete(context.Background(), []state.DeleteRequest{
				{Key: testKeys[1], ETag: &etags[1]},
				{Key: testKeys[2], ETag: &config.BadEtag},
			}, state.BulkStoreOpts{})
			require.Error(t, err)
			unwrapErr, ok = err.(interface{ Unwrap() []error })
			require.True(t, ok, "Returned error is not a joined error")
			errs = unwrapErr.Unwrap()
			require.Len(t, errs, 1)
			require.ErrorAs(t, errs[0], &bulkStoreErr)
			assert.Equal(t, testKeys[2], bulkStoreErr.Key())
			etagErr = bulkStoreErr.ETagError()
			require.NotNil(t, etagErr)
			assert.Equal(t, state.ETagMismatch, etagErr.Kind())

			// Validate key 1 missing
			res, err = statestore.Get(context.Background(), &state.GetRequest{
				Key: testKeys[1],
			})
			require.NoError(t, err)
			require.Empty(t, res.Data)
			require.Empty(t, res.ETag)

			// Validate key 2 unchanged
			res, err = statestore.Get(context.Background(), &state.GetRequest{
				Key: testKeys[2],
			})
			require.NoError(t, err)
			assertEquals(t, thirdValue, res)
			require.NotNil(t, res.ETag)
			require.Equal(t, etags[2], *res.ETag)

			// Try and delete bulk with valid ETags
			err = statestore.BulkDelete(context.Background(), []state.DeleteRequest{
				{Key: testKeys[2], ETag: &etags[2]},
				{Key: testKeys[3], ETag: &etags[3]},
			}, state.BulkStoreOpts{})
			require.NoError(t, err)

			// Validate keys missing
			bulkRes, err = statestore.BulkGet(context.Background(), []state.GetRequest{
				{Key: testKeys[2]},
				{Key: testKeys[3]},
			}, state.BulkGetOpts{})
			require.NoError(t, err)
			require.Len(t, bulkRes, 2)
			foundKeys := []string{}
			for i := 0; i < 2; i++ {
				require.Empty(t, bulkRes[i].Data)
				require.Empty(t, bulkRes[i].ETag)
				foundKeys = append(foundKeys, bulkRes[i].Key)
			}
			expectKeys := []string{
				testKeys[2],
				testKeys[3],
			}
			slices.Sort(foundKeys)
			slices.Sort(expectKeys)
			assert.EqualValues(t, expectKeys, foundKeys)
		})
	} else {
		t.Run("etag feature not present", func(t *testing.T) {
			features := statestore.Features()
			require.False(t, state.FeatureETag.IsPresent(features))
		})
	}

	if config.HasOperation("first-write") {
		t.Run("first-write without etag", func(t *testing.T) {
			testKey := key + "-firstwrite-test"
			firstValue := []byte("testValue1")
			secondValue := []byte("testValue2")

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
						ETag: ptr.Of(""),
					},
					{
						Key:   testKey,
						Value: secondValue,
						Options: state.SetStateOption{
							Concurrency: state.FirstWrite,
							Consistency: state.Strong,
						},
						ETag: ptr.Of(""),
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

					// Set the value
					err = statestore.Set(context.Background(), requestSet[0])
					require.NoError(t, err)

					// Validate the set
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
			testKey := key + "-firstwrite-etag-test"
			firstValue := []byte("testValue1")
			secondValue := []byte("testValue2")

			request := &state.SetRequest{
				Key:   testKey,
				Value: firstValue,
			}

			err := statestore.Set(context.Background(), request)
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
		t.Run("set ttl with bad value should error", func(t *testing.T) {
			require.Error(t, statestore.Set(context.Background(), &state.SetRequest{
				Key:   key + "-ttl",
				Value: "⏱️",
				Metadata: map[string]string{
					"ttlInSeconds": "foo",
				},
			}))
		})

		t.Run("set and get with TTL", func(t *testing.T) {
			// Check if ttl feature is listed
			features := statestore.Features()
			require.True(t, state.FeatureTTL.IsPresent(features))

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
	} else {
		t.Run("ttl feature not present", func(t *testing.T) {
			// We skip this check for Cloudflare Workers KV
			// Even though the component supports TTLs, it's not tested in the conformance tests because the minimum TTL for the component is 1 minute, and the state store doesn't have strong consistency
			if config.ComponentName == "cloudflare.workerskv" {
				t.Skip()
			}

			features := statestore.Features()
			require.False(t, state.FeatureTTL.IsPresent(features))
		})

		t.Run("no TTL should not return any expire time", func(t *testing.T) {
			err := statestore.Set(context.Background(), &state.SetRequest{
				Key:      key + "-no-ttl",
				Value:    "⏱️",
				Metadata: map[string]string{},
			})
			require.NoError(t, err)

			// Request immediately
			res, err := statestore.Get(context.Background(), &state.GetRequest{Key: key + "-no-ttl"})
			require.NoError(t, err)
			assertEquals(t, "⏱️", res)

			assert.NotContains(t, res.Metadata, "ttlExpireTime")
		})

		t.Run("ttlExpireTime", func(t *testing.T) {
			if !config.HasOperation("transaction") {
				// This test is only for state stores that support transactions
				return
			}

			unsupported := []string{
				"redis.v6",
				"redis.v7",
				"etcd.v1",
			}

			for _, noSup := range unsupported {
				if strings.Contains(config.ComponentName, noSup) {
					t.Skipf("skipping test for unsupported state store %s", noSup)
				}
			}

			t.Run("set and get expire time", func(t *testing.T) {
				now := time.Now()
				err := statestore.Set(context.Background(), &state.SetRequest{
					Key:   key + "-ttl-expire-time",
					Value: "⏱️",
					Metadata: map[string]string{
						// Expire in an hour.
						"ttlInSeconds": "3600",
					},
				})
				require.NoError(t, err)

				// Request immediately
				res, err := statestore.Get(context.Background(), &state.GetRequest{
					Key: key + "-ttl-expire-time",
				})
				require.NoError(t, err)
				assertEquals(t, "⏱️", res)

				require.Containsf(t, res.Metadata, "ttlExpireTime", "expected metadata to contain ttlExpireTime")
				expireTime, err := time.Parse(time.RFC3339, res.Metadata["ttlExpireTime"])
				require.NoError(t, err)
				assert.InDelta(t, now.Add(time.Hour).UnixMilli(), expireTime.UnixMilli(), float64(time.Minute*10))
			})

			t.Run("ttl set to -1 should remove the TTL of a state store key", func(t *testing.T) {
				req := func(meta map[string]string) *state.SetRequest {
					return &state.SetRequest{
						Key:      key + "-ttl-expire-time-minus-1",
						Value:    "⏱️",
						Metadata: meta,
					}
				}

				require.NoError(t, statestore.Set(context.Background(), req(map[string]string{
					// Expire in 2 seconds.
					"ttlInSeconds": "2",
				})))

				// Request immediately
				res, err := statestore.Get(context.Background(), &state.GetRequest{
					Key: key + "-ttl-expire-time-minus-1",
				})
				require.NoError(t, err)
				assertEquals(t, "⏱️", res)
				assert.Contains(t, res.Metadata, "ttlExpireTime")

				// Remove TTL by setting a value of -1.
				require.NoError(t, statestore.Set(context.Background(), req(map[string]string{
					"ttlInSeconds": "-1",
				})))
				res, err = statestore.Get(context.Background(), &state.GetRequest{
					Key: key + "-ttl-expire-time-minus-1",
				})
				require.NoError(t, err)
				assertEquals(t, "⏱️", res)
				assert.NotContains(t, res.Metadata, "ttlExpireTime")

				// Ensure that the key is not expired after previous TTL.
				time.Sleep(3 * time.Second)

				res, err = statestore.Get(context.Background(), &state.GetRequest{
					Key: key + "-ttl-expire-time-minus-1",
				})
				require.NoError(t, err)
				assertEquals(t, "⏱️", res)

				// Set a new TTL.
				require.NoError(t, statestore.Set(context.Background(), req(map[string]string{
					"ttlInSeconds": "2",
				})))
				res, err = statestore.Get(context.Background(), &state.GetRequest{
					Key: key + "-ttl-expire-time-minus-1",
				})
				require.NoError(t, err)
				assertEquals(t, "⏱️", res)
				assert.Contains(t, res.Metadata, "ttlExpireTime")

				// Remove TTL by omitting the ttlInSeconds field.
				require.NoError(t, statestore.Set(context.Background(), req(map[string]string{})))
				res, err = statestore.Get(context.Background(), &state.GetRequest{
					Key: key + "-ttl-expire-time-minus-1",
				})
				require.NoError(t, err)
				assertEquals(t, "⏱️", res)
				assert.NotContains(t, res.Metadata, "ttlExpireTime")

				// Ensure key is not expired after previous TTL.
				time.Sleep(3 * time.Second)
				res, err = statestore.Get(context.Background(), &state.GetRequest{
					Key: key + "-ttl-expire-time-minus-1",
				})
				require.NoError(t, err)
				assertEquals(t, "⏱️", res)
				assert.NotContains(t, res.Metadata, "ttlExpireTime")
			})

			t.Run("set and get expire time bulkGet", func(t *testing.T) {
				now := time.Now()
				require.NoError(t, statestore.Set(context.Background(), &state.SetRequest{
					Key:      key + "-ttl-expire-time-bulk-1",
					Value:    "123",
					Metadata: map[string]string{"ttlInSeconds": "3600"},
				}))

				require.NoError(t, statestore.Set(context.Background(), &state.SetRequest{
					Key:      key + "-ttl-expire-time-bulk-2",
					Value:    "234",
					Metadata: map[string]string{"ttlInSeconds": "3600"},
				}))

				// Request immediately
				res, err := statestore.BulkGet(context.Background(), []state.GetRequest{
					{Key: key + "-ttl-expire-time-bulk-1"},
					{Key: key + "-ttl-expire-time-bulk-2"},
				}, state.BulkGetOpts{})
				require.NoError(t, err)

				require.Len(t, res, 2)
				sort.Slice(res, func(i, j int) bool {
					return res[i].Key < res[j].Key
				})

				assert.Equal(t, key+"-ttl-expire-time-bulk-1", res[0].Key)
				assert.Equal(t, key+"-ttl-expire-time-bulk-2", res[1].Key)
				assert.Equal(t, []byte(`"123"`), res[0].Data)
				assert.Equal(t, []byte(`"234"`), res[1].Data)

				for i := range res {
					if config.HasOperation("transaction") {
						require.Containsf(t, res[i].Metadata, "ttlExpireTime", "expected metadata to contain ttlExpireTime")
						expireTime, err := time.Parse(time.RFC3339, res[i].Metadata["ttlExpireTime"])
						require.NoError(t, err)
						// Check the expire time is returned and is in a 10 minute window. This
						// window should be _more_ than enough.
						assert.InDelta(t, now.Add(time.Hour).UnixMilli(), expireTime.UnixMilli(), float64(time.Minute*10))
					} else {
						assert.NotContains(t, res[i].Metadata, "ttlExpireTime")
					}
				}
			})
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
	case nil:
		assert.Empty(t, actual)
	default:
		// Other golang primitive types (string, bool ...)
		if err := json.Unmarshal(actual, &v); err != nil {
			assert.Failf(t, "unmarshal error", "error: %v, json: %s", err, string(actual))
		}
		assert.Equal(t, expect, v)
	}
}
