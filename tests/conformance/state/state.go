// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/tests/conformance/utils"
)

type ValueType struct {
	Message string `json:"message"`
}

type scenario struct {
	key                  string
	value                interface{}
	expectedReadResponse []byte
	toBeDeleted          bool
	bulkOnly             bool
	transactionOnly      bool
	transactionGroup     int
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
			key:                  fmt.Sprintf("%s-int", key),
			value:                123,
			expectedReadResponse: []byte("123"),
		},
		{
			key:                  fmt.Sprintf("%s-bool", key),
			value:                true,
			expectedReadResponse: []byte("true"),
		},
		{
			key:                  fmt.Sprintf("%s-bytes", key),
			value:                []byte{0x1},
			expectedReadResponse: []byte{0x1},
		},
		{
			key:                  fmt.Sprintf("%s-string-with-json", key),
			value:                "{\"a\":\"b\"}",
			expectedReadResponse: []byte("\"{\\\"a\\\":\\\"b\\\"}\""),
		},
		{
			key:                  fmt.Sprintf("%s-string", key),
			value:                "hello world",
			expectedReadResponse: []byte("\"hello world\""),
		},
		{
			key:                  fmt.Sprintf("%s-struct", key),
			value:                ValueType{Message: "test"},
			expectedReadResponse: []byte("{\"message\":\"test\"}"),
		},
		{
			key:                  fmt.Sprintf("%s-to-be-deleted", key),
			value:                "to be deleted",
			expectedReadResponse: []byte("\"to be deleted\""),
			toBeDeleted:          true,
		},
		{
			key:                  fmt.Sprintf("%s-bulk-int", key),
			value:                123,
			expectedReadResponse: []byte("123"),
			bulkOnly:             true,
		},
		{
			key:                  fmt.Sprintf("%s-bulk-bool", key),
			value:                true,
			expectedReadResponse: []byte("true"),
			bulkOnly:             true,
		},
		{
			key:                  fmt.Sprintf("%s-bulk-bytes", key),
			value:                []byte{0x1},
			expectedReadResponse: []byte{0x1},
			bulkOnly:             true,
		},
		{
			key:                  fmt.Sprintf("%s-bulk-string", key),
			value:                "hello world",
			expectedReadResponse: []byte("\"hello world\""),
			bulkOnly:             true,
		},
		{
			key:                  fmt.Sprintf("%s-bulk-struct", key),
			value:                ValueType{Message: "test"},
			expectedReadResponse: []byte("{\"message\":\"test\"}"),
			bulkOnly:             true,
		},
		{
			key:                  fmt.Sprintf("%s-bulk-to-be-deleted", key),
			value:                "to be deleted",
			expectedReadResponse: []byte("\"to be deleted\""),
			toBeDeleted:          true,
			bulkOnly:             true,
		},
		{
			key:                  fmt.Sprintf("%s-bulk-to-be-deleted-too", key),
			value:                "to be deleted too",
			expectedReadResponse: []byte("\"to be deleted too\""),
			toBeDeleted:          true,
			bulkOnly:             true,
		},
		{
			key:                  fmt.Sprintf("%s-trx-int", key),
			value:                123,
			expectedReadResponse: []byte("123"),
			transactionOnly:      true,
			transactionGroup:     1,
		},
		{
			key:                  fmt.Sprintf("%s-trx-bool", key),
			value:                true,
			expectedReadResponse: []byte("true"),
			transactionOnly:      true,
			transactionGroup:     1,
		},
		{
			key:                  fmt.Sprintf("%s-trx-bytes", key),
			value:                []byte{0x1},
			expectedReadResponse: []byte{0x1},
			transactionOnly:      true,
			transactionGroup:     1,
		},
		{
			key:                  fmt.Sprintf("%s-trx-string", key),
			value:                "hello world",
			expectedReadResponse: []byte("\"hello world\""),
			transactionOnly:      true,
			transactionGroup:     1,
		},
		{
			key:                  fmt.Sprintf("%s-trx-struct", key),
			value:                ValueType{Message: "test"},
			expectedReadResponse: []byte("{\"message\":\"test\"}"),
			transactionOnly:      true,
			transactionGroup:     2,
		},
		{
			key:                  fmt.Sprintf("%s-trx-to-be-deleted", key),
			value:                "to be deleted",
			expectedReadResponse: []byte("\"to be deleted\""),
			toBeDeleted:          true,
			transactionOnly:      true,
			transactionGroup:     1,
		},
		{
			key:                  fmt.Sprintf("%s-trx-to-be-deleted-too", key),
			value:                "to be deleted too",
			expectedReadResponse: []byte("\"to be deleted too\""),
			toBeDeleted:          true,
			transactionOnly:      true,
			transactionGroup:     3,
		},
	}

	t.Run("init", func(t *testing.T) {
		err := statestore.Init(state.Metadata{
			Properties: props,
		})
		assert.Nil(t, err)
	})

	t.Run("ping", func(t *testing.T) {
		err := statestore.Ping()
		assert.Nil(t, err)
	})

	if config.HasOperation("set") {
		t.Run("set", func(t *testing.T) {
			for _, scenario := range scenarios {
				if !scenario.bulkOnly && !scenario.transactionOnly {
					t.Logf("Setting value for %s", scenario.key)
					err := statestore.Set(&state.SetRequest{
						Key:   scenario.key,
						Value: scenario.value,
					})
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
					res, err := statestore.Get(&state.GetRequest{
						Key: scenario.key,
					})
					assert.Nil(t, err)
					assert.Equal(t, scenario.expectedReadResponse, res.Data)
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
					err := statestore.Delete(&state.DeleteRequest{
						Key: scenario.key,
					})
					assert.Nil(t, err)

					t.Logf("Checking value absence for %s", scenario.key)
					res, err := statestore.Get(&state.GetRequest{
						Key: scenario.key,
					})
					assert.Nil(t, err)
					assert.Nil(t, res.Data)
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
			err := statestore.BulkSet(bulk)
			assert.Nil(t, err)

			for _, scenario := range scenarios {
				if scenario.bulkOnly {
					t.Logf("Checking value presence for %s", scenario.key)
					// Data should have been inserted at this point
					res, err := statestore.Get(&state.GetRequest{
						Key: scenario.key,
					})
					assert.Nil(t, err)
					assert.Equal(t, scenario.expectedReadResponse, res.Data)
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
			err := statestore.BulkDelete(bulk)
			assert.Nil(t, err)

			for _, req := range bulk {
				t.Logf("Checking value absence for %s", req.Key)
				res, err := statestore.Get(&state.GetRequest{
					Key: req.Key,
				})
				assert.Nil(t, err)
				assert.Nil(t, res.Data)
			}
		})
	}

	// nolint: nestif
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

			transactionStore := statestore.(state.TransactionalStore)
			sort.Ints(transactionGroups)
			for _, transactionGroup := range transactionGroups {
				t.Logf("Testing transaction #%d", transactionGroup)
				err := transactionStore.Multi(&state.TransactionalStateRequest{
					Operations: transactions[transactionGroup],
					// For CosmosDB
					Metadata: map[string]string{
						"partitionKey": "myPartition",
					},
				})
				assert.Nil(t, err)
				for _, scenario := range scenarios {
					if scenario.transactionOnly {
						if scenario.transactionGroup == transactionGroup {
							t.Logf("Checking value presence for %s", scenario.key)
							// Data should have been inserted at this point
							res, err := statestore.Get(&state.GetRequest{
								Key: scenario.key,
								// For CosmosDB
								Metadata: map[string]string{
									"partitionKey": "myPartition",
								},
							})
							assert.Nil(t, err)
							assert.Equal(t, scenario.expectedReadResponse, res.Data)
						}

						if scenario.toBeDeleted && (scenario.transactionGroup == transactionGroup-1) {
							t.Logf("Checking value absence for %s", scenario.key)
							// Data should have been deleted at this point
							res, err := statestore.Get(&state.GetRequest{
								Key: scenario.key,
								// For CosmosDB
								Metadata: map[string]string{
									"partitionKey": "myPartition",
								},
							})
							assert.Nil(t, err)
							assert.Nil(t, res.Data)
						}
					}
				}
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
			assert.True(t, state.FeatureETag.IsPresent(features))

			// Delete any potential object, it's important to start from a clean slate.
			err := statestore.Delete(&state.DeleteRequest{
				Key: testKey,
			})
			assert.Nil(t, err)

			// Set an object.
			err = statestore.Set(&state.SetRequest{
				Key:   testKey,
				Value: firstValue,
			})
			assert.Nil(t, err)

			// Validate the set.
			res, err := statestore.Get(&state.GetRequest{
				Key: testKey,
			})

			assert.Nil(t, err)
			assert.Equal(t, firstValue, res.Data)
			etag := res.ETag

			// Try and update with wrong ETag, expect failure.
			err = statestore.Set(&state.SetRequest{
				Key:   testKey,
				Value: secondValue,
				ETag:  &fakeEtag,
			})
			assert.NotNil(t, err)

			// Try and update with corect ETag, expect success.
			err = statestore.Set(&state.SetRequest{
				Key:   testKey,
				Value: secondValue,
				ETag:  etag,
			})
			assert.Nil(t, err)

			// Validate the set.
			res, err = statestore.Get(&state.GetRequest{
				Key: testKey,
			})
			assert.Nil(t, err)
			assert.Equal(t, secondValue, res.Data)
			assert.NotEqual(t, etag, res.ETag)
			etag = res.ETag

			// Try and delete with wrong ETag, expect failure.
			err = statestore.Delete(&state.DeleteRequest{
				Key:  testKey,
				ETag: &fakeEtag,
			})
			assert.NotNil(t, err)

			// Try and delete with correct ETag, expect success.
			err = statestore.Delete(&state.DeleteRequest{
				Key:  testKey,
				ETag: etag,
			})
			assert.Nil(t, err)
		})
	} else {
		// Check if eTag feature is NOT listed
		features := statestore.Features()
		assert.False(t, state.FeatureETag.IsPresent(features))
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
					}, {
						Key:   testKey,
						Value: secondValue,
						Options: state.SetStateOption{
							Concurrency: state.FirstWrite,
							Consistency: state.Strong,
						},
					},
				},
				{{
					Key:   testKey,
					Value: firstValue,
					Options: state.SetStateOption{
						Concurrency: state.FirstWrite,
						Consistency: state.Strong,
					},
					ETag: &emptyString,
				}, {
					Key:   testKey,
					Value: secondValue,
					Options: state.SetStateOption{
						Concurrency: state.FirstWrite,
						Consistency: state.Strong,
					},
					ETag: &emptyString,
				}},
			}

			for _, requestSet := range requestSets {
				// Delete any potential object, it's important to start from a clean slate.
				err := statestore.Delete(&state.DeleteRequest{
					Key: testKey,
				})
				assert.Nil(t, err)

				err = statestore.Set(requestSet[0])
				assert.Nil(t, err)

				// Validate the set.
				res, err := statestore.Get(&state.GetRequest{
					Key: testKey,
				})
				assert.Nil(t, err)
				assert.Equal(t, firstValue, res.Data)

				// Second write expect fail
				err = statestore.Set(requestSet[1])
				assert.NotNil(t, err)
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
			err := statestore.Delete(&state.DeleteRequest{
				Key: testKey,
			})
			assert.Nil(t, err)

			err = statestore.Set(request)
			assert.Nil(t, err)

			// Validate the set.
			res, err := statestore.Get(&state.GetRequest{
				Key: testKey,
			})
			assert.Nil(t, err)
			assert.Equal(t, firstValue, res.Data)

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
			err = statestore.Set(request)
			assert.Nil(t, err)

			// Validate the set.
			res, err = statestore.Get(&state.GetRequest{
				Key: testKey,
			})
			assert.Nil(t, err)
			assert.NotEqual(t, etag, res.ETag)
			assert.Equal(t, secondValue, res.Data)

			request.ETag = etag

			// Second write expect fail
			err = statestore.Set(request)
			assert.NotNil(t, err)
		})
	}
}
