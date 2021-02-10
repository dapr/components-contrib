// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/sets"
)

type ValueType struct {
	Message string `json:"message"`
}

const (
	defaultDuration         = 10 * time.Millisecond
	defaultBulkRequestCount = 10
)

type TestConfig struct {
	utils.CommonConfig
	maxInitDuration   time.Duration
	maxSetDuration    time.Duration
	maxGetDuration    time.Duration
	maxDeleteDuration time.Duration
	numBulkRequests   int
}

func NewTestConfig(component string, allOperations bool, operations []string, conf map[string]string) TestConfig {
	tc := TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "state",
			ComponentName: component,
			AllOperations: allOperations,
			Operations:    sets.NewString(operations...),
		},
		maxInitDuration:   defaultDuration,
		maxSetDuration:    defaultDuration,
		maxGetDuration:    defaultDuration,
		maxDeleteDuration: defaultDuration,
		numBulkRequests:   defaultBulkRequestCount,
	}
	if val, ok := conf["maxInitDuration"]; ok {
		v, err := strconv.Atoi(val)
		if err == nil {
			tc.maxInitDuration = time.Duration(v) * time.Millisecond
		}
	}
	if val, ok := conf["maxSetDuration"]; ok {
		v, err := strconv.Atoi(val)
		if err == nil {
			tc.maxSetDuration = time.Duration(v) * time.Millisecond
		}
	}
	if val, ok := conf["maxGetDuration"]; ok {
		v, err := strconv.Atoi(val)
		if err == nil {
			tc.maxGetDuration = time.Duration(v) * time.Millisecond
		}
	}
	if val, ok := conf["maxDeleteDuration"]; ok {
		v, err := strconv.Atoi(val)
		if err == nil {
			tc.maxDeleteDuration = time.Duration(v) * time.Millisecond
		}
	}
	if val, ok := conf["numBulkRequests"]; ok {
		v, err := strconv.Atoi(val)
		if err == nil {
			tc.numBulkRequests = v
		}
	}

	return tc
}

/*
	State store component tests
*/
func ConformanceTests(t *testing.T, props map[string]string, statestore state.Store, config TestConfig) {
	// Test vars
	key := strings.ReplaceAll(uuid.New().String(), "-", "")
	t.Logf("Base key for test: %s", key)
	b, _ := json.Marshal(ValueType{Message: "test"})
	value := b

	// Init
	t.Run("init", func(t *testing.T) {
		start := time.Now()
		err := statestore.Init(state.Metadata{
			Properties: props,
		})
		elapsed := time.Since(start)
		assert.Nil(t, err)
		assert.Lessf(t, elapsed.Microseconds(), config.maxInitDuration.Microseconds(),
			"test took %dμs but must complete in less than %dμs", elapsed.Microseconds(), config.maxDeleteDuration.Microseconds())
	})

	if config.HasOperation("set") {
		// Set
		t.Run("set", func(t *testing.T) {
			setReq := &state.SetRequest{
				Key:   key,
				Value: value,
			}
			start := time.Now()
			err := statestore.Set(setReq)
			elapsed := time.Since(start)
			assert.Nil(t, err)
			assert.Lessf(t, elapsed.Microseconds(), config.maxSetDuration.Microseconds(),
				"test took %dμs but must complete in less than %dμs", elapsed.Microseconds(), config.maxSetDuration.Microseconds())
		})
	}

	if config.HasOperation("get") {
		// Get
		t.Run("get", func(t *testing.T) {
			getReq := &state.GetRequest{
				Key: key,
			}
			start := time.Now()
			getRes, err := statestore.Get(getReq)
			elapsed := time.Since(start)
			assert.Nil(t, err)
			assert.Equal(t, value, getRes.Data)
			assert.Lessf(t, elapsed.Microseconds(), config.maxGetDuration.Microseconds(),
				"test took %v but must complete in less than %v", elapsed.Microseconds(), config.maxGetDuration.Microseconds())
		})
	}

	if config.HasOperation("delete") {
		// Delete
		t.Run("delete", func(t *testing.T) {
			delReq := &state.DeleteRequest{
				Key: key,
			}
			start := time.Now()
			err := statestore.Delete(delReq)
			elapsed := time.Since(start)
			assert.Nil(t, err)
			assert.Lessf(t, elapsed.Microseconds(), config.maxDeleteDuration.Microseconds(),
				"test took %dμs but must complete in less than %dμs", elapsed.Microseconds(), config.maxDeleteDuration.Microseconds())
		})
	}

	if config.HasOperation("bulkset") || config.HasOperation("bulkdelete") {
		// Bulk test vars
		var bulkSetReqs []state.SetRequest
		var bulkDeleteReqs []state.DeleteRequest
		for k := 0; k < config.numBulkRequests; k++ {
			bkey := fmt.Sprintf("%s-%d", key, k)
			bulkSetReqs = append(bulkSetReqs, state.SetRequest{
				Key:   bkey,
				Value: value,
			})
			bulkDeleteReqs = append(bulkDeleteReqs, state.DeleteRequest{
				Key: bkey,
			})
		}

		if config.HasOperation("bulkset") {
			// BulkSet
			t.Run("bulkset", func(t *testing.T) {
				start := time.Now()
				err := statestore.BulkSet(bulkSetReqs)
				elapsed := time.Since(start)
				maxElapsed := config.maxSetDuration * time.Duration(config.numBulkRequests) // assumes at least linear scale
				assert.Nil(t, err)
				assert.Lessf(t, elapsed.Microseconds(), maxElapsed.Microseconds(),
					"test took %dμs but must complete in less than %dμs", elapsed.Microseconds(), maxElapsed.Microseconds())
				for k := 0; k < config.numBulkRequests; k++ {
					bkey := fmt.Sprintf("%s-%d", key, k)
					greq := &state.GetRequest{
						Key: bkey,
					}
					_, err = statestore.Get(greq)
					assert.Nil(t, err)
				}
			})
		}

		// BulkGet is not implemented yet in state/store.go

		if config.HasOperation("bulkdelete") {
			// BulkDelete
			t.Run("bulkdelete", func(t *testing.T) {
				start := time.Now()
				err := statestore.BulkDelete(bulkDeleteReqs)
				elapsed := time.Since(start)
				maxElapsed := config.maxDeleteDuration * time.Duration(config.numBulkRequests) // assumes at least linear scale
				assert.Nil(t, err)
				assert.Lessf(t, elapsed.Microseconds(), maxElapsed.Microseconds(),
					"test took %dμs but must complete in less than %dμs", elapsed.Microseconds(), maxElapsed.Microseconds())
			})
		}

		if config.HasOperation("transaction") {
			t.Run("transaction", func(t *testing.T) {
				// We do not test failure transactions because each database has their own failure conditions.
				saveReq := &state.TransactionalStateRequest{
					Operations: []state.TransactionalStateOperation{
						{
							Operation: state.Upsert,
							Request: state.SetRequest{
								Key:   fmt.Sprintf("%s-transactionstate", key),
								Value: "hello world",
							},
						},
						{
							Operation: state.Upsert,
							Request: state.SetRequest{
								Key:   fmt.Sprintf("%s-to-be-deleted", key),
								Value: "hello world",
							},
						},
					},
					// For CosmosDB
					Metadata: map[string]string{
						"partitionKey": "myPartition",
					},
				}

				deleteReq := &state.TransactionalStateRequest{
					Operations: []state.TransactionalStateOperation{
						{
							Operation: state.Delete,
							Request: state.DeleteRequest{
								Key: fmt.Sprintf("%s-to-be-deleted", key),
							},
						},
					},
					// For CosmosDB
					Metadata: map[string]string{
						"partitionKey": "myPartition",
					},
				}

				reqCount := 0
				transactionStore := statestore.(state.TransactionalStore)
				start := time.Now()

				// Save
				err := transactionStore.Multi(saveReq)
				reqCount = reqCount + 1
				assert.Nil(t, err)

				// Delete
				err = transactionStore.Multi(deleteReq)
				reqCount = reqCount + 1
				assert.Nil(t, err)

				elapsed := time.Since(start)
				maxElapsed := config.maxSetDuration * time.Duration(reqCount) // assumes at least linear scale
				assert.Lessf(t, elapsed.Microseconds(), maxElapsed.Microseconds(),
					"test took %dμs but must complete in less than %dμs", elapsed.Microseconds(), maxElapsed.Microseconds())

				res, err := statestore.Get(&state.GetRequest{
					Key: fmt.Sprintf("%s-transactionstate", key),
					// For CosmosDB
					Metadata: map[string]string{
						"partitionKey": "myPartition",
					},
				})
				assert.Nil(t, err)
				assert.NotNil(t, res.Data)

				res, err = statestore.Get(&state.GetRequest{
					Key: fmt.Sprintf("%s-to-be-deleted", key),
					// For CosmosDB
					Metadata: map[string]string{
						"partitionKey": "myPartition",
					},
				})
				assert.Nil(t, err)
				assert.Nil(t, res.Data)
			})
		}
	}
}
