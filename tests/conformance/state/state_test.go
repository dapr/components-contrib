package state

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/redis"
	"github.com/dapr/components-contrib/tests/conformance"
	"github.com/stretchr/testify/assert"
)

type ValueType struct {
	Message string `json:"message"`
}

const (
	maxInitDurationInMs   = 10
	maxSetDurationInMs    = 10
	maxGetDurationInMs    = 10
	maxDeleteDurationInMs = 10
	numBulkRequests       = 10
)

/*
	State store component tests
*/
func TestRedis(t *testing.T) {
	runWithStateStore(t, "redis", func() state.Store {
		return redis.NewRedisStateStore(nil)
	})
}

func runWithStateStore(t *testing.T, name string, componentFactory func() state.Store) {
	store := componentFactory()
	comps, err := conformance.LoadComponents(fmt.Sprintf("../../config/state/%s", name))
	assert.Nil(t, err)
	assert.Equal(t, len(comps), 1) // We only expect a single component per state store

	c := comps[0]
	props := conformance.ConvertMetadataToProperties(c.Spec.Metadata)
	// Run the state store conformance tests
	stateStoreConformanceTests(t, props, store)
}

func stateStoreConformanceTests(t *testing.T, props map[string]string, statestore state.Store) {
	// Test vars
	rand.Seed(time.Now().Unix())
	key := conformance.NewRandString(8)
	b, _ := json.Marshal(ValueType{Message: "test"})
	value := b

	// Init
	t.Run("init", func(t *testing.T) {
		start := time.Now()
		err := statestore.Init(state.Metadata{
			Properties: props,
		})
		elapsed := time.Since(start)
		maxElapsed := time.Millisecond * time.Duration(maxInitDurationInMs)
		assert.Nil(t, err)
		assert.Less(t, elapsed.Microseconds(), maxElapsed.Microseconds(),
			"test took %dμs but must complete in less than %dμs", elapsed.Microseconds(), maxElapsed.Microseconds())
	})

	// Set
	t.Run("set", func(t *testing.T) {
		setReq := &state.SetRequest{
			Key:   key,
			Value: value,
		}
		start := time.Now()
		err := statestore.Set(setReq)
		elapsed := time.Since(start)
		maxElapsed := time.Millisecond * time.Duration(maxSetDurationInMs)
		assert.Nil(t, err)
		assert.Less(t, elapsed.Microseconds(), maxElapsed.Microseconds(),
			"test took %dμs but must complete in less than %dμs", elapsed.Microseconds(), maxElapsed.Microseconds())
	})

	// Get
	t.Run("get", func(t *testing.T) {
		getReq := &state.GetRequest{
			Key: key,
		}
		start := time.Now()
		getRes, err := statestore.Get(getReq) // nolint:govet
		elapsed := time.Since(start)
		maxElapsed := time.Millisecond * time.Duration(maxGetDurationInMs)
		assert.Nil(t, err)
		assert.Equal(t, value, getRes.Data)
		assert.Less(t, elapsed.Microseconds(), maxElapsed.Microseconds(),
			"test took %dμs but must complete in less than %dμs", elapsed.Microseconds(), maxElapsed.Microseconds())
	})

	// Delete
	t.Run("delete", func(t *testing.T) {
		delReq := &state.DeleteRequest{
			Key: key,
		}
		start := time.Now()
		err := statestore.Delete(delReq)
		elapsed := time.Since(start)
		maxElapsed := time.Millisecond * time.Duration(maxDeleteDurationInMs)
		assert.Nil(t, err)
		assert.Less(t, elapsed.Microseconds(), maxElapsed.Microseconds(),
			"test took %dμs but must complete in less than %dμs", elapsed.Microseconds(), maxElapsed.Microseconds())
	})

	// Bulk test vars
	var bulkSetReqs []state.SetRequest
	var bulkDeleteReqs []state.DeleteRequest
	for k := 0; k < numBulkRequests; k++ {
		bkey := fmt.Sprintf("%s-%d", key, k)
		bulkSetReqs = append(bulkSetReqs, state.SetRequest{
			Key:   bkey,
			Value: value,
		})
		bulkDeleteReqs = append(bulkDeleteReqs, state.DeleteRequest{
			Key: bkey,
		})
	}

	// BulkSet
	t.Run("bulkset", func(t *testing.T) {
		start := time.Now()
		err := statestore.BulkSet(bulkSetReqs)
		elapsed := time.Since(start)
		maxElapsed := time.Millisecond * time.Duration(maxDeleteDurationInMs*numBulkRequests) // assumes at least linear scale
		assert.Nil(t, err)
		assert.Less(t, elapsed.Microseconds(), maxElapsed.Microseconds(),
			"test took %dμs but must complete in less than %dμs", elapsed.Microseconds(), maxElapsed.Microseconds())
		for k := 0; k < numBulkRequests; k++ {
			bkey := fmt.Sprintf("%s-%d", key, k)
			greq := &state.GetRequest{
				Key: bkey,
			}
			_, err = statestore.Get(greq)
			assert.Nil(t, err)
		}
	})

	// BulkDelete
	t.Run("bulkdelete", func(t *testing.T) {
		start := time.Now()
		err := statestore.BulkDelete(bulkDeleteReqs)
		elapsed := time.Since(start)
		maxElapsed := time.Millisecond * time.Duration(maxDeleteDurationInMs*numBulkRequests) // assumes at least linear scale
		assert.Nil(t, err)
		assert.Less(t, elapsed.Microseconds(), maxElapsed.Microseconds(),
			"test took %dμs but must complete in less than %dμs", elapsed.Microseconds(), maxElapsed.Microseconds())
	})
}
