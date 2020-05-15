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
	maxInitDurationInMs   = 1000
	maxSetDurationInMs    = 100
	maxGetDurationInMs    = 100
	maxDeleteDurationInMs = 100
	numBulkRequests       = 10

	componentType = "state"
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
	b, err := json.Marshal(ValueType{Message: "test"})
	assert.Nil(t, err)
	value := b

	// Init
	t.Run("init", func(t *testing.T) {
		start := time.Now()
		err = statestore.Init(state.Metadata{
			Properties: props,
		})
		duration := time.Since(start)
		maxDuration := time.Millisecond * time.Duration(maxInitDurationInMs)
		assert.Nil(t, err)
		assert.Less(t, duration.Microseconds(), maxDuration.Microseconds())
	})

	// Set
	t.Run("set", func(t *testing.T) {
		start := time.Now()
		setReq := &state.SetRequest{
			Key:   key,
			Value: value,
		}
		err = statestore.Set(setReq)
		duration := time.Since(start)
		maxDuration := time.Millisecond * time.Duration(maxSetDurationInMs)
		counter := 0
		if assert.Nil(t, err) {
			counter++
		}
		if assert.Less(t, duration.Microseconds(), maxDuration.Microseconds()) {
			counter++
		}
	})

	// Get
	t.Run("get", func(t *testing.T) {
		start := time.Now()
		getReq := &state.GetRequest{
			Key: key,
		}
		getRes, err := statestore.Get(getReq) // nolint:govet
		duration := time.Since(start)
		maxDuration := time.Millisecond * time.Duration(maxGetDurationInMs)
		counter := 0
		if assert.Nil(t, err) {
			counter++
		}
		if assert.Equal(t, value, getRes.Data) {
			counter++
		}
		if assert.Less(t, duration.Microseconds(), maxDuration.Microseconds()) {
			counter++
		}
	})

	// Delete
	t.Run("delete", func(t *testing.T) {
		start := time.Now()
		delReq := &state.DeleteRequest{
			Key: key,
		}
		err = statestore.Delete(delReq)
		duration := time.Since(start)
		maxDuration := time.Millisecond * time.Duration(maxDeleteDurationInMs)
		counter := 0
		if assert.Nil(t, err) {
			counter++
		}
		if assert.Less(t, duration.Microseconds(), maxDuration.Microseconds()) {
			counter++
		}
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
		err = statestore.BulkSet(bulkSetReqs)
		duration := time.Since(start)
		maxDuration := time.Millisecond * time.Duration(maxDeleteDurationInMs*numBulkRequests)
		counter := 0
		if assert.Nil(t, err) {
			counter++
		}
		if assert.Less(t, duration.Microseconds(), maxDuration.Microseconds()) {
			counter++
		}
		for k := 0; k < numBulkRequests; k++ {
			bkey := fmt.Sprintf("%s-%d", key, k)
			greq := &state.GetRequest{
				Key: bkey,
			}
			_, err = statestore.Get(greq)
			if assert.Nil(t, err) {
				counter++
			}
		}
	})

	// BulkDelete
	t.Run("bulkdelete", func(t *testing.T) {
		start := time.Now()
		err = statestore.BulkDelete(bulkDeleteReqs)
		duration := time.Since(start)
		maxDuration := time.Millisecond * time.Duration(maxDeleteDurationInMs*numBulkRequests)
		counter := 0
		if assert.Nil(t, err) {
			counter++
		}
		if assert.Less(t, duration.Microseconds(), maxDuration.Microseconds()) {
			counter++
		}
	})
}
