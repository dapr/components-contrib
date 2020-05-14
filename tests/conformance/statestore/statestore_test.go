package statestore_conformance

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"math/rand"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/mongodb"
	"github.com/dapr/components-contrib/state/redis"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/dapr/pkg/apis/components/v1alpha1"
	"github.com/dapr/dapr/pkg/components"
	config "github.com/dapr/dapr/pkg/config/modes"
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

func newKey(length int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyz")
	b := make([]rune, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestRedis(t *testing.T) {
	runTestWithStateStore(t, "../../config/statestore/redis", func() state.Store {
		return redis.NewRedisStateStore(nil)
	})
}

func TestMongoDB(t *testing.T) {
	runTestWithStateStore(t, "../../config/statestore/mongodb", func() state.Store {
		return mongodb.NewMongoDB(nil)
	})
}

func runTestWithStateStore(t *testing.T, componentPath string, componentFactory func() state.Store) {
	store := componentFactory()
	comps, err := loadComponents(componentPath)
	assert.Nil(t, err)
	assert.Equal(t, len(comps), 1) // We only expect a single component per state store

	c := comps[0]
	props := convertMetadataToProperties(c.Spec.Metadata)
	checkAPIConformance(t, props, store)
}

func checkAPIConformance(t *testing.T, props map[string]string, statestore state.Store) {
	rand.Seed(time.Now().Unix())
	key := newKey(8)
	b, err := json.Marshal(ValueType{Message: "test"})
	assert.Nil(t, err)
	value := b

	// Init
	t.Run("init", func(t *testing.T) {
		start := time.Now()
		err := statestore.Init(state.Metadata{
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
		assert.Nil(t, err)
		assert.Less(t, duration.Microseconds(), maxDuration.Microseconds())
	})

	// Get
	t.Run("get", func(t *testing.T) {
		start := time.Now()
		getReq := &state.GetRequest{
			Key: key,
		}
		getRes, err := statestore.Get(getReq)
		duration := time.Since(start)
		maxDuration := time.Millisecond * time.Duration(maxGetDurationInMs)
		assert.Nil(t, err)
		assert.Equal(t, value, getRes.Data)
		assert.Less(t, duration.Microseconds(), maxDuration.Microseconds())
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
		assert.Nil(t, err)
		assert.Less(t, duration.Microseconds(), maxDuration.Microseconds())
	})

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
		assert.Nil(t, err)
		assert.Less(t, duration.Microseconds(), maxDuration.Microseconds())
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
		err = statestore.BulkDelete(bulkDeleteReqs)
		duration := time.Since(start)
		maxDuration := time.Millisecond * time.Duration(maxDeleteDurationInMs*numBulkRequests)
		assert.Nil(t, err)
		assert.Less(t, duration.Microseconds(), maxDuration.Microseconds())
	})
}

func loadComponents(componentPath string) ([]v1alpha1.Component, error) {
	cfg := config.StandaloneConfig{
		ComponentsPath: componentPath,
	}
	standaloneComps := components.NewStandaloneComponents(cfg)
	components, err := standaloneComps.LoadComponents()
	if err != nil {
		return nil, err
	}
	return components, nil
}

func convertMetadataToProperties(items []v1alpha1.MetadataItem) map[string]string {
	properties := map[string]string{}
	for _, c := range items {
		properties[c.Name] = c.Value
	}
	return properties
}
