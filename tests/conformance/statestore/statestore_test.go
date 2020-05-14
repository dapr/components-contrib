package statestore_conformance

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dapr/components-contrib/state"
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
	maxSetDurationInMs    = 2
	maxGetDurationInMs    = 2
	maxDeleteDurationInMs = 2
	numBulkRequests       = 10
)

func TestStatestore(t *testing.T) {
	testCases := []struct {
		factoryMethod func() state.Store
		componentPath string
	}{
		{
			factoryMethod: func() state.Store {
				return redis.NewRedisStateStore(nil)
			},
			componentPath: "../../config/statestore/redis",
		},
	}

	for i, tt := range testCases {
		cfg := config.StandaloneConfig{
			ComponentsPath: tt.componentPath,
		}
		standaloneComps := components.NewStandaloneComponents(cfg)
		comps, err := standaloneComps.LoadComponents()
		if err != nil {
			panic(err)
		}
		for j, c := range comps {
			testName := fmt.Sprintf("test%d-%d", i, j)
			t.Run(testName, func(t *testing.T) {
				statestore := tt.factoryMethod()
				md := convertMetadataToProperties(c.Spec.Metadata)

				// Init
				func() {
					start := time.Now()
					err := statestore.Init(state.Metadata{
						Properties: md,
					})
					duration := time.Since(start)
					maxDuration := time.Millisecond * time.Duration(maxInitDurationInMs)
					assert.Nil(t, err)
					assert.Less(t, duration.Microseconds(), maxDuration.Microseconds())
				}()

				etag := "1"
				key := testName
				b, err := json.Marshal(ValueType{Message: "test"})
				assert.Nil(t, err)
				value := b

				// Set
				func() {
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
				}()

				// Get
				func() {
					start := time.Now()
					getReq := &state.GetRequest{
						Key: key,
					}
					getRes, err := statestore.Get(getReq)
					duration := time.Since(start)
					maxDuration := time.Millisecond * time.Duration(maxGetDurationInMs)
					assert.Nil(t, err)
					assert.Equal(t, etag, getRes.ETag)
					assert.Equal(t, value, getRes.Data)
					assert.Less(t, duration.Microseconds(), maxDuration.Microseconds())
				}()

				// Delete
				func() {
					start := time.Now()
					delReq := &state.DeleteRequest{
						Key:  key,
						ETag: etag,
					}
					err = statestore.Delete(delReq)
					duration := time.Since(start)
					maxDuration := time.Millisecond * time.Duration(maxDeleteDurationInMs)
					assert.Nil(t, err)
					assert.Less(t, duration.Microseconds(), maxDuration.Microseconds())
				}()

				var bulkSetReqs []state.SetRequest
				var bulkDeleteReqs []state.DeleteRequest
				for k := 0; k < numBulkRequests; k++ {
					bkey := fmt.Sprintf("%s-%d", testName, k)
					bulkSetReqs = append(bulkSetReqs, state.SetRequest{
						Key:   bkey,
						Value: value,
					})
					bulkDeleteReqs = append(bulkDeleteReqs, state.DeleteRequest{
						Key: bkey,
					})
				}

				// BulkSet
				func() {
					start := time.Now()
					err = statestore.BulkSet(bulkSetReqs)
					duration := time.Since(start)
					maxDuration := time.Millisecond * time.Duration(maxDeleteDurationInMs*numBulkRequests)
					assert.Nil(t, err)
					assert.Less(t, duration.Microseconds(), maxDuration.Microseconds())
					for k := 0; k < numBulkRequests; k++ {
						bkey := fmt.Sprintf("%s-%d", testName, k)
						greq := &state.GetRequest{
							Key: bkey,
						}
						_, err = statestore.Get(greq)
						assert.Nil(t, err)
					}
				}()

				// BulkDelete
				func() {
					start := time.Now()
					err = statestore.BulkDelete(bulkDeleteReqs)
					duration := time.Since(start)
					maxDuration := time.Millisecond * time.Duration(maxDeleteDurationInMs*numBulkRequests)
					assert.Nil(t, err)
					assert.Less(t, duration.Microseconds(), maxDuration.Microseconds())
					for k := 0; k < numBulkRequests; k++ {
						bkey := fmt.Sprintf("%s-%d", testName, k)
						greq := &state.GetRequest{
							Key: bkey,
						}
						res, err := statestore.Get(greq)
						assert.Nil(t, err)
						assert.Equal(t, []byte(nil), res.Data)
					}
				}()
			})
		}
	}
}

func convertMetadataToProperties(items []v1alpha1.MetadataItem) map[string]string {
	properties := map[string]string{}
	for _, c := range items {
		properties[c.Name] = c.Value
	}
	return properties
}
