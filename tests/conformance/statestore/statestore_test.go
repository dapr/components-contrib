package statestore

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"math/rand"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/aerospike"
	"github.com/dapr/components-contrib/state/memcached"
	"github.com/dapr/components-contrib/state/mongodb"
	"github.com/dapr/components-contrib/state/redis"
	"github.com/dapr/components-contrib/tests/conformance"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/dapr/pkg/apis/components/v1alpha1"
	"github.com/dapr/dapr/pkg/components"
	config "github.com/dapr/dapr/pkg/config/modes"
)

type ValueType struct {
	Message string `json:"message"`
}

const (
	maxInitDurationInMs   = 30
	maxSetDurationInMs    = 15
	maxGetDurationInMs    = 10
	maxDeleteDurationInMs = 15
	numBulkRequests       = 15

	componentType = "pubsub"

	initName       = "init"
	getName        = "get"
	setName        = "set"
	deleteName     = "delete"
	bulkSetName    = "bulkset"
	bulkDeleteName = "bulkdelete"
)

var (
	renderDisabled  = false
	renderReportDir = ""
	renderPretty    = false
	renderFormat    = ""
)

func newKey(length int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyz")
	b := make([]rune, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestMain(m *testing.M) {
	_, renderDisabled = os.LookupEnv("RENDER_DISABLE")
	renderReportDir = os.Getenv("RENDER_REPORT_DIR")
	renderFormat = os.Getenv("RENDER_FORMAT")
	_, renderPretty = os.LookupEnv("RENDER_PRETTY")

	os.Exit(m.Run())
}

func TestRedis(t *testing.T) {
	runTestWithStateStore(t, "redis", func() state.Store {
		return redis.NewRedisStateStore(nil)
	})
}

func TestMongoDB(t *testing.T) {
	runTestWithStateStore(t, "mongodb", func() state.Store {
		return mongodb.NewMongoDB(nil)
	})
}

func TestMemcached(t *testing.T) {
	runTestWithStateStore(t, "memcached", func() state.Store {
		return memcached.NewMemCacheStateStore(nil)
	})
}

func TestAerospike(t *testing.T) {
	runTestWithStateStore(t, "aerospike", func() state.Store {
		return aerospike.NewAerospikeStateStore(nil)
	})
}

func runTestWithStateStore(t *testing.T, name string, componentFactory func() state.Store) {
	report := conformance.NewComponentReport(name, componentType)

	store := componentFactory()
	comps, err := loadComponents(fmt.Sprintf("../../config/statestore/%s", name))
	assert.Nil(t, err)
	assert.Equal(t, len(comps), 1) // We only expect a single component per state store

	c := comps[0]
	props := convertMetadataToProperties(c.Spec.Metadata)
	checkAPIConformance(t, props, store, report)

	if !renderDisabled {
		b, renderedFormat, err := report.Render(renderFormat, renderPretty)
		if err != nil {
			panic(fmt.Sprintf("error rendering conformance report: %+v", err))
		}
		if renderReportDir == "" {
			t.Logf("%+v", string(b))
		} else {
			reportOut := fmt.Sprintf("%s.%s", filepath.Join(renderReportDir, name), renderedFormat)
			if err = ioutil.WriteFile(reportOut, b, 0666); err != nil {
				panic(fmt.Sprintf("error writing conformance report %s: %+v", reportOut, err))
			}
		}
	}
}

func checkAPIConformance(t *testing.T, props map[string]string, statestore state.Store, report *conformance.ComponentReport) {
	rand.Seed(time.Now().Unix())
	key := newKey(8)
	b, err := json.Marshal(ValueType{Message: "test"})
	assert.Nil(t, err)
	value := b

	// Init
	t.Run(initName, func(t *testing.T) {
		start := time.Now()
		err := statestore.Init(state.Metadata{
			Properties: props,
		})
		duration := time.Since(start)
		maxDuration := time.Millisecond * time.Duration(maxInitDurationInMs)
		assert.Nil(t, err)
		assert.Less(t, duration.Microseconds(), maxDuration.Microseconds())
		report.AddFunctionReport(conformance.NewFunctionReport(initName, duration.Microseconds(), true))
	})

	// Set
	t.Run(setName, func(t *testing.T) {
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
		report.AddFunctionReport(conformance.NewFunctionReport(setName, duration.Microseconds(), counter == 2))
	})

	// Get
	t.Run(getName, func(t *testing.T) {
		start := time.Now()
		getReq := &state.GetRequest{
			Key: key,
		}
		getRes, err := statestore.Get(getReq)
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
		report.AddFunctionReport(conformance.NewFunctionReport(getName, duration.Microseconds(), counter == 3))
	})

	// Delete
	t.Run(deleteName, func(t *testing.T) {
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
		report.AddFunctionReport(conformance.NewFunctionReport(deleteName, duration.Microseconds(), counter == 2))
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
	t.Run(bulkSetName, func(t *testing.T) {
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
			report.AddFunctionReport(conformance.NewFunctionReport(bulkSetName, duration.Microseconds(), counter == numBulkRequests+2))
		}
	})

	// BulkDelete
	t.Run(bulkDeleteName, func(t *testing.T) {
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
		report.AddFunctionReport(conformance.NewFunctionReport(bulkDeleteName, duration.Microseconds(), counter == 2))
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
