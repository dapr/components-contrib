// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rethinkdb

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

// go test -timeout 30s github.com/dapr/components-contrib/state/rethinkdb -count 1 -run ^TestGetRethinkDBMetadata$
func TestGetRethinkDBMetadata(t *testing.T) {
	testLogger := logger.NewLogger("test")
	t.Run("With required connect configuration", func(t *testing.T) {
		p := getTestMetadata()
		m, err := metadataToConfig(p, testLogger)
		assert.Nil(t, err)
		assert.Equal(t, p["address"], m.Address)
		assert.Equal(t, p["database"], m.Database)
		assert.True(t, m.Archive)
	})

	t.Run("With optional table configuration", func(t *testing.T) {
		p := getTestMetadata()

		timeout := 15 * time.Second
		p["timeout"] = fmt.Sprintf("%v", timeout)

		maxOpen := 30
		p["max_open"] = fmt.Sprintf("%v", maxOpen)

		discoverHosts := true
		p["discover_hosts"] = fmt.Sprintf("%v", discoverHosts)

		m, err := metadataToConfig(p, testLogger)
		assert.Nil(t, err)
		assert.Equal(t, maxOpen, m.MaxOpen)
		assert.Equal(t, discoverHosts, m.DiscoverHosts)
	})
}

// go test -timeout 30s ./state/rethinkdb -run ^TestRethinkDBStateStore

func TestRethinkDBStateStore(t *testing.T) {
	if !isLiveTest() {
		t.SkipNow()
	}

	m := state.Metadata{Properties: getTestMetadata()}
	db := NewRethinkDBStateStore(logger.NewLogger("test"))

	t.Run("With init", func(t *testing.T) {
		if err := db.Init(m); err != nil {
			t.Fatalf("error initializing db: %v", err)
		}
		assert.Equal(t, stateTableNameDefault, db.config.Table)

		m.Properties["table"] = "test"
		if err := db.Init(m); err != nil {
			t.Fatalf("error initializing db: %v", err)
		}
		assert.Equal(t, "test", db.config.Table)
	})

	t.Run("With struct data", func(t *testing.T) {
		// create and set data
		d := &testObj{F1: "test", F2: 1, F3: time.Now().UTC()}
		k := fmt.Sprintf("ids-%d", time.Now().UnixNano())

		if err := db.Set(&state.SetRequest{Key: k, Value: d}); err != nil {
			t.Fatalf("error setting data to db: %v", err)
		}

		// get set data and compare
		resp, err := db.Get(&state.GetRequest{Key: k})
		assert.Nil(t, err)
		d2 := testGetTestObj(t, resp)
		assert.NotNil(t, d2)
		assert.Equal(t, d.F1, d2.F1)
		assert.Equal(t, d.F2, d2.F2)
		assert.Equal(t, d.F3.Format(time.RFC3339), d2.F3.Format(time.RFC3339))

		// update data and set it again
		d2.F2 = 2
		d2.F3 = time.Now().UTC()
		tag := fmt.Sprintf("hash-%d", time.Now().UnixNano())
		if err = db.Set(&state.SetRequest{Key: k, Value: d2, ETag: &tag}); err != nil {
			t.Fatalf("error setting data to db: %v", err)
		}

		// get updated data and compare
		resp2, err := db.Get(&state.GetRequest{Key: k})
		assert.Nil(t, err)
		d3 := testGetTestObj(t, resp2)
		assert.NotNil(t, d3)
		assert.Equal(t, d2.F1, d3.F1)
		assert.Equal(t, d2.F2, d3.F2)
		assert.Equal(t, d2.F3.Format(time.RFC3339), d3.F3.Format(time.RFC3339))

		// delete data
		if err := db.Delete(&state.DeleteRequest{Key: k}); err != nil {
			t.Fatalf("error on data deletion: %v", err)
		}
	})

	t.Run("With byte data", func(t *testing.T) {
		// create and set data
		d := []byte("test")
		k := fmt.Sprintf("idb-%d", time.Now().UnixNano())

		if err := db.Set(&state.SetRequest{Key: k, Value: d}); err != nil {
			t.Fatalf("error setting data to db: %v", err)
		}

		// get set data and compare
		resp, err := db.Get(&state.GetRequest{Key: k})
		assert.Nil(t, err)
		assert.NotNil(t, resp)
		assert.NotNil(t, resp.Data)
		assert.Equal(t, string(d), string(resp.Data))

		// delete data
		if err := db.Delete(&state.DeleteRequest{Key: k}); err != nil {
			t.Fatalf("error on data deletion: %v", err)
		}
	})

	t.Run("With bulk", func(t *testing.T) {
		testBulk(t, db, 0)
	})
}

func TestRethinkDBStateStoreRongRun(t *testing.T) {
	if !isLiveTest() {
		t.SkipNow()
	}

	m := state.Metadata{Properties: getTestMetadata()}
	db := NewRethinkDBStateStore(logger.NewLogger("test"))
	if err := db.Init(m); err != nil {
		t.Fatalf("error initializing db: %v", err)
	}

	for i := 0; i < 1000; i++ {
		testBulk(t, db, i)
	}
}

func testBulk(t *testing.T, db *RethinkDB, i int) {
	// create data list
	deleteList := make([]state.DeleteRequest, 0)
	setList := make([]state.SetRequest, 3)
	for i := range setList {
		d := []byte("test")
		k := fmt.Sprintf("test-id-%d", i)
		deleteList = append(deleteList, state.DeleteRequest{Key: k})
		setList[i] = state.SetRequest{Key: k, Value: d}
	}

	// bulk set it
	if err := db.BulkSet(setList); err != nil {
		t.Fatalf("error setting data to db: %v -- run %d", err, i)
	}

	// check for the data
	for _, v := range deleteList {
		resp, err := db.Get(&state.GetRequest{Key: v.Key})
		assert.Nilf(t, err, " -- run %d", i)
		assert.NotNil(t, resp)
		assert.NotNil(t, resp.Data)
	}

	// delete data
	if err := db.BulkDelete(deleteList); err != nil {
		t.Fatalf("error on data deletion: %v -- run %d", err, i)
	}

	// check for the data NOT being there
	for _, v := range deleteList {
		resp, err := db.Get(&state.GetRequest{Key: v.Key})
		assert.Nilf(t, err, " -- run %d", i)
		assert.NotNil(t, resp)
		assert.Nil(t, resp.Data)
	}
}

// go test -timeout 30s github.com/dapr/components-contrib/state/rethinkdb -run ^TestRethinkDBStateStoreMulti$ -count 1 -v
func TestRethinkDBStateStoreMulti(t *testing.T) {
	if !isLiveTest() {
		t.SkipNow()
	}

	m := state.Metadata{Properties: getTestMetadata()}
	db := NewRethinkDBStateStore(logger.NewLogger("test"))
	if err := db.Init(m); err != nil {
		t.Fatalf("error initializing db: %v", err)
	}

	numOfRecords := 4
	recordIDFormat := "multi-%d"
	t.Run("With multi", func(t *testing.T) {
		// create data list
		d := []byte("test")
		list := make([]state.SetRequest, numOfRecords)
		for i := 0; i < numOfRecords; i++ {
			list[i] = state.SetRequest{Key: fmt.Sprintf(recordIDFormat, i), Value: d}
		}
		if err := db.BulkSet(list); err != nil {
			t.Fatalf("error setting multi to db: %v", err)
		}

		// test multi
		d2 := []byte("test")
		req := state.TransactionalStateRequest{
			Operations: []state.TransactionalStateOperation{
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:   fmt.Sprintf(recordIDFormat, 0),
						Value: d2,
					},
				},
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:   fmt.Sprintf(recordIDFormat, 1),
						Value: d2,
					},
				},
				{
					Operation: state.Delete,
					Request:   state.DeleteRequest{Key: fmt.Sprintf(recordIDFormat, 2)},
				},
				{
					Operation: state.Delete,
					Request:   state.DeleteRequest{Key: fmt.Sprintf(recordIDFormat, 3)},
				},
			},
		}

		// execute multi
		if err := db.Multi(req); err != nil {
			t.Fatalf("error setting multi to db: %v", err)
		}

		// the one not deleted should be still there
		m1, err := db.Get(&state.GetRequest{Key: fmt.Sprintf(recordIDFormat, 1)})
		assert.Nil(t, err)
		assert.NotNil(t, m1)
		assert.NotNil(t, m1.Data)
		assert.Equal(t, string(d2), string(m1.Data))

		// the one deleted should not
		m2, err := db.Get(&state.GetRequest{Key: fmt.Sprintf(recordIDFormat, 3)})
		assert.Nil(t, err)
		assert.NotNil(t, m2)
		assert.Nil(t, m2.Data)
	})
}

type testObj struct {
	F1 string    `json:"f1"`
	F2 int       `json:"f2"`
	F3 time.Time `json:"f3"`
}

func testGetTestObj(t *testing.T, resp *state.GetResponse) *testObj {
	if resp == nil || resp.Data == nil {
		t.Fatal("invalid response")
	}

	var d testObj
	if err := json.Unmarshal(resp.Data, &d); err != nil {
		t.Fatalf("invalid data: %v", err)
	}

	return &d
}

func isLiveTest() bool {
	return os.Getenv("RUN_LIVE_RETHINKDB_TEST") == "true"
}

func getTestMetadata() map[string]string {
	return map[string]string{
		"address":  "127.0.0.1:28015", // default
		"database": "dapr",
		"username": "admin",     // default
		"password": "rethinkdb", // default
		"archive":  "true",
	}
}
