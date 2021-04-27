// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package statechange

import (
	"os"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

func getTestMetadata() map[string]string {
	return map[string]string{
		"address":  "127.0.0.1:28015",
		"database": "dapr",
		"username": "admin",
		"password": "rethinkdb",
		"table":    "daprstate",
	}
}

func getNewRethinkActorBinding() *Binding {
	l := logger.NewLogger("test")
	if os.Getenv("DEBUG") != "" {
		l.SetOutputLevel(logger.DebugLevel)
	}

	return NewRethinkDBStateChangeBinding(l)
}

/*
go test github.com/dapr/components-contrib/bindings/rethinkdb/statechange \
	-run ^TestBinding$ -count 1
*/

func TestBinding(t *testing.T) {
	if os.Getenv("RUN_LIVE_RETHINKDB_TEST") != "true" {
		t.SkipNow()
	}
	testDuration := 10 * time.Second
	testDurationStr := os.Getenv("RETHINKDB_TEST_DURATION")
	if testDurationStr != "" {
		d, err := time.ParseDuration(testDurationStr)
		if err != nil {
			t.Fatalf("invalid test duration: %s, expected time.Duration", testDurationStr)
		}
		testDuration = d
	}

	m := bindings.Metadata{
		Name:       "test",
		Properties: getTestMetadata(),
	}
	assert.NotNil(t, m.Properties)

	b := getNewRethinkActorBinding()
	err := b.Init(m)
	assert.NoErrorf(t, err, "error initializing")

	go func() {
		err = b.Read(func(res *bindings.ReadResponse) ([]byte, error) {
			assert.NotNil(t, res)
			t.Logf("state change event:\n%s", string(res.Data))

			return nil, nil
		})
		assert.NoErrorf(t, err, "error on read")
	}()

	testTimer := time.AfterFunc(testDuration, func() {
		t.Log("done")
		b.stopCh <- true
	})
	defer testTimer.Stop()
	<-b.stopCh
}
