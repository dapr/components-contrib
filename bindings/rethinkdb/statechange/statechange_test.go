// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package statechange

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

const (
	testTableName                  = "daprstate"
	inputBindTestDurationInSeconds = 120
)

func getTestMetadata() map[string]string {
	return map[string]string{
		"address":  "127.0.0.1:28015",
		"database": "dapr",
		"table":    testTableName,
	}
}

/*
go test github.com/dapr/components-contrib/bindings/rethinkdb/statechange \
	-timeout 30s -run ^TestMetadata$ -count 1
*/

func TestMetadata(t *testing.T) {
	t.Run("With required connect configuration", func(t *testing.T) {
		p := getTestMetadata()
		m, err := metadataToConfig(p)
		assert.Nil(t, err)
		assert.Equal(t, p["address"], m.Address)
		assert.Equal(t, p["database"], m.Database)
		assert.Equal(t, p["table"], m.Table)
	})

	t.Run("With optional table configuration", func(t *testing.T) {
		p := getTestMetadata()

		timeout := time.Duration(15 * time.Second)
		p["timeout"] = fmt.Sprintf("%v", timeout)

		maxOpen := 30
		p["max_open"] = fmt.Sprintf("%v", maxOpen)

		discoverHosts := true
		p["discover_hosts"] = fmt.Sprintf("%v", discoverHosts)

		m, err := metadataToConfig(p)
		assert.Nil(t, err)
		assert.Equal(t, maxOpen, m.MaxOpen)
		assert.Equal(t, discoverHosts, m.DiscoverHosts)
	})
}

func getNewRethinkActorBinding() *ActorBinding {
	l := logger.NewLogger("cron")
	if os.Getenv("DEBUG") != "" {
		l.SetOutputLevel(logger.DebugLevel)
	}
	return NewRethinkDBActorBinding(l)
}

/*
go test github.com/dapr/components-contrib/bindings/rethinkdb/statechange \
	-run ^TestBinding$ -count 1
*/

func TestBinding(t *testing.T) {
	if os.Getenv("RUN_LIVE_RETHINKDB_TEST") != "true" {
		t.SkipNow() // skip this test until able to read credentials in test infra
	}

	m := bindings.Metadata{
		Name:       "test",
		Properties: getTestMetadata(),
	}
	assert.NotNil(t, m.Properties)

	b := getNewRethinkActorBinding()
	err := b.Init(m)
	assert.NoErrorf(t, err, "error initializing")

	endTestAt := time.Now().Add(time.Duration(inputBindTestDurationInSeconds * time.Second))
	t.Logf("reading until: %v", endTestAt)
	err = b.Read(func(res *bindings.ReadResponse) error {
		assert.NotNil(t, res)

		t.Logf("state change event:\n%s", string(res.Data))

		if time.Now().Before(endTestAt) {
			return nil
		}

		resp, err := b.Invoke(&bindings.InvokeRequest{
			Operation: bindings.DeleteOperation,
		})
		assert.NoError(t, err)
		tableVal, exists := resp.Metadata["table"]
		assert.Truef(t, exists, "Response metadata doesn't include the expected 'table' key")
		assert.Equal(t, testTableName, tableVal)

		return nil
	})
	assert.NoErrorf(t, err, "error on read")
}

/*
	go test github.com/dapr/components-contrib/bindings/rethinkdb/statechange \
		-timeout 30s -run ^TestInvokeInvalidOperation$ -count 1
*/

func TestInvokeInvalidOperation(t *testing.T) {
	if os.Getenv("RUN_LIVE_RETHINKDB_TEST") != "true" {
		t.SkipNow() // skip this test until able to read credentials in test infra
	}

	m := bindings.Metadata{
		Name:       "test",
		Properties: getTestMetadata(),
	}
	assert.NotNil(t, m.Properties)

	b := getNewRethinkActorBinding()
	err := b.Init(m)
	assert.NoErrorf(t, err, "error initializing")
	_, err = b.Invoke(&bindings.InvokeRequest{
		Operation: bindings.CreateOperation,
	})
	assert.Error(t, err)
}
