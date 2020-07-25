// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package statechange

import (
	"os"
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

const (
	testTableName = "daprstate"
)

func getTestMetadata() map[string]string {
	return map[string]string{
		"address":  "127.0.0.1:28015",
		"database": "dapr",
		"table":    testTableName,
	}
}

func getNewRethinkActorBinding() *Binding {
	l := logger.NewLogger("cron")
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

	// this will be blocking hence the stop channel above
	err = b.Read(func(res *bindings.ReadResponse) error {
		assert.NotNil(t, res)
		t.Logf("state change event:\n%s", string(res.Data))
		return nil
	})
	assert.NoErrorf(t, err, "error on read")
}
