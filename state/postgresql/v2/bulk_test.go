/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package postgresql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

// These tests exercise only the argument-validation and short-circuit
// branches of the bulk/DeleteWithPrefix paths. The full database-backed
// suite runs under the conformance harness (tests/conformance/state_test.go)
// with docker-compose Postgres; see tests/config/state/tests.yml. In
// particular the ETag/first-write fallback behaviour of BulkSet is
// exercised end-to-end there — reaching into the uninitialised *PostgreSQL
// to trigger that fallback from a unit test would panic in a goroutine
// (see state.DoBulkSetDelete) and is deliberately not attempted here.

func TestBulkSetEmptyShortCircuits(t *testing.T) {
	p := NewPostgreSQLStateStore(logger.NewLogger("test")).(*PostgreSQL)
	// nil and empty slices must not touch the (unset) db.
	require.NoError(t, p.BulkSet(t.Context(), nil, state.BulkStoreOpts{}))
	require.NoError(t, p.BulkSet(t.Context(), []state.SetRequest{}, state.BulkStoreOpts{}))
}

func TestBulkDeleteEmptyShortCircuits(t *testing.T) {
	p := NewPostgreSQLStateStore(logger.NewLogger("test")).(*PostgreSQL)
	require.NoError(t, p.BulkDelete(t.Context(), nil, state.BulkStoreOpts{}))
	require.NoError(t, p.BulkDelete(t.Context(), []state.DeleteRequest{}, state.BulkStoreOpts{}))
}

func TestBulkSetRejectsEmptyKey(t *testing.T) {
	p := NewPostgreSQLStateStore(logger.NewLogger("test")).(*PostgreSQL)
	err := p.BulkSet(t.Context(), []state.SetRequest{{Key: "", Value: []byte("v")}}, state.BulkStoreOpts{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing key in set operation")
}

func TestBulkDeleteRejectsEmptyKey(t *testing.T) {
	p := NewPostgreSQLStateStore(logger.NewLogger("test")).(*PostgreSQL)
	err := p.BulkDelete(t.Context(), []state.DeleteRequest{{Key: ""}}, state.BulkStoreOpts{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing key in delete operation")
}

func TestFeaturesAdvertiseDeleteWithPrefix(t *testing.T) {
	p := NewPostgreSQLStateStore(logger.NewLogger("test")).(*PostgreSQL)
	assert.True(t, state.FeatureDeleteWithPrefix.IsPresent(p.Features()),
		"postgres v2 must advertise FeatureDeleteWithPrefix for workflow purge fast path")
}
