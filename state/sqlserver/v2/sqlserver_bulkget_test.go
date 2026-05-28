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

package sqlserver

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqlserverAuth "github.com/dapr/components-contrib/common/authentication/sqlserver"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

// newTestSQLServer wires a *SQLServer with a sqlmock-backed *sql.DB and the
// minimum metadata fields BulkGet needs.
func newTestSQLServer(t *testing.T, chunkSize int) (*SQLServer, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	s := &SQLServer{
		logger: logger.NewLogger("test"),
		db:     db,
	}
	s.metadata = sqlServerMetadata{
		SQLServerAuthMetadata: sqlserverAuth.SQLServerAuthMetadata{
			SchemaName: "dbo",
		},
		TableName:        "state",
		BulkGetChunkSize: chunkSize,
	}

	cleanup := func() { db.Close() }
	return s, mock, cleanup
}

var bulkCols = []string{"Key", "Data", "BinaryData", "isBinary", "RowVersion", "ExpireDate"}

func TestBulkGetEmpty(t *testing.T) {
	t.Parallel()

	s, mock, cleanup := newTestSQLServer(t, defaultBulkGetChunkSize)
	defer cleanup()

	res, err := s.BulkGet(t.Context(), nil, state.BulkGetOpts{})
	require.NoError(t, err)
	assert.Empty(t, res)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetEmptyKeyValidation(t *testing.T) {
	t.Parallel()

	s, mock, cleanup := newTestSQLServer(t, defaultBulkGetChunkSize)
	defer cleanup()

	req := []state.GetRequest{{Key: "key1"}, {Key: ""}, {Key: "key3"}}

	res, err := s.BulkGet(t.Context(), req, state.BulkGetOpts{})
	require.Error(t, err)
	assert.Nil(t, res)

	// No query should have been issued.
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetSingleQueryAllPresent(t *testing.T) {
	t.Parallel()

	s, mock, cleanup := newTestSQLServer(t, defaultBulkGetChunkSize)
	defer cleanup()

	rowVer1 := []byte{0x01, 0x02}
	rowVer2 := []byte{0x03, 0x04}
	rows := sqlmock.NewRows(bulkCols).
		AddRow("key1", `"value1"`, nil, false, rowVer1, nil).
		AddRow("key2", `"value2"`, nil, false, rowVer2, nil)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{{Key: "key1"}, {Key: "key2"}}

	res, err := s.BulkGet(t.Context(), req, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, res, 2)

	byKey := indexByKey(res)
	assert.Equal(t, []byte(`"value1"`), byKey["key1"].Data)
	require.NotNil(t, byKey["key1"].ETag)
	assert.Equal(t, hex.EncodeToString(rowVer1), *byKey["key1"].ETag)
	assert.Empty(t, byKey["key1"].Error)
	assert.Equal(t, []byte(`"value2"`), byKey["key2"].Data)
	require.NotNil(t, byKey["key2"].ETag)
	assert.Equal(t, hex.EncodeToString(rowVer2), *byKey["key2"].ETag)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetMissingKeys(t *testing.T) {
	t.Parallel()

	s, mock, cleanup := newTestSQLServer(t, defaultBulkGetChunkSize)
	defer cleanup()

	rows := sqlmock.NewRows(bulkCols).
		AddRow("key1", `"value1"`, nil, false, []byte{0x01}, nil)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{{Key: "key1"}, {Key: "key2"}, {Key: "key3"}}

	res, err := s.BulkGet(t.Context(), req, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, res, 3)

	byKey := indexByKey(res)
	assert.NotNil(t, byKey["key1"].ETag)
	assert.Equal(t, []byte(`"value1"`), byKey["key1"].Data)
	assert.Empty(t, byKey["key1"].Error)

	for _, k := range []string{"key2", "key3"} {
		r := byKey[k]
		assert.Equal(t, k, r.Key)
		assert.Empty(t, r.Error)
		assert.Nil(t, r.Data)
		assert.Nil(t, r.ETag)
		assert.Nil(t, r.Metadata)
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetWithExpiration(t *testing.T) {
	t.Parallel()

	s, mock, cleanup := newTestSQLServer(t, defaultBulkGetChunkSize)
	defer cleanup()

	expireAt := time.Now().Add(1 * time.Hour).UTC()
	rows := sqlmock.NewRows(bulkCols).
		AddRow("key1", `"value1"`, nil, false, []byte{0x01}, expireAt)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{{Key: "key1"}}

	res, err := s.BulkGet(t.Context(), req, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.NotNil(t, res[0].Metadata)
	assert.Equal(t, expireAt.Format(time.RFC3339), res[0].Metadata[state.GetRespMetaKeyTTLExpireTime])

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetBinary(t *testing.T) {
	t.Parallel()

	s, mock, cleanup := newTestSQLServer(t, defaultBulkGetChunkSize)
	defer cleanup()

	binaryPayload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	// isBinary=true rows have NULL Data and use BinaryData. Mirror the v2
	// schema: Data column is NULL when isBinary=1.
	rows := sqlmock.NewRows(bulkCols).
		AddRow("k_text", `"plain"`, nil, false, []byte{0x01}, nil).
		AddRow("k_bin", nil, binaryPayload, true, []byte{0x02}, nil)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{{Key: "k_text"}, {Key: "k_bin"}}

	res, err := s.BulkGet(t.Context(), req, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, res, 2)

	byKey := indexByKey(res)
	assert.Equal(t, []byte(`"plain"`), byKey["k_text"].Data)
	assert.Empty(t, byKey["k_text"].Error)
	assert.Equal(t, binaryPayload, byKey["k_bin"].Data)
	assert.Empty(t, byKey["k_bin"].Error)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetNonBinaryWithNullDataIsError(t *testing.T) {
	t.Parallel()

	s, mock, cleanup := newTestSQLServer(t, defaultBulkGetChunkSize)
	defer cleanup()

	// Defensive: shouldn't happen in production (non-binary rows always have
	// JSON Data), but if it does we surface a per-key error rather than panic.
	rows := sqlmock.NewRows(bulkCols).
		AddRow("k1", nil, nil, false, []byte{0x01}, nil)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{{Key: "k1"}}

	res, err := s.BulkGet(t.Context(), req, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, "k1", res[0].Key)
	assert.Contains(t, res[0].Error, "NULL Data column", "error message must describe what actually happened")
	assert.Nil(t, res[0].Data)
	assert.Nil(t, res[0].ETag)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetQueryFailure(t *testing.T) {
	t.Parallel()

	s, mock, cleanup := newTestSQLServer(t, defaultBulkGetChunkSize)
	defer cleanup()

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("connection refused"))

	req := []state.GetRequest{{Key: "key1"}, {Key: "key2"}}

	res, err := s.BulkGet(t.Context(), req, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, res, 2)

	for _, r := range res {
		assert.Contains(t, r.Error, "bulk get query failed:")
		assert.Contains(t, r.Error, "connection refused")
		assert.Nil(t, r.Data)
		assert.Nil(t, r.ETag)
	}
	assert.Equal(t, "key1", res[0].Key)
	assert.Equal(t, "key2", res[1].Key)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetRowsScanFailure(t *testing.T) {
	t.Parallel()

	s, mock, cleanup := newTestSQLServer(t, defaultBulkGetChunkSize)
	defer cleanup()

	// Wrong column type for isBinary forces Scan to fail for that row.
	// The dedup-and-map-back pattern can't attribute a scan error to a
	// specific request key (the row's key is lost on Scan failure), so
	// the affected key surfaces as a missing-key response (no Data, no
	// ETag, no Error). The warn log is the operational signal —
	// documented trade-off in the PR description.
	rows := sqlmock.NewRows(bulkCols).
		AddRow("k1", `"v1"`, nil, "not-a-bool", []byte{0x01}, nil)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{{Key: "k1"}}

	res, err := s.BulkGet(t.Context(), req, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, "k1", res[0].Key)
	assert.Empty(t, res[0].Error, "scan failure surfaces as a missing-key response, not a per-key error")
	assert.Nil(t, res[0].Data)
	assert.Nil(t, res[0].ETag)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetDuplicateKeys(t *testing.T) {
	t.Parallel()

	// Duplicate request keys must yield duplicate response entries with
	// the same data, matching the default fan-out BulkStore semantics
	// (response length == request length, request order preserved). The
	// dedup-and-map-back pattern issues only distinct keys over the wire
	// and rebuilds the response by iterating the original request slice.
	s, mock, cleanup := newTestSQLServer(t, defaultBulkGetChunkSize)
	defer cleanup()

	rowVer1 := []byte{0x01}
	rowVer2 := []byte{0x02}
	rows := sqlmock.NewRows(bulkCols).
		AddRow("k1", `"v1"`, nil, false, rowVer1, nil).
		AddRow("k2", `"v2"`, nil, false, rowVer2, nil)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{
		{Key: "k1"},
		{Key: "k1"},
		{Key: "k2"},
		{Key: "k1"},
	}

	res, err := s.BulkGet(t.Context(), req, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, res, 4, "response length must match request length even with duplicates")

	expected := []struct {
		key    string
		data   []byte
		rowVer []byte
	}{
		{"k1", []byte(`"v1"`), rowVer1},
		{"k1", []byte(`"v1"`), rowVer1},
		{"k2", []byte(`"v2"`), rowVer2},
		{"k1", []byte(`"v1"`), rowVer1},
	}
	for i, e := range expected {
		assert.Equal(t, e.key, res[i].Key, "position %d", i)
		assert.Equal(t, e.data, res[i].Data, "position %d", i)
		require.NotNil(t, res[i].ETag, "position %d", i)
		assert.Equal(t, hex.EncodeToString(e.rowVer), *res[i].ETag, "position %d", i)
		assert.Empty(t, res[i].Error, "position %d", i)
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetRowsErrFailure(t *testing.T) {
	t.Parallel()

	s, mock, cleanup := newTestSQLServer(t, defaultBulkGetChunkSize)
	defer cleanup()

	rows := sqlmock.NewRows(bulkCols).
		AddRow("key1", `"value1"`, nil, false, []byte{0x01}, nil).
		AddRow("key2", `"value2"`, nil, false, []byte{0x02}, nil).
		RowError(1, errors.New("network timeout"))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{{Key: "key1"}, {Key: "key2"}, {Key: "key3"}}

	res, err := s.BulkGet(t.Context(), req, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, res, 3)

	byKey := indexByKey(res)
	assert.Empty(t, byKey["key1"].Error)
	assert.NotNil(t, byKey["key1"].Data)
	for _, k := range []string{"key2", "key3"} {
		r := byKey[k]
		assert.Equal(t, k, r.Key)
		assert.Contains(t, r.Error, "rows iteration failed:")
		assert.Contains(t, r.Error, "network timeout")
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetRowsErrFailure_WithDuplicates(t *testing.T) {
	t.Parallel()

	s, mock, cleanup := newTestSQLServer(t, defaultBulkGetChunkSize)
	defer cleanup()

	// key1 appears twice in the request. The mock scans key1 successfully
	// then rows.Err() fires before key2 / key3 are produced. Every
	// duplicate entry for a found key must keep the same populated data;
	// every entry for an unfound key (key2, key3) must surface the iter
	// error. This pins the duplicate-plus-iteration-error interaction
	// introduced by the dedup-and-map-back refactor.
	rows := sqlmock.NewRows(bulkCols).
		AddRow("key1", `"value1"`, nil, false, []byte{0x01}, nil).
		AddRow("key2", `"value2"`, nil, false, []byte{0x02}, nil).
		RowError(1, errors.New("network timeout"))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{
		{Key: "key1"},
		{Key: "key1"},
		{Key: "key2"},
		{Key: "key3"},
	}

	res, err := s.BulkGet(t.Context(), req, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, res, 4)

	// Both occurrences of key1 carry the same data; rd.found=true is
	// definitive even though iteration later failed.
	for _, i := range []int{0, 1} {
		assert.Equal(t, "key1", res[i].Key, "position %d", i)
		assert.Equal(t, []byte(`"value1"`), res[i].Data, "position %d", i)
		require.NotNil(t, res[i].ETag, "position %d", i)
		assert.Empty(t, res[i].Error, "position %d", i)
	}
	// key2 and key3 never reached found=true; the iter error fills them.
	for _, i := range []int{2, 3} {
		assert.Contains(t, res[i].Error, "rows iteration failed:", "position %d", i)
		assert.Contains(t, res[i].Error, "network timeout", "position %d", i)
		assert.Nil(t, res[i].Data, "position %d", i)
		assert.Nil(t, res[i].ETag, "position %d", i)
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetRowsErrAfterAllFound(t *testing.T) {
	t.Parallel()

	s, mock, cleanup := newTestSQLServer(t, defaultBulkGetChunkSize)
	defer cleanup()

	// Mock returns rows for k1 then triggers rows.Err() at the boundary
	// past the last row — every requested key has rd.found=true by the
	// time iteration ends, but rows.Err() is non-nil. The production
	// code must not silently drop this error: the per-key responses
	// still carry their data (correct, the rows were definitively
	// scanned), and the function warn-logs the iteration error so
	// transient driver/network failures stay diagnosable.
	rows := sqlmock.NewRows(bulkCols).
		AddRow("k1", `"v1"`, nil, false, []byte{0x01}, nil).
		RowError(1, errors.New("network reset after last row"))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{{Key: "k1"}}

	res, err := s.BulkGet(t.Context(), req, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, res, 1)
	// Data is preserved because k1's row was scanned successfully
	// before the iteration error fired.
	assert.Equal(t, "k1", res[0].Key)
	assert.Equal(t, []byte(`"v1"`), res[0].Data)
	require.NotNil(t, res[0].ETag)
	assert.Empty(t, res[0].Error)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetCaseDifferingKeys_RequestCasingPreserved(t *testing.T) {
	t.Parallel()

	// Two request entries that differ only by case. The dedup-and-map-back
	// pattern sets res[i].Key = r.Key in the response loop, so each
	// response carries the original request casing regardless of what
	// the DB stored or returned.
	//
	// On a real SQL Server with the default case-insensitive collation,
	// both binds would match the same stored row and the DB would return
	// it once. The PR description documents that the in-tree code does
	// not attempt to detect or correct this — Dapr internals are
	// case-consistent. This test only pins the structural Go-side
	// guarantee: response Key always matches request Key.
	s, mock, cleanup := newTestSQLServer(t, defaultBulkGetChunkSize)
	defer cleanup()

	// Mock returns a row matching the lowercase request; sqlmock echoes
	// whatever we configure — it doesn't simulate CI collation.
	rows := sqlmock.NewRows(bulkCols).
		AddRow("key1", `"v1"`, nil, false, []byte{0x01}, nil)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{{Key: "KEY1"}, {Key: "key1"}}

	res, err := s.BulkGet(t.Context(), req, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, res, 2)
	assert.Equal(t, "KEY1", res[0].Key, "response Key must preserve request casing (uppercase entry)")
	assert.Equal(t, "key1", res[1].Key, "response Key must preserve request casing (lowercase entry)")
	// The lowercase request matched the returned row; uppercase did not
	// (rowByKey["KEY1"] stays at zero-value). On a real CI-collation DB
	// both would match; the in-tree behavior is documented in the PR
	// description.
	assert.Equal(t, []byte(`"v1"`), res[1].Data)
	require.NotNil(t, res[1].ETag)
	assert.Nil(t, res[0].Data)
	assert.Nil(t, res[0].ETag)
	assert.Empty(t, res[0].Error)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetChunking_MultipleChunks(t *testing.T) {
	t.Parallel()

	s, mock, cleanup := newTestSQLServer(t, 2)
	defer cleanup()

	// 5 keys at chunkSize=2 → 3 chunks (2, 2, 1).
	chunk1 := sqlmock.NewRows(bulkCols).
		AddRow("k1", `"v1"`, nil, false, []byte{0x01}, nil).
		AddRow("k2", `"v2"`, nil, false, []byte{0x02}, nil)
	chunk2 := sqlmock.NewRows(bulkCols).
		AddRow("k3", `"v3"`, nil, false, []byte{0x03}, nil).
		AddRow("k4", `"v4"`, nil, false, []byte{0x04}, nil)
	chunk3 := sqlmock.NewRows(bulkCols).
		AddRow("k5", `"v5"`, nil, false, []byte{0x05}, nil)

	mock.ExpectQuery("SELECT").WillReturnRows(chunk1)
	mock.ExpectQuery("SELECT").WillReturnRows(chunk2)
	mock.ExpectQuery("SELECT").WillReturnRows(chunk3)

	req := []state.GetRequest{
		{Key: "k1"}, {Key: "k2"}, {Key: "k3"}, {Key: "k4"}, {Key: "k5"},
	}

	res, err := s.BulkGet(t.Context(), req, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, res, 5)

	byKey := indexByKey(res)
	for i, k := range []string{"k1", "k2", "k3", "k4", "k5"} {
		assert.Equal(t, []byte(`"v`+string(rune('0'+i+1))+`"`), byKey[k].Data, "key %s", k)
		assert.Empty(t, byKey[k].Error, "key %s", k)
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetChunking_ExactMultiple(t *testing.T) {
	t.Parallel()

	s, mock, cleanup := newTestSQLServer(t, 2)
	defer cleanup()

	// 4 keys, chunkSize 2 → exactly 2 chunks.
	chunk1 := sqlmock.NewRows(bulkCols).
		AddRow("k1", `"v1"`, nil, false, []byte{0x01}, nil).
		AddRow("k2", `"v2"`, nil, false, []byte{0x02}, nil)
	chunk2 := sqlmock.NewRows(bulkCols).
		AddRow("k3", `"v3"`, nil, false, []byte{0x03}, nil).
		AddRow("k4", `"v4"`, nil, false, []byte{0x04}, nil)

	mock.ExpectQuery("SELECT").WillReturnRows(chunk1)
	mock.ExpectQuery("SELECT").WillReturnRows(chunk2)

	req := []state.GetRequest{{Key: "k1"}, {Key: "k2"}, {Key: "k3"}, {Key: "k4"}}

	res, err := s.BulkGet(t.Context(), req, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, res, 4)

	byKey := indexByKey(res)
	for i, k := range []string{"k1", "k2", "k3", "k4"} {
		assert.Equal(t, []byte(`"v`+string(rune('0'+i+1))+`"`), byKey[k].Data, "key %s", k)
		assert.Empty(t, byKey[k].Error, "key %s", k)
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetChunking_ContextAlreadyCanceled_ChunkedPath(t *testing.T) {
	t.Parallel()

	// Chunked path (len(req)=4 > chunkSize=2): the eager ctx.Err() check
	// at the top of the chunk loop fires before any query is issued, so
	// every requested key is reported with ctx.Err() as its error and no
	// SQL runs.
	s, mock, cleanup := newTestSQLServer(t, 2)
	defer cleanup()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	req := []state.GetRequest{{Key: "k1"}, {Key: "k2"}, {Key: "k3"}, {Key: "k4"}}

	res, err := s.BulkGet(ctx, req, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, res, 4)

	byKey := indexByKey(res)
	for _, k := range []string{"k1", "k2", "k3", "k4"} {
		r := byKey[k]
		assert.Equal(t, k, r.Key)
		assert.Equal(t, context.Canceled.Error(), r.Error, "fast and chunked paths must produce the same per-key error string for cancellation")
		assert.Nil(t, r.Data)
		assert.Nil(t, r.ETag)
	}

	// No ExpectQuery was registered, so a passing ExpectationsWereMet here
	// proves no SQL was issued — the cancellation short-circuit fired
	// before the first chunk.
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetChunking_ContextAlreadyCanceled_FastPath(t *testing.T) {
	t.Parallel()

	// Fast path (len(req) <= chunkSize): BulkGet's eager ctx.Err() check
	// fires before bulkGetChunk is invoked, so the per-key error format
	// matches the chunked path (plain ctx.Err() string, no driver wrap).
	// This unification means callers tuning bulkGetChunkSize see the same
	// cancellation behavior regardless of which path their request takes.
	s, mock, cleanup := newTestSQLServer(t, defaultBulkGetChunkSize)
	defer cleanup()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	req := []state.GetRequest{{Key: "k1"}, {Key: "k2"}}

	res, err := s.BulkGet(ctx, req, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, res, 2)

	byKey := indexByKey(res)
	for _, k := range []string{"k1", "k2"} {
		r := byKey[k]
		assert.Equal(t, k, r.Key)
		assert.Equal(t, context.Canceled.Error(), r.Error, "fast and chunked paths must produce the same per-key error string for cancellation")
		assert.Nil(t, r.Data)
		assert.Nil(t, r.ETag)
	}

	// No expectations registered, and ExpectationsWereMet passing here
	// proves the eager check short-circuited before any query was issued.
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestNormalizeBulkGetChunkSize(t *testing.T) {
	t.Parallel()

	log := logger.NewLogger("test")

	tests := []struct {
		name string
		in   int
		want int
	}{
		{"zero defaults", 0, defaultBulkGetChunkSize},
		{"negative defaults", -5, defaultBulkGetChunkSize},
		{"under default kept", 100, 100},
		{"at default kept", defaultBulkGetChunkSize, defaultBulkGetChunkSize},
		{"between default and max kept", 1500, 1500},
		{"at max kept", maxBulkGetChunkSize, maxBulkGetChunkSize},
		{"above max clamped", maxBulkGetChunkSize + 1, maxBulkGetChunkSize},
		{"way above max clamped", 10000, maxBulkGetChunkSize},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := normalizeBulkGetChunkSize(log, tt.in)
			assert.Equal(t, tt.want, got)
		})
	}
}

// indexByKey turns the result slice into a map for order-independent
// assertions in tests.
func indexByKey(res []state.BulkGetResponse) map[string]state.BulkGetResponse {
	m := make(map[string]state.BulkGetResponse, len(res))
	for _, r := range res {
		m[r.Key] = r
	}
	return m
}

// Compile-time assertion: BulkGet on *SQLServer satisfies the BulkStore
// signature. If this fails, the embedded BulkStore would be used instead.
//
//nolint:staticcheck // S1021: explicit type kept to guard the method shape
var _ = func() bool {
	var s *SQLServer
	var fn func(ctx context.Context, req []state.GetRequest, opts state.BulkGetOpts) ([]state.BulkGetResponse, error)
	fn = s.BulkGet
	_ = fn
	return true
}()
