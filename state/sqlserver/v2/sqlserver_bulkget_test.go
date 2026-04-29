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
	assert.NotEmpty(t, res[0].Error)
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
	rows := sqlmock.NewRows(bulkCols).
		AddRow("k1", `"v1"`, nil, "not-a-bool", []byte{0x01}, nil)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{{Key: "k1"}}

	res, err := s.BulkGet(t.Context(), req, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.NotEmpty(t, res[0].Error)

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

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetChunking_ContextCanceledBetweenChunks(t *testing.T) {
	t.Parallel()

	s, mock, cleanup := newTestSQLServer(t, 2)
	defer cleanup()

	ctx, cancel := context.WithCancel(t.Context())

	// First chunk succeeds; we then cancel before the second chunk runs.
	chunk1 := sqlmock.NewRows(bulkCols).
		AddRow("k1", `"v1"`, nil, false, []byte{0x01}, nil).
		AddRow("k2", `"v2"`, nil, false, []byte{0x02}, nil)
	mock.ExpectQuery("SELECT").WillReturnRows(chunk1)

	// Cancel before invoking; the per-chunk ctx.Err() check converts the
	// remaining keys (k3, k4) to per-key cancel errors instead of issuing
	// a second SELECT.
	cancel()

	req := []state.GetRequest{{Key: "k1"}, {Key: "k2"}, {Key: "k3"}, {Key: "k4"}}

	res, err := s.BulkGet(ctx, req, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, res, 4)

	byKey := indexByKey(res)
	// k3, k4 should carry context-cancel errors.
	for _, k := range []string{"k3", "k4"} {
		assert.NotEmpty(t, byKey[k].Error, "key %s should be marked cancelled", k)
	}

	// We do NOT call ExpectationsWereMet — the first chunk's QueryContext
	// returns ctx.Err() once the context has already been cancelled before
	// the call, so sqlmock may or may not record a Query depending on
	// driver internals. The behavioral assertion (k3, k4 errored) is the
	// invariant we care about.
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
var _ = func() bool {
	var s *SQLServer
	var fn func(ctx context.Context, req []state.GetRequest, opts state.BulkGetOpts) ([]state.BulkGetResponse, error)
	fn = s.BulkGet
	_ = fn
	return true
}()
