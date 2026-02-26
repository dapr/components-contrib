/*
Copyright 2025 The Dapr Authors
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

package oracledatabase

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

func TestConnectionString(t *testing.T) {
	tests := []struct {
		name           string
		metadata       map[string]string
		expectedConn   string
		expectedError  string
		withWallet     bool
		walletLocation string
	}{
		{
			name: "Simple URL format",
			metadata: map[string]string{
				"connectionString": "oracle://system:pass@localhost:1521/FREEPDB1",
			},
			expectedConn: "oracle://system:pass@localhost:1521/FREEPDB1?",
		},
		{
			name: "Pure descriptor format",
			metadata: map[string]string{
				"connectionString": "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))",
			},
			expectedConn: "oracle://:@:0/?connStr=%28DESCRIPTION%3D%28ADDRESS%3D%28PROTOCOL%3DTCP%29%28HOST%3Dlocalhost%29%28PORT%3D1521%29%29%28CONNECT_DATA%3D%28SERVICE_NAME%3DFREEPDB1%29%29%29",
		},
		{
			name: "URL with descriptor format",
			metadata: map[string]string{
				"connectionString": "oracle://system:pass@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))",
			},
			expectedConn: "oracle://system:pass@:0/?connStr=%28DESCRIPTION%3D%28ADDRESS%3D%28PROTOCOL%3DTCP%29%28HOST%3Dlocalhost%29%28PORT%3D1521%29%29%28CONNECT_DATA%3D%28SERVICE_NAME%3DFREEPDB1%29%29%29",
		},
		{
			name: "Complex descriptor with load balancing and failover",
			metadata: map[string]string{
				"connectionString": "(DESCRIPTION=(CONNECT_TIMEOUT=30)(RETRY_COUNT=20)(RETRY_DELAY=3)(FAILOVER=ON)(LOAD_BALANCE=OFF)(ADDRESS_LIST=(LOAD_BALANCE=ON)(ADDRESS=(PROTOCOL=TCP)(HOST=db1.example.com)(PORT=1521)))(ADDRESS_LIST=(LOAD_BALANCE=ON)(ADDRESS=(PROTOCOL=TCP)(HOST=db2.example.com)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1_service)))",
			},
			expectedConn: "oracle://:@:0/?connStr=%28DESCRIPTION%3D%28CONNECT_TIMEOUT%3D30%29%28RETRY_COUNT%3D20%29%28RETRY_DELAY%3D3%29%28FAILOVER%3DON%29%28LOAD_BALANCE%3DOFF%29%28ADDRESS_LIST%3D%28LOAD_BALANCE%3DON%29%28ADDRESS%3D%28PROTOCOL%3DTCP%29%28HOST%3Ddb1.example.com%29%28PORT%3D1521%29%29%29%28ADDRESS_LIST%3D%28LOAD_BALANCE%3DON%29%28ADDRESS%3D%28PROTOCOL%3DTCP%29%28HOST%3Ddb2.example.com%29%28PORT%3D1521%29%29%29%28CONNECT_DATA%3D%28SERVICE_NAME%3DFREEPDB1_service%29%29%29",
		},
		{
			name: "Simple URL with wallet",
			metadata: map[string]string{
				"connectionString":     "oracle://system:pass@localhost:1521/service",
				"oracleWalletLocation": "/path/to/wallet",
			},
			withWallet:     true,
			walletLocation: "/path/to/wallet",
			expectedConn:   "oracle://system:pass@localhost:1521/service?WALLET=%2Fpath%2Fto%2Fwallet&TRACE FILE=trace.log&SSL=enable&SSL Verify=false",
		},
		{
			name: "Descriptor with wallet",
			metadata: map[string]string{
				"connectionString":     "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=test.example.com)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))",
				"oracleWalletLocation": "/path/to/wallet",
			},
			withWallet:     true,
			walletLocation: "/path/to/wallet",
			expectedConn:   "oracle://:@:0/?WALLET=%2Fpath%2Fto%2Fwallet&TRACE FILE=trace.log&SSL=enable&SSL Verify=false&connStr=%28DESCRIPTION%3D%28ADDRESS%3D%28PROTOCOL%3DTCP%29%28HOST%3Dtest.example.com%29%28PORT%3D1521%29%29%28CONNECT_DATA%3D%28SERVICE_NAME%3DFREEPDB1%29%29%29",
		},
		{
			name: "URL with descriptor and existing parameters",
			metadata: map[string]string{
				"connectionString":     "oracle://system:pass@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=test.example.com)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))?param1=value1",
				"oracleWalletLocation": "/path/to/wallet",
			},
			withWallet:     true,
			walletLocation: "/path/to/wallet",
			expectedConn:   "oracle://system:pass@:0/?param1=value1&TRACE FILE=trace.log&SSL=enable&SSL Verify=false&WALLET=%2Fpath%2Fto%2Fwallet&connStr=%28DESCRIPTION%3D%28ADDRESS%3D%28PROTOCOL%3DTCP%29%28HOST%3Dtest.example.com%29%28PORT%3D1521%29%29%28CONNECT_DATA%3D%28SERVICE_NAME%3DFREEPDB1%29%29%29",
		},
		{
			name: "Compressed descriptor format",
			metadata: map[string]string{
				"connectionString": "(DESCRIPTION=(CONNECT_TIMEOUT=90)(RETRY_COUNT=20)(RETRY_DELAY=3)(TRANSPORT_CONNECT_TIMEOUT=3)(ADDRESS=(PROTOCOL=TCP)(HOST=db.example.com)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=MYSERVICE)))",
			},
			expectedConn: "oracle://@:0/?connStr=%28DESCRIPTION%3D%28CONNECT_TIMEOUT%3D90%29%28RETRY_COUNT%3D20%29%28RETRY_DELAY%3D3%29%28TRANSPORT_CONNECT_TIMEOUT%3D3%29%28ADDRESS%3D%28PROTOCOL%3DTCP%29%28HOST%3Ddb.example.com%29%28PORT%3D1521%29%29%28CONNECT_DATA%3D%28SERVICE_NAME%3DMYSERVICE%29%29%29",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create metadata
			metadata := state.Metadata{
				Base: metadata.Base{
					Properties: tt.metadata,
				},
			}

			meta, err := parseMetadata(metadata.Properties)
			require.NoError(t, err)

			actualConnectionString, err := parseConnectionString(meta)
			require.NoError(t, err)

			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
				return
			} else {
				require.NoError(t, err)
			}

			expectedURL, err := url.Parse(tt.expectedConn)
			require.NoError(t, err)
			actualURL, err := url.Parse(actualConnectionString)
			require.NoError(t, err)

			assert.Equal(t, expectedURL.Scheme, actualURL.Scheme)
			assert.Equal(t, expectedURL.Host, actualURL.Host)
			assert.Equal(t, expectedURL.Path, actualURL.Path)
			assert.Equal(t, expectedURL.User.Username(), actualURL.User.Username())
			ep, _ := expectedURL.User.Password()
			ap, _ := actualURL.User.Password()
			assert.Equal(t, ep, ap)

			query, err := url.ParseQuery(expectedURL.RawQuery)
			require.NoError(t, err)

			for k, v := range query {
				assert.Equal(t, v, actualURL.Query()[k])
			}

			if tt.withWallet {
				assert.Equal(t, tt.walletLocation, meta.OracleWalletLocation)
				assert.Contains(t, actualConnectionString, "WALLET=")
				assert.Contains(t, actualConnectionString, "SSL=enable")
			}
		})
	}
}

func TestBulkGetQueryFailure(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	o := &oracleDatabaseAccess{
		logger:   logger.NewLogger("test"),
		db:       db,
		metadata: oracleDatabaseMetadata{TableName: "state"},
	}

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("connection refused"))

	req := []state.GetRequest{
		{Key: "key1"},
		{Key: "key2"},
	}

	res, err := o.BulkGet(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, res, 2)

	for _, r := range res {
		assert.NotEmpty(t, r.Error)
		assert.Contains(t, r.Error, "connection refused")
		assert.Nil(t, r.Data)
		assert.Nil(t, r.ETag)
	}
	assert.Equal(t, "key1", res[0].Key)
	assert.Equal(t, "key2", res[1].Key)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetBinaryDecodeFailure(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	o := &oracleDatabaseAccess{
		logger:   logger.NewLogger("test"),
		db:       db,
		metadata: oracleDatabaseMetadata{TableName: "state"},
	}

	// First row: valid non-binary data
	// Second row: binary flag "Y" but value is not valid JSON-encoded base64
	// Third row: valid binary data
	validBinaryValue, _ := json.Marshal(base64.StdEncoding.EncodeToString([]byte("hello")))
	rows := sqlmock.NewRows([]string{"key", "value", "binary_yn", "etag", "expiration_time"}).
		AddRow("key1", `"plain text"`, "N", "etag1", nil).
		AddRow("key2", `not-valid-json`, "Y", "etag2", nil).
		AddRow("key3", string(validBinaryValue), "Y", "etag3", nil)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{
		{Key: "key1"},
		{Key: "key2"},
		{Key: "key3"},
	}

	res, err := o.BulkGet(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, res, 3)

	// key1 should succeed
	assert.Equal(t, "key1", res[0].Key)
	assert.Empty(t, res[0].Error)
	assert.NotNil(t, res[0].Data)

	// key2 should have an error with clean struct (no ETag/Metadata leaking)
	assert.Equal(t, "key2", res[1].Key)
	assert.NotEmpty(t, res[1].Error)
	assert.Nil(t, res[1].ETag, "error response should not leak ETag")
	assert.Nil(t, res[1].Metadata, "error response should not leak Metadata")

	// key3 should succeed
	assert.Equal(t, "key3", res[2].Key)
	assert.Empty(t, res[2].Error)
	assert.Equal(t, []byte("hello"), res[2].Data)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetBinaryBase64DecodeFailure(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	o := &oracleDatabaseAccess{
		logger:   logger.NewLogger("test"),
		db:       db,
		metadata: oracleDatabaseMetadata{TableName: "state"},
	}

	// Value is valid JSON string but not valid base64
	rows := sqlmock.NewRows([]string{"key", "value", "binary_yn", "etag", "expiration_time"}).
		AddRow("key1", `"not-valid-base64!!!"`, "Y", "etag1", nil)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{
		{Key: "key1"},
	}

	res, err := o.BulkGet(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, res, 1)

	assert.Equal(t, "key1", res[0].Key)
	assert.NotEmpty(t, res[0].Error)
	assert.Nil(t, res[0].ETag, "error response should not leak ETag")
	assert.Nil(t, res[0].Metadata, "error response should not leak Metadata")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetRowsErrFailure(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	o := &oracleDatabaseAccess{
		logger:   logger.NewLogger("test"),
		db:       db,
		metadata: oracleDatabaseMetadata{TableName: "state"},
	}

	// sqlmock's RowError(N, err) causes rows.Next() to return false (with the
	// error surfaced via rows.Err()) when attempting to read row at index N.
	// We need at least N+1 rows added for RowError(N) to fire; the error
	// prevents the Nth row from being read. So we add a dummy second row
	// (key2) and set RowError(1, ...) -- key1 at index 0 is scanned
	// successfully, then the attempt to advance to index 1 fails.
	rows := sqlmock.NewRows([]string{"key", "value", "binary_yn", "etag", "expiration_time"}).
		AddRow("key1", `"value1"`, "N", "etag1", nil).
		AddRow("key2", `"value2"`, "N", "etag2", nil).
		RowError(1, errors.New("network timeout"))

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{
		{Key: "key1"},
		{Key: "key2"},
		{Key: "key3"},
	}

	res, err := o.BulkGet(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, res, 3)

	// Build a map for easier assertion regardless of ordering.
	byKey := make(map[string]state.BulkGetResponse, len(res))
	for _, r := range res {
		byKey[r.Key] = r
	}

	// key1 was scanned successfully before the error.
	successfulResult := byKey["key1"]
	assert.Empty(t, successfulResult.Error)
	assert.NotNil(t, successfulResult.Data)

	// key2 and key3 were never returned by the DB; they should carry the
	// rows.Err() message as per-key errors.
	for _, k := range []string{"key2", "key3"} {
		r := byKey[k]
		assert.Equal(t, k, r.Key)
		assert.NotEmpty(t, r.Error, "unfound key %q should have an error from rows.Err()", k)
		assert.Contains(t, r.Error, "network timeout")
		assert.Nil(t, r.Data, "error entry should have nil data")
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetRowsErrAfterAllRowsProcessed(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	o := &oracleDatabaseAccess{
		logger:   logger.NewLogger("test"),
		db:       db,
		metadata: oracleDatabaseMetadata{TableName: "state"},
	}

	// Return the one requested row successfully, then trigger rows.Err().
	// This tests the warning-log path where all requested keys are found but
	// rows.Err() still reports an error (e.g. a late network hiccup).
	//
	// sqlmock limitation: CloseError only sets the error returned by
	// rows.Close(), NOT rows.Err(). To make rows.Err() return an error we
	// must use RowError. RowError(N) requires at least N+1 rows to be added
	// so that sqlmock actually attempts to advance to index N. We add a
	// dummy second row and set RowError(1, ...) -- row 0 (key1) is read
	// successfully, then the attempt to advance to row 1 fails, and
	// rows.Err() reports the error.
	rows := sqlmock.NewRows([]string{"key", "value", "binary_yn", "etag", "expiration_time"}).
		AddRow("key1", `"value1"`, "N", "etag1", nil).
		AddRow("dummy", `"dummy"`, "N", "dummy", nil).
		RowError(1, errors.New("late network error"))

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{
		{Key: "key1"},
	}

	res, err := o.BulkGet(t.Context(), req)
	require.NoError(t, err)
	// key1 was found before the error, so the warning-log branch is taken
	// (no unfound keys to attach the error to). The result should still
	// contain the successfully scanned key.
	require.Len(t, res, 1)
	assert.Equal(t, "key1", res[0].Key)
	assert.Empty(t, res[0].Error)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetWithExpiration(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	o := &oracleDatabaseAccess{
		logger:   logger.NewLogger("test"),
		db:       db,
		metadata: oracleDatabaseMetadata{TableName: "state"},
	}

	expTime := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{"key", "value", "binary_yn", "etag", "expiration_time"}).
		AddRow("key1", `"value1"`, "N", "etag1", expTime)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{
		{Key: "key1"},
	}

	res, err := o.BulkGet(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, "key1", res[0].Key)
	assert.Empty(t, res[0].Error)
	assert.NotNil(t, res[0].Metadata)
	assert.Contains(t, res[0].Metadata, state.GetRespMetaKeyTTLExpireTime)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetEmpty(t *testing.T) {
	t.Parallel()

	o := &oracleDatabaseAccess{
		logger:   logger.NewLogger("test"),
		metadata: oracleDatabaseMetadata{TableName: "state"},
	}

	res, err := o.BulkGet(t.Context(), []state.GetRequest{})
	require.NoError(t, err)
	assert.Empty(t, res)
}

func TestBulkGetMissingKeys(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	o := &oracleDatabaseAccess{
		logger:   logger.NewLogger("test"),
		db:       db,
		metadata: oracleDatabaseMetadata{TableName: "state"},
	}

	// DB only returns key2; key1 and key3 are not in the database.
	rows := sqlmock.NewRows([]string{"key", "value", "binary_yn", "etag", "expiration_time"}).
		AddRow("key2", `"value2"`, "N", "etag2", nil)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{
		{Key: "key1"},
		{Key: "key2"},
		{Key: "key3"},
	}

	res, err := o.BulkGet(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, res, 3)

	// Build a map for easier assertion regardless of ordering.
	byKey := make(map[string]state.BulkGetResponse, len(res))
	for _, r := range res {
		byKey[r.Key] = r
	}

	// key2 was returned by the DB — should have data and etag.
	r2 := byKey["key2"]
	assert.Empty(t, r2.Error)
	assert.NotNil(t, r2.Data)
	assert.NotNil(t, r2.ETag)

	// key1 and key3 were not in the DB — should be empty entries with no error.
	for _, k := range []string{"key1", "key3"} {
		r := byKey[k]
		assert.Equal(t, k, r.Key)
		assert.Empty(t, r.Error, "missing key should not have an error")
		assert.Nil(t, r.Data, "missing key should have nil data")
		assert.Nil(t, r.ETag, "missing key should have nil etag")
		assert.Nil(t, r.Metadata, "missing key should have nil metadata")
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetRowsScanFailure(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	o := &oracleDatabaseAccess{
		logger:   logger.NewLogger("test"),
		db:       db,
		metadata: oracleDatabaseMetadata{TableName: "state"},
	}

	// To trigger a Scan error, pass a string for the expiration_time column.
	// sql.NullTime.Scan cannot convert a plain string to time.Time,
	// so rows.Scan() will return an error for that row.
	rows := sqlmock.NewRows([]string{"key", "value", "binary_yn", "etag", "expiration_time"}).
		AddRow("key1", `"value1"`, "N", "etag1", nil).                    // valid row
		AddRow("key2", `"value2"`, "N", "etag2", "not-a-time-value").     // expiration_time is a string — Scan will fail
		AddRow("key3", `"value3"`, "N", "etag3", nil)                     // valid row

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	req := []state.GetRequest{
		{Key: "key1"},
		{Key: "key2"},
		{Key: "key3"},
	}

	res, err := o.BulkGet(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, res, 3)

	// key1 should succeed.
	assert.Equal(t, "key1", res[0].Key)
	assert.Empty(t, res[0].Error)
	assert.NotNil(t, res[0].Data)

	// key2 should have a per-key scan error.
	assert.NotEmpty(t, res[1].Error, "scan failure should produce a per-key error")
	assert.Nil(t, res[1].Data, "scan failure entry should have nil data")

	// key3 should succeed despite key2's failure.
	assert.Equal(t, "key3", res[2].Key)
	assert.Empty(t, res[2].Error)
	assert.NotNil(t, res[2].Data)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkGetEmptyKeyValidation(t *testing.T) {
	t.Parallel()

	o := &oracleDatabaseAccess{
		logger:   logger.NewLogger("test"),
		metadata: oracleDatabaseMetadata{TableName: "state"},
	}

	req := []state.GetRequest{
		{Key: "key1"},
		{Key: ""},
		{Key: "key3"},
	}

	res, err := o.BulkGet(t.Context(), req)
	require.Error(t, err)
	assert.Nil(t, res)
	assert.Equal(t, "missing key in bulk get operation", err.Error())
}
