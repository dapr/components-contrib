/*
Copyright 2021 The Dapr Authors
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

package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/dapr/components-contrib/metadata"
	"os"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestQuery(t *testing.T) {
	m, mock, _ := mockDatabase(t)

	defer func() {
		if err := m.Close(); err != nil {
			t.Errorf("failed to close database: %s", err)
		}
	}()

	t.Run("no dbType provided", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"id", "value", "timestamp"}).
			AddRow(1, "value-1", time.Now()).
			AddRow(2, "value-2", time.Now().Add(1000)).
			AddRow(3, "value-3", time.Now().Add(2000))

		mock.ExpectQuery("SELECT \\* FROM foo WHERE id < 4").WillReturnRows(rows)
		ret, err := m.query(context.Background(), `SELECT * FROM foo WHERE id < 4`)
		require.NoError(t, err)
		t.Logf("query result: %s", ret)
		assert.Contains(t, string(ret), "\"id\":1")
		var result []any
		err = json.Unmarshal(ret, &result)
		require.NoError(t, err)
		assert.Len(t, result, 3)
	})

	t.Run("dbType provided", func(t *testing.T) {
		col1 := sqlmock.NewColumn("id").OfType("BIGINT", 1)
		col2 := sqlmock.NewColumn("value").OfType("FLOAT", 1.0)
		col3 := sqlmock.NewColumn("timestamp").OfType("TIME", time.Now())
		rows := sqlmock.NewRowsWithColumnDefinition(col1, col2, col3).
			AddRow(1, 1.1, time.Now()).
			AddRow(2, 2.2, time.Now().Add(1000)).
			AddRow(3, 3.3, time.Now().Add(2000))
		mock.ExpectQuery("SELECT \\* FROM foo WHERE id < 4").WillReturnRows(rows)
		ret, err := m.query(context.Background(), "SELECT * FROM foo WHERE id < 4")
		require.NoError(t, err)
		t.Logf("query result: %s", ret)

		// verify number
		assert.Contains(t, string(ret), "\"id\":1")
		assert.Contains(t, string(ret), "\"value\":2.2")

		var result []any
		err = json.Unmarshal(ret, &result)
		require.NoError(t, err)
		assert.Len(t, result, 3)

		// verify timestamp
		ts, ok := result[0].(map[string]any)["timestamp"].(string)
		assert.True(t, ok)
		var tt time.Time
		tt, err = time.Parse(time.RFC3339, ts)
		require.NoError(t, err)
		t.Logf("time stamp is: %v", tt)
	})
}

func TestExec(t *testing.T) {
	m, mock, _ := mockDatabase(t)

	defer func() {
		if err := m.Close(); err != nil {
			t.Errorf("failed to close database: %s", err)
		}
	}()

	mock.ExpectExec("INSERT INTO foo \\(id, v1, ts\\) VALUES \\(.*\\)").WillReturnResult(sqlmock.NewResult(1, 1))
	i, err := m.exec(context.Background(), "INSERT INTO foo (id, v1, ts) VALUES (1, 'test-1', '2021-01-22')")
	assert.Equal(t, int64(1), i)
	require.NoError(t, err)
}

func TestInvoke(t *testing.T) {
	m, mock, _ := mockDatabase(t)

	defer func() {
		if err := m.Close(); err != nil {
			t.Errorf("failed to close database: %s", err)
		}
	}()

	t.Run("exec operation succeeds", func(t *testing.T) {
		mock.ExpectExec("INSERT INTO foo \\(id, v1, ts\\) VALUES \\(.*\\)").WillReturnResult(sqlmock.NewResult(1, 1))
		md := map[string]string{commandSQLKey: "INSERT INTO foo (id, v1, ts) VALUES (1, 'test-1', '2021-01-22')"}
		req := &bindings.InvokeRequest{
			Data:      nil,
			Metadata:  md,
			Operation: execOperation,
		}
		resp, err := m.Invoke(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, "1", resp.Metadata[respRowsAffectedKey])
	})

	t.Run("exec operation fails", func(t *testing.T) {
		mock.ExpectExec("INSERT INTO foo \\(id, v1, ts\\) VALUES \\(.*\\)").WillReturnError(errors.New("insert failed"))
		md := map[string]string{commandSQLKey: "INSERT INTO foo (id, v1, ts) VALUES (1, 'test-1', '2021-01-22')"}
		req := &bindings.InvokeRequest{
			Data:      nil,
			Metadata:  md,
			Operation: execOperation,
		}
		resp, err := m.Invoke(context.Background(), req)
		assert.Nil(t, resp)
		require.Error(t, err)
	})

	t.Run("query operation succeeds", func(t *testing.T) {
		col1 := sqlmock.NewColumn("id").OfType("BIGINT", 1)
		col2 := sqlmock.NewColumn("value").OfType("FLOAT", 1.0)
		col3 := sqlmock.NewColumn("timestamp").OfType("TIME", time.Now())
		rows := sqlmock.NewRowsWithColumnDefinition(col1, col2, col3).AddRow(1, 1.1, time.Now())
		mock.ExpectQuery("SELECT \\* FROM foo WHERE id < \\d+").WillReturnRows(rows)

		md := map[string]string{commandSQLKey: "SELECT * FROM foo WHERE id < 2"}
		req := &bindings.InvokeRequest{
			Data:      nil,
			Metadata:  md,
			Operation: queryOperation,
		}
		resp, err := m.Invoke(context.Background(), req)
		require.NoError(t, err)
		var data []any
		err = json.Unmarshal(resp.Data, &data)
		require.NoError(t, err)
		assert.Len(t, data, 1)
	})

	t.Run("query operation fails", func(t *testing.T) {
		mock.ExpectQuery("SELECT \\* FROM foo WHERE id < \\d+").WillReturnError(errors.New("query failed"))
		md := map[string]string{commandSQLKey: "SELECT * FROM foo WHERE id < 2"}
		req := &bindings.InvokeRequest{
			Data:      nil,
			Metadata:  md,
			Operation: queryOperation,
		}
		resp, err := m.Invoke(context.Background(), req)
		assert.Nil(t, resp)
		require.Error(t, err)
	})

	t.Run("close operation", func(t *testing.T) {
		mock.ExpectClose()
		req := &bindings.InvokeRequest{
			Operation: closeOperation,
		}
		resp, _ := m.Invoke(context.Background(), req)
		assert.Nil(t, resp)
	})

	t.Run("unsupported operation", func(t *testing.T) {
		req := &bindings.InvokeRequest{
			Data:      nil,
			Metadata:  map[string]string{},
			Operation: "unsupported",
		}
		resp, err := m.Invoke(context.Background(), req)
		assert.Nil(t, resp)
		require.Error(t, err)
	})
}

func mockDatabase(t *testing.T) (*Mysql, sqlmock.Sqlmock, error) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	m := NewMysql(logger.NewLogger("test")).(*Mysql)
	m.db = db

	return m, mock, err
}

func TestInit(t *testing.T) {
	// We expect Init to return an error because we do not have a running MySQL
	// instance, but we want to make sure the error message is not about the SSL cert
	// or parsing the DSN.

	// This is a publicly available SSL cert for Azure MySQL.
	// https://dl.cacerts.digicert.com/DigiCertGlobalRootCA.crt.pem
	validPem := `-----BEGIN CERTIFICATE-----
MIIDrzCCApegAwIBAgIQCDvgVpBCRrGhdWrJWZHHSjANBgkqhkiG9w0BAQUFADBh
MQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMRkwFwYDVQQLExB3
d3cuZGlnaWNlcnQuY29tMSAwHgYDVQQDExdEaWdpQ2VydCBHbG9iYWwgUm9vdCBD
QTAeFw0wNjExMTAwMDAwMDBaFw0zMTExMTAwMDAwMDBaMGExCzAJBgNVBAYTAlVT
MRUwEwYDVQQKEwxEaWdpQ2VydCBJbmMxGTAXBgNVBAsTEHd3dy5kaWdpY2VydC5j
b20xIDAeBgNVBAMTF0RpZ2lDZXJ0IEdsb2JhbCBSb290IENBMIIBIjANBgkqhkiG
9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4jvhEXLeqKTTo1eqUKKPC3eQyaKl7hLOllsB
CSDMAZOnTjC3U/dDxGkAV53ijSLdhwZAAIEJzs4bg7/fzTtxRuLWZscFs3YnFo97
nh6Vfe63SKMI2tavegw5BmV/Sl0fvBf4q77uKNd0f3p4mVmFaG5cIzJLv07A6Fpt
43C/dxC//AH2hdmoRBBYMql1GNXRor5H4idq9Joz+EkIYIvUX7Q6hL+hqkpMfT7P
T19sdl6gSzeRntwi5m3OFBqOasv+zbMUZBfHWymeMr/y7vrTC0LUq7dBMtoM1O/4
gdW7jVg/tRvoSSiicNoxBN33shbyTApOB6jtSj1etX+jkMOvJwIDAQABo2MwYTAO
BgNVHQ8BAf8EBAMCAYYwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUA95QNVbR
TLtm8KPiGxvDl7I90VUwHwYDVR0jBBgwFoAUA95QNVbRTLtm8KPiGxvDl7I90VUw
DQYJKoZIhvcNAQEFBQADggEBAMucN6pIExIK+t1EnE9SsPTfrgT1eXkIoyQY/Esr
hMAtudXH/vTBH1jLuG2cenTnmCmrEbXjcKChzUyImZOMkXDiqw8cvpOp/2PV5Adg
06O/nVsJ8dWO41P0jmP6P6fbtGbfYmbW0W5BjfIttep3Sp+dWOIrWcBAI+0tKIJF
PnlUkiaY4IBIqDfv8NZ5YBberOgOzW6sRBc4L0na4UU+Krk2U886UAb3LujEV0ls
YSEY1QSteDwsOoBrp+uvFRTp2InBuThs4pFsiv9kuXclVzDAGySj4dzp30d8tbQk
CAUw7C29C79Fv1C5qfPrmAESrciIxpg0X40KPMbp1ZWVbd4=
-----END CERTIFICATE-----`

	invalidPem := `-----BEGIN CERTIFICATE--
MIIDrzCCApegAwIBAgIQCDvgVpBCRrGhdWrJWZHHSjANBgkqhkiG9w0BAQUFADBh
MQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMRkwFwYDVQQLExB3
d3cuZGlnaWNlcnQuY29tMSAwHgYDVQQDExdEaWdpQ2VydCBHbG9iYWwgUm9vdCBD
QTAeFw0wNjExMTAwMDAwMDBaFw0zMTExMTAwMDAwMDBaMGExCzAJBgNVBAYTAlVT
MRUwEwYDVQQKEwxEaWdpQ2VydCBJbmMxGTAXBgNVBAsTEHd3dy5kaWdpY2VydC5j
b20xIDAeBgNVBAMTF0RpZ2lDZXJ0IEdsb2JhbCBSb290IENBMIIBIjANBgkqhkiG
9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4jvhEXLeqKTTo1eqUKKPC3eQyaKl7hLOllsB
CSDMAZOnTjC3U/dDxGkAV53ijSLdhwZAAIEJzs4bg7/fzTtxRuLWZscFs3YnFo97
nh6Vfe63SKMI2tavegw5BmV/Sl0fvBf4q77uKNd0f3p4mVmFaG5cIzJLv07A6Fpt
43C/dxC//AH2hdmoRBBYMql1GNXRor5H4idq9Joz+EkIYIvUX7Q6hL+hqkpMfT7P
T19sdl6gSzeRntwi5m3OFBqOasv+zbMUZBfHWymeMr/y7vrTC0LUq7dBMtoM1O/4
gdW7jVg/tRvoSSiicNoxBN33shbyTApOB6jtSj1etX+jkMOvJwIDAQABo2MwYTAO
BgNVHQ8BAf8EBAMCAYYwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUA95QNVbR
TLtm8KPiGxvDl7I90VUwHwYDVR0jBBgwFoAUA95QNVbRTLtm8KPiGxvDl7I90VUw
DQYJKoZIhvcNAQEFBQADggEBAMucN6pIExIK+t1EnE9SsPTfrgT1eXkIoyQY/Esr
hMAtudXH/vTBH1jLuG2cenTnmCmrEbXjcKChzUyImZOMkXDiqw8cvpOp/2PV5Adg
06O/nVsJ8dWO41P0jmP6P6fbtGbfYmbW0W5BjfIttep3Sp+dWOIrWcBAI+0tKIJF
PnlUkiaY4IBIqDfv8NZ5YBberOgOzW6sRBc4L0na4UU+Krk2U886UAb3LujEV0ls
YSEY1QSteDwsOoBrp+uvFRTp2InBuThs4pFsiv9kuXclVzDAGySj4dzp30d8tbQk
CAUw7C29C79Fv1C5qfPrmAESrciIxpg0X40KPMbp1ZWVbd4=
-----END CERTIFICATE--`

	t.Run("init with no pemFile or pemContents", func(t *testing.T) {
		url := "user:secret@tcp(localhost:3306)/test"
		sqlMeta := createMetaData(url, "", "")

		b := NewMysql(logger.NewLogger("test")).(*Mysql)
		if err := b.Init(context.Background(), sqlMeta); err != nil {
			if !strings.Contains(err.Error(), "unable to ping the DB") {
				t.Errorf("failed to init database: %s", err)
			}
		}
	})

	t.Run("init with pemFile", func(t *testing.T) {
		// Copy the valid PEM file to a temporary location.
		tempFile := "cert.pem"
		if err := os.WriteFile(tempFile, []byte(validPem), 0400); err != nil {
			t.Fatalf("failed to write to %s: %s", tempFile, err)
		}

		defer func() {
			if err := os.Remove(tempFile); err != nil {
				t.Fatalf("failed to remove %s: %s", tempFile, err)
			}
		}()

		url := "user:secret@tcp(localhost:3306)/test?tls=custom"
		sqlMeta := createMetaData(url, tempFile, "")

		b := NewMysql(logger.NewLogger("test")).(*Mysql)
		if err := b.Init(context.Background(), sqlMeta); err == nil {
			t.Errorf("expected unable to ping the DB, got nil")
		} else if !strings.Contains(err.Error(), "unable to ping the DB") {
			t.Errorf("expected unable to ping the DB, got %s", err)
		}
	})

	t.Run("init with corrupted pemFile", func(t *testing.T) {
		// Copy the invalid PEM file to a temporary location.
		tempFile := "cert.pem"
		if err := os.WriteFile(tempFile, []byte(invalidPem), 0400); err != nil {
			t.Fatalf("failed to write to %s: %s", tempFile, err)
		}

		defer func() {
			if err := os.Remove(tempFile); err != nil {
				t.Fatalf("failed to remove %s: %s", tempFile, err)
			}
		}()

		url := "user:secret@tcp(localhost:3306)/test?tls=custom"
		sqlMeta := createMetaData(url, tempFile, "")

		b := NewMysql(logger.NewLogger("test")).(*Mysql)
		if err := b.Init(context.Background(), sqlMeta); err == nil {
			t.Errorf("expected failed to decode PEM error, got nil")
		} else if !strings.Contains(err.Error(), "failed to decode PEM") {
			t.Errorf("failed to init database: %s", err)
		}
	})

	t.Run("init with valid pemContents and corrupt pemFile", func(t *testing.T) {
		// Copy the invalid PEM file to a temporary location.
		tempFile := "cert.pem"
		if err := os.WriteFile(tempFile, []byte(invalidPem), 0400); err != nil {
			t.Fatalf("failed to write to %s: %s", tempFile, err)
		}

		defer func() {
			if err := os.Remove(tempFile); err != nil {
				t.Fatalf("failed to remove %s: %s", tempFile, err)
			}
		}()

		url := "user:secret@tcp(localhost:3306)/test?tls=custom"
		sqlMeta := createMetaData(url, tempFile, validPem)

		b := NewMysql(logger.NewLogger("test")).(*Mysql)
		if err := b.Init(context.Background(), sqlMeta); err == nil {
			t.Errorf("expected unable to ping the DB, got nil")
		} else if !strings.Contains(err.Error(), "unable to ping the DB") {
			t.Errorf("expected unable to ping the DB, got %s", err)
		}
	})

	t.Run("init with pemContents", func(t *testing.T) {
		url := "user:secret@tcp(localhost:3306)/test?tls=custom"
		sqlMeta := createMetaData(url, "", validPem)

		b := NewMysql(logger.NewLogger("test")).(*Mysql)
		if err := b.Init(context.Background(), sqlMeta); err != nil {
			if !strings.Contains(err.Error(), "unable to ping the DB") {
				t.Errorf("failed to init database: %s", err)
			}

		}
	})

	t.Run("init with corrupted pemContents", func(t *testing.T) {
		url := "user:secret@tcp(localhost:3306)/test?tls=custom"
		sqlMeta := createMetaData(url, "", invalidPem)

		b := NewMysql(logger.NewLogger("test")).(*Mysql)
		if err := b.Init(context.Background(), sqlMeta); err == nil {
			t.Errorf("expected failed to decode PEM error, got nil")
		} else if !strings.Contains(err.Error(), "failed to decode PEM") {
			t.Errorf("failed to init database: %s", err)
		}
	})

	t.Run("init with valid pem and non-custom tls", func(t *testing.T) {
		url := "user:secret@tcp(localhost:3306)/test?tls=noncustom"
		sqlMeta := createMetaData(url, "", validPem)

		b := NewMysql(logger.NewLogger("test")).(*Mysql)
		if err := b.Init(context.Background(), sqlMeta); err == nil {
			t.Errorf("expected illegal Data Source Name (DSN) error, got nil")
		} else if !strings.Contains(err.Error(), "illegal Data Source Name (DSN)") {
			t.Errorf("expected illegal Data Source Name (DSN) error, got %s", err)
		}
	})
}

func createMetaData(url, pemPath, pemContents string) bindings.Metadata {
	return bindings.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"url":         url,
				"pemPath":     pemPath,
				"pemContents": pemContents,
			},
		},
	}
}
