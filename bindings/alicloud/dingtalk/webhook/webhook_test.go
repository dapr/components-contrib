// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package webhook

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestPublishMsg(t *testing.T) { //nolint:paralleltest
	msg := "{\"type\": \"text\",\"text\": {\"content\": \"hello\"}}"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("{\"errcode\":0}"))
		require.NoError(t, err)
		if r.Method != "POST" {
			t.Errorf("Expected 'POST' request, got '%s'", r.Method)
		}
		if r.URL.EscapedPath() != "/test" {
			t.Errorf("Expected request to '/test', got '%s'", r.URL.EscapedPath())
		}

		body, err := ioutil.ReadAll(r.Body)
		require.Nil(t, err)
		assert.Equal(t, msg, string(body))
	}))
	defer ts.Close()

	m := bindings.Metadata{Name: "test", Properties: map[string]string{
		"url":    ts.URL + "/test",
		"secret": "",
		"id":     "x",
	}}

	d := NewDingTalkWebhook(logger.NewLogger("test"))
	err := d.Init(m)
	require.NoError(t, err)

	req := &bindings.InvokeRequest{Data: []byte(msg), Operation: bindings.CreateOperation, Metadata: map[string]string{}}
	_, err = d.Invoke(req)
	require.NoError(t, err)
}

func TestBindingReadAndInvoke(t *testing.T) { //nolint:paralleltest
	msg := "{\"type\": \"text\",\"text\": {\"content\": \"hello\"}}"

	m := bindings.Metadata{Name: "test",
		Properties: map[string]string{
			"url":    "/test",
			"secret": "",
			"id":     "x",
		}}

	d := NewDingTalkWebhook(logger.NewLogger("test"))
	err := d.Init(m)
	assert.NoError(t, err)

	var count int32
	ch := make(chan bool, 1)

	handler := func(in *bindings.ReadResponse) ([]byte, error) {
		assert.Equal(t, msg, string(in.Data))
		atomic.AddInt32(&count, 1)
		ch <- true

		return nil, nil
	}

	err = d.Read(handler)
	require.NoError(t, err)

	req := &bindings.InvokeRequest{Data: []byte(msg), Operation: bindings.GetOperation, Metadata: map[string]string{}}
	_, err = d.Invoke(req)
	require.NoError(t, err)

	select {
	case <-ch:
		require.True(t, atomic.LoadInt32(&count) > 0)
	case <-time.After(time.Second):
		require.FailNow(t, "read timeout")
	}
}
