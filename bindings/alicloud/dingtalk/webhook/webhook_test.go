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

package webhook

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func TestPublishMsg(t *testing.T) { //nolint:paralleltest
	msg := "{\"type\": \"text\",\"text\": {\"content\": \"hello\"}}"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("{\"errcode\":0}"))
		require.NoError(t, err)
		if r.Method != http.MethodPost {
			t.Errorf("Expected 'POST' request, got '%s'", r.Method)
		}
		if r.URL.EscapedPath() != "/test" {
			t.Errorf("Expected request to '/test', got '%s'", r.URL.EscapedPath())
		}

		body, err := io.ReadAll(r.Body)
		require.Nil(t, err)
		assert.Equal(t, msg, string(body))
	}))
	defer ts.Close()

	m := bindings.Metadata{Base: metadata.Base{Name: "test", Properties: map[string]string{
		"url":    ts.URL + "/test",
		"secret": "",
		"id":     "x",
	}}}

	d := NewDingTalkWebhook(logger.NewLogger("test"))
	err := d.Init(m)
	require.NoError(t, err)

	req := &bindings.InvokeRequest{Data: []byte(msg), Operation: bindings.CreateOperation, Metadata: map[string]string{}}
	_, err = d.Invoke(context.Background(), req)
	require.NoError(t, err)
}

func TestBindingReadAndInvoke(t *testing.T) { //nolint:paralleltest
	msg := "{\"type\": \"text\",\"text\": {\"content\": \"hello\"}}"

	m := bindings.Metadata{Base: metadata.Base{
		Name: "test",
		Properties: map[string]string{
			"url":    "/test",
			"secret": "",
			"id":     "x",
		},
	}}

	d := NewDingTalkWebhook(logger.NewLogger("test"))
	err := d.Init(m)
	assert.NoError(t, err)

	var count int32
	ch := make(chan bool, 1)

	handler := func(ctx context.Context, in *bindings.ReadResponse) ([]byte, error) {
		assert.Equal(t, msg, string(in.Data))
		atomic.AddInt32(&count, 1)
		ch <- true

		return nil, nil
	}

	err = d.Read(context.Background(), handler)
	require.NoError(t, err)

	req := &bindings.InvokeRequest{Data: []byte(msg), Operation: bindings.GetOperation, Metadata: map[string]string{}}
	_, err = d.Invoke(context.Background(), req)
	require.NoError(t, err)

	select {
	case <-ch:
		require.True(t, atomic.LoadInt32(&count) > 0)
	case <-time.After(time.Second):
		require.FailNow(t, "read timeout")
	}
}
