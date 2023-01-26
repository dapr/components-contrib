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

package sms

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

type mockTransport struct {
	response     *http.Response
	errToReturn  error
	request      *http.Request
	requestCount int32
}

func (t *mockTransport) reset() {
	atomic.StoreInt32(&t.requestCount, 0)
	t.request = nil
}

func (t *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	atomic.AddInt32(&t.requestCount, 1)
	t.request = req

	return t.response, t.errToReturn
}

func TestInit(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"toNumber": "toNumber", "fromNumber": "fromNumber"}
	tw := NewSMS(logger.NewLogger("test"))
	err := tw.Init(context.Background(), m)
	assert.NotNil(t, err)
}

func TestParseDuration(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"toNumber":   "toNumber",
		"fromNumber": "fromNumber",
		"accountSid": "accountSid",
		"authToken":  "authToken", "timeout": "badtimeout",
	}
	tw := NewSMS(logger.NewLogger("test"))
	err := tw.Init(context.Background(), m)
	assert.NotNil(t, err)
}

func TestWriteShouldSucceed(t *testing.T) {
	httpTransport := &mockTransport{
		response: &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))},
	}
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"toNumber":   "toNumber",
		"fromNumber": "fromNumber",
		"accountSid": "accountSid",
		"authToken":  "authToken",
	}
	tw := NewSMS(logger.NewLogger("test")).(*SMS)
	tw.httpClient = &http.Client{
		Transport: httpTransport,
	}
	err := tw.Init(context.Background(), m)
	assert.NoError(t, err)

	t.Run("Should succeed with expected url and headers", func(t *testing.T) {
		httpTransport.reset()
		_, err := tw.Invoke(context.Background(), &bindings.InvokeRequest{
			Data: []byte("hello world"),
			Metadata: map[string]string{
				toNumber: "toNumber",
			},
		})

		assert.NoError(t, err)
		assert.Equal(t, int32(1), httpTransport.requestCount)
		assert.Equal(t, "https://api.twilio.com/2010-04-01/Accounts/accountSid/Messages.json", httpTransport.request.URL.String())
		assert.NotNil(t, httpTransport.request)
		assert.Equal(t, "application/x-www-form-urlencoded", httpTransport.request.Header.Get("Content-Type"))
		assert.Equal(t, "application/json", httpTransport.request.Header.Get("Accept"))
		authUserName, authPassword, _ := httpTransport.request.BasicAuth()
		assert.Equal(t, "accountSid", authUserName)
		assert.Equal(t, "authToken", authPassword)
	})
}

func TestWriteShouldFail(t *testing.T) {
	httpTransport := &mockTransport{
		response: &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))},
	}
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"fromNumber": "fromNumber",
		"accountSid": "accountSid",
		"authToken":  "authToken",
	}
	tw := NewSMS(logger.NewLogger("test")).(*SMS)
	tw.httpClient = &http.Client{
		Transport: httpTransport,
	}
	err := tw.Init(context.Background(), m)
	assert.NoError(t, err)

	t.Run("Missing 'to' should fail", func(t *testing.T) {
		httpTransport.reset()
		_, err := tw.Invoke(context.Background(), &bindings.InvokeRequest{
			Data:     []byte("hello world"),
			Metadata: map[string]string{},
		})

		assert.NotNil(t, err)
	})

	t.Run("Twilio call failed should be returned", func(t *testing.T) {
		httpTransport.reset()
		httpErr := errors.New("twilio fake error")
		httpTransport.errToReturn = httpErr
		_, err := tw.Invoke(context.Background(), &bindings.InvokeRequest{
			Data: []byte("hello world"),
			Metadata: map[string]string{
				toNumber: "toNumber",
			},
		})

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), httpErr.Error())
	})

	t.Run("Twilio call returns status not >=200 and <300", func(t *testing.T) {
		httpTransport.reset()
		httpTransport.response.StatusCode = 401
		_, err := tw.Invoke(context.Background(), &bindings.InvokeRequest{
			Data: []byte("hello world"),
			Metadata: map[string]string{
				toNumber: "toNumber",
			},
		})

		assert.NotNil(t, err)
	})
}

func TestMessageBody(t *testing.T) {
	httpTransport := &mockTransport{
		response: &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))},
	}
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"toNumber":   "toNumber",
		"fromNumber": "fromNumber",
		"accountSid": "accountSid",
		"authToken":  "authToken",
	}
	tw := NewSMS(logger.NewLogger("test")).(*SMS)
	tw.httpClient = &http.Client{
		Transport: httpTransport,
	}
	err := tw.Init(context.Background(), m)
	require.NoError(t, err)

	tester := func(reqData []byte, expectBody string) func(t *testing.T) {
		return func(t *testing.T) {
			httpTransport.reset()
			_, err := tw.Invoke(context.Background(), &bindings.InvokeRequest{
				Data: reqData,
				Metadata: map[string]string{
					toNumber: "toNumber",
				},
			})

			require.NoError(t, err)
			assert.Equal(t, int32(1), httpTransport.requestCount)
			assert.Equal(t, "https://api.twilio.com/2010-04-01/Accounts/accountSid/Messages.json", httpTransport.request.URL.String())
			assert.NotNil(t, httpTransport.request)

			body, err := io.ReadAll(httpTransport.request.Body)
			require.NoError(t, err)
			q, err := url.ParseQuery(string(body))
			require.NoError(t, err)

			found := q.Get("Body")
			assert.Equal(t, expectBody, found)
		}
	}

	t.Run("Data is not JSON-encoded", func(t *testing.T) {
		t.Run("Message body is string", tester([]byte("hello world"), "hello world"))
		t.Run("Message body is int", tester([]byte("42"), "42"))
		t.Run("Message body is empty", tester([]byte(""), ""))
	})

	t.Run("Data is JSON-encoded", func(t *testing.T) {
		t.Run("Message body is string", tester([]byte(`"hello world"`), "hello world"))
		t.Run("Message body is int", tester([]byte("42"), "42"))
		t.Run("Message body is bool", tester([]byte("true"), "true"))
		t.Run("Message body is empty", tester([]byte(""), ""))
	})
}
