// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package sms

import (
	"errors"
	"io/ioutil"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

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
	err := tw.Init(m)
	assert.NotNil(t, err)
}

func TestParseDuration(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"toNumber": "toNumber", "fromNumber": "fromNumber",
		"accountSid": "accountSid", "authToken": "authToken", "timeout": "badtimeout",
	}
	tw := NewSMS(logger.NewLogger("test"))
	err := tw.Init(m)
	assert.NotNil(t, err)
}

func TestWriteShouldSucceed(t *testing.T) {
	httpTransport := &mockTransport{
		response: &http.Response{StatusCode: 200, Body: ioutil.NopCloser(strings.NewReader(""))},
	}
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"toNumber": "toNumber", "fromNumber": "fromNumber",
		"accountSid": "accountSid", "authToken": "authToken",
	}
	tw := NewSMS(logger.NewLogger("test"))
	tw.httpClient = &http.Client{
		Transport: httpTransport,
	}
	err := tw.Init(m)
	assert.Nil(t, err)

	t.Run("Should succeed with expected url and headers", func(t *testing.T) {
		httpTransport.reset()
		_, err := tw.Invoke(&bindings.InvokeRequest{
			Data: []byte("hello world"),
			Metadata: map[string]string{
				toNumber: "toNumber",
			},
		})

		assert.Nil(t, err)
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
		response: &http.Response{StatusCode: 200, Body: ioutil.NopCloser(strings.NewReader(""))},
	}
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"fromNumber": "fromNumber",
		"accountSid": "accountSid", "authToken": "authToken",
	}
	tw := NewSMS(logger.NewLogger("test"))
	tw.httpClient = &http.Client{
		Transport: httpTransport,
	}
	err := tw.Init(m)
	assert.Nil(t, err)

	t.Run("Missing 'to' should fail", func(t *testing.T) {
		httpTransport.reset()
		_, err := tw.Invoke(&bindings.InvokeRequest{
			Data:     []byte("hello world"),
			Metadata: map[string]string{},
		})

		assert.NotNil(t, err)
	})

	t.Run("Twilio call failed should be returned", func(t *testing.T) {
		httpTransport.reset()
		httpErr := errors.New("twilio fake error")
		httpTransport.errToReturn = httpErr
		_, err := tw.Invoke(&bindings.InvokeRequest{
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
		_, err := tw.Invoke(&bindings.InvokeRequest{
			Data: []byte("hello world"),
			Metadata: map[string]string{
				toNumber: "toNumber",
			},
		})

		assert.NotNil(t, err)
	})
}
