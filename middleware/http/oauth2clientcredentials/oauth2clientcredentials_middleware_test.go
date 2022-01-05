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

package oauth2clientcredentials

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	fh "github.com/valyala/fasthttp"
	oauth2 "golang.org/x/oauth2"

	"github.com/dapr/components-contrib/middleware"
	mock "github.com/dapr/components-contrib/middleware/http/oauth2clientcredentials/mocks"
	"github.com/dapr/kit/logger"
)

func mockedRequestHandler(ctx *fh.RequestCtx) {}

// TestOAuth2ClientCredentialsMetadata will check
// - if the metadata checks are correct in place.
func TestOAuth2ClientCredentialsMetadata(t *testing.T) {
	// Specify components metadata
	var metadata middleware.Metadata

	// Missing all
	metadata.Properties = map[string]string{}

	log := logger.NewLogger("oauth2clientcredentials.test")
	_, err := NewOAuth2ClientCredentialsMiddleware(log).GetHandler(metadata)
	assert.EqualError(t, err, "Parameter 'headerName' needs to be set. Parameter 'clientID' needs to be set. Parameter 'clientSecret' needs to be set. Parameter 'scopes' needs to be set. Parameter 'tokenURL' needs to be set. Parameter 'authStyle' needs to be set. Parameter 'authStyle' can only have the values 0,1,2. Received: ''. ")

	// Invalid authStyle (non int)
	metadata.Properties = map[string]string{
		"clientID":     "testId",
		"clientSecret": "testSecret",
		"scopes":       "ascope",
		"tokenURL":     "https://localhost:9999",
		"headerName":   "someHeader",
		"authStyle":    "asdf", // This is the value to test
	}
	_, err2 := NewOAuth2ClientCredentialsMiddleware(log).GetHandler(metadata)
	assert.EqualError(t, err2, "Parameter 'authStyle' can only have the values 0,1,2. Received: 'asdf'. ")

	// Invalid authStyle (int > 2)
	metadata.Properties["authStyle"] = "3"
	_, err3 := NewOAuth2ClientCredentialsMiddleware(log).GetHandler(metadata)
	assert.EqualError(t, err3, "Parameter 'authStyle' can only have the values 0,1,2. Received: '3'. ")

	// Invalid authStyle (int < 0)
	metadata.Properties["authStyle"] = "-1"
	_, err4 := NewOAuth2ClientCredentialsMiddleware(log).GetHandler(metadata)
	assert.EqualError(t, err4, "Parameter 'authStyle' can only have the values 0,1,2. Received: '-1'. ")
}

// TestOAuth2ClientCredentialsToken will check
// - if the Token was added to the RequestHeader value specified.
func TestOAuth2ClientCredentialsToken(t *testing.T) {
	// Setup
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	// Mock mockTokenProvider
	mockTokenProvider := mock.NewMockTokenProviderInterface(mockCtrl)

	gomock.InOrder(
		// First call returning abc and Bearer, expires within 1 second
		mockTokenProvider.
			EXPECT().
			GetToken(gomock.Any()).
			Return(&oauth2.Token{
				AccessToken: "abcd",
				TokenType:   "Bearer",
				Expiry:      time.Now().In(time.UTC).Add(1 * time.Second),
			}, nil).
			Times(1),
	)

	// Specify components metadata
	var metadata middleware.Metadata
	metadata.Properties = map[string]string{
		"clientID":     "testId",
		"clientSecret": "testSecret",
		"scopes":       "ascope",
		"tokenURL":     "https://localhost:9999",
		"headerName":   "someHeader",
		"authStyle":    "1",
	}

	// Initialize middleware component and inject mocked TokenProvider
	log := logger.NewLogger("oauth2clientcredentials.test")
	oauth2clientcredentialsMiddleware := NewOAuth2ClientCredentialsMiddleware(log)
	oauth2clientcredentialsMiddleware.SetTokenProvider(mockTokenProvider)
	handler, err := oauth2clientcredentialsMiddleware.GetHandler(metadata)
	require.NoError(t, err)

	// First handler call should return abc Token
	var requestContext1 fh.RequestCtx
	handler(mockedRequestHandler)(&requestContext1)
	// Assertion
	assert.Equal(t, "Bearer abcd", string(requestContext1.Request.Header.Peek("someHeader")))
}

// TestOAuth2ClientCredentialsCache will check
// - if the Cache is working.
func TestOAuth2ClientCredentialsCache(t *testing.T) {
	// Setup
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	// Mock mockTokenProvider
	mockTokenProvider := mock.NewMockTokenProviderInterface(mockCtrl)

	gomock.InOrder(
		// First call returning abc and Bearer, expires within 1 second
		mockTokenProvider.
			EXPECT().
			GetToken(gomock.Any()).
			Return(&oauth2.Token{
				AccessToken: "abc",
				TokenType:   "Bearer",
				Expiry:      time.Now().In(time.UTC).Add(1 * time.Second),
			}, nil).
			Times(1),
		// Second call returning def and MAC, expires within 1 second
		mockTokenProvider.
			EXPECT().
			GetToken(gomock.Any()).
			Return(&oauth2.Token{
				AccessToken: "def",
				TokenType:   "MAC",
				Expiry:      time.Now().In(time.UTC).Add(1 * time.Second),
			}, nil).
			Times(1),
	)

	// Specify components metadata
	var metadata middleware.Metadata
	metadata.Properties = map[string]string{
		"clientID":     "testId",
		"clientSecret": "testSecret",
		"scopes":       "ascope",
		"tokenURL":     "https://localhost:9999",
		"headerName":   "someHeader",
		"authStyle":    "1",
	}

	// Initialize middleware component and inject mocked TokenProvider
	log := logger.NewLogger("oauth2clientcredentials.test")
	oauth2clientcredentialsMiddleware := NewOAuth2ClientCredentialsMiddleware(log)
	oauth2clientcredentialsMiddleware.SetTokenProvider(mockTokenProvider)
	handler, err := oauth2clientcredentialsMiddleware.GetHandler(metadata)
	require.NoError(t, err)

	// First handler call should return abc Token
	var requestContext1 fh.RequestCtx
	handler(mockedRequestHandler)(&requestContext1)
	// Assertion
	assert.Equal(t, "Bearer abc", string(requestContext1.Request.Header.Peek("someHeader")))

	// Second handler call should still return 'cached' abc Token
	var requestContext2 fh.RequestCtx
	handler(mockedRequestHandler)(&requestContext2)
	// Assertion
	assert.Equal(t, "Bearer abc", string(requestContext2.Request.Header.Peek("someHeader")))

	// Wait at a second to invalidate cache entry for abc
	time.Sleep(1 * time.Second)

	// Third call should return def Token
	var requestContext3 fh.RequestCtx
	handler(mockedRequestHandler)(&requestContext3)
	// Assertion
	assert.Equal(t, "MAC def", string(requestContext3.Request.Header.Peek("someHeader")))
}
