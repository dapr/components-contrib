// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package oauth2clientcredentials

import (
	"testing"
	"time"

	mock "github.com/dapr/components-contrib/middleware/http/oauth2clientcredentials/mocks"

	"github.com/dapr/components-contrib/middleware"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	fh "github.com/valyala/fasthttp"
	oauth2 "golang.org/x/oauth2"
)

func mockedRequestHandler(ctx *fh.RequestCtx) {
	// mockedRequestContext = *ctx
}

// TestOAuth2ClientCredentialsToken will check
// 1. If the Token was added to the RequestHeader value specified
// 2. If the Cache is working
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
		"clientID":       "testId",
		"clientSecret":   "testSecret",
		"tokenURL":       "https://localhost:9999",
		"authHeaderName": "someHeader",
		"authStyle":      "1",
	}

	// Initialize middleware component and inject mocked TokenProvider
	oauth2clientcredentialsMiddleware := NewOAuth2ClientCredentialsMiddleware()
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
