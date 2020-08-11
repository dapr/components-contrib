// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package oauth2clientcredentials

import (
	"testing"
	"time"

	"github.com/dapr/components-contrib/middleware"
	mock "github.com/dapr/components-contrib/middleware/http/oauth2clientcredentials/mocks"
	"github.com/golang/mock/gomock"
	oauth2 "golang.org/x/oauth2"

	fh "github.com/valyala/fasthttp"
)

var mockedRequestContext fh.RequestCtx

func mockedRequestHandler(ctx *fh.RequestCtx) {
	// mockedRequestContext = *ctx
}

func TestOAuth2Token(t *testing.T) {
	// Setup
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	// Mock mockTokenProvider
	mockTokenProvider := mock.NewMockTokenProviderInterface(mockCtrl)

	mockTokenProvider.
		EXPECT().
		GetToken(gomock.Any()).
		Return(&oauth2.Token{
			AccessToken:  "abc",
			TokenType:    "Bearer",
			RefreshToken: "refresh",
			Expiry:       time.Now().In(time.UTC).Add(5 * time.Second),
		}, nil).
		Times(1)

	// Run Test
	var metadata middleware.Metadata
	metadata.Properties = map[string]string{
		"clientID":     "127.0.0.1",
		"clientSecret": "30003",
		"tokenURL":     "https://localhost:9999",
		"authStyle":    "1",
	}

	// get Handler with metadata
	t.Logf("%s", &mockedRequestContext.Request.Header)

	handler, _ := NewOAuth2ClientCredentialsMiddleware().GetHandler(metadata)

	// Invoke
	handler(mockedRequestHandler)(&mockedRequestContext)

	t.Logf("%s", &mockedRequestContext.Request.Header)

}
