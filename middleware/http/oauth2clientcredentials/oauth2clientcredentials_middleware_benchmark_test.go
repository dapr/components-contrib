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

package oauth2clientcredentials

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"

	"github.com/dapr/components-contrib/middleware"
	mock "github.com/dapr/components-contrib/middleware/http/oauth2clientcredentials/mocks"
	"github.com/dapr/kit/logger"
)

func BenchmarkTestOAuth2ClientCredentialsGetHandler(b *testing.B) {
	mockCtrl := gomock.NewController(b)
	defer mockCtrl.Finish()
	mockTokenProvider := mock.NewMockTokenProviderInterface(mockCtrl)
	gomock.InOrder(
		mockTokenProvider.
			EXPECT().
			GetToken(gomock.Any()).
			Return(&oauth2.Token{
				AccessToken: "abcd",
				TokenType:   "Bearer",
				Expiry:      time.Now().Add(1 * time.Minute),
			}, nil).
			Times(1),
	)

	var metadata middleware.Metadata
	metadata.Properties = map[string]string{
		"clientID":     "testId",
		"clientSecret": "testSecret",
		"scopes":       "ascope",
		"tokenURL":     "https://localhost:9999",
		"headerName":   "authorization",
		"authStyle":    "1",
	}

	log := logger.NewLogger("oauth2clientcredentials.test")
	oauth2clientcredentialsMiddleware, ok := NewOAuth2ClientCredentialsMiddleware(log).(*Middleware)
	require.True(b, ok)
	oauth2clientcredentialsMiddleware.SetTokenProvider(mockTokenProvider)
	handler, err := oauth2clientcredentialsMiddleware.GetHandler(b.Context(), metadata)
	require.NoError(b, err)

	for i := range b.N {
		url := fmt.Sprintf("http://dapr.io/api/v1/users/%d", i)
		r := httptest.NewRequest(http.MethodGet, url, nil)
		w := httptest.NewRecorder()
		handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w, r)
	}
}

func BenchmarkTestOAuth2ClientCredentialsGetHandlerWithPathFilter(b *testing.B) {
	mockCtrl := gomock.NewController(b)
	defer mockCtrl.Finish()
	mockTokenProvider := mock.NewMockTokenProviderInterface(mockCtrl)
	gomock.InOrder(
		mockTokenProvider.
			EXPECT().
			GetToken(gomock.Any()).
			Return(&oauth2.Token{
				AccessToken: "abcd",
				TokenType:   "Bearer",
				Expiry:      time.Now().Add(1 * time.Minute),
			}, nil).
			Times(1),
	)

	var metadata middleware.Metadata
	metadata.Properties = map[string]string{
		"clientID":     "testId",
		"clientSecret": "testSecret",
		"scopes":       "ascope",
		"tokenURL":     "https://localhost:9999",
		"headerName":   "authorization",
		"authStyle":    "1",
		"pathFilter":   "/api/v1/users/.*",
	}

	log := logger.NewLogger("oauth2clientcredentials.test")
	oauth2clientcredentialsMiddleware, ok := NewOAuth2ClientCredentialsMiddleware(log).(*Middleware)
	require.True(b, ok)
	oauth2clientcredentialsMiddleware.SetTokenProvider(mockTokenProvider)
	handler, err := oauth2clientcredentialsMiddleware.GetHandler(b.Context(), metadata)
	require.NoError(b, err)

	for i := range b.N {
		url := fmt.Sprintf("http://dapr.io/api/v1/users/%d", i)
		r := httptest.NewRequest(http.MethodGet, url, nil)
		w := httptest.NewRecorder()
		handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w, r)
	}
}
