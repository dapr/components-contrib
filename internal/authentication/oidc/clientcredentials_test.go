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

package oidc

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2"
	ccreds "golang.org/x/oauth2/clientcredentials"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/dapr/kit/logger"
)

func TestRun(t *testing.T) {
	var lock sync.Mutex
	clock := clocktesting.NewFakeClock(time.Now())
	called := make(chan struct{}, 1)
	var retErr error = nil

	fetchTokenFn := func(context.Context) (*oauth2.Token, error) {
		lock.Lock()
		defer lock.Unlock()
		called <- struct{}{}
		return &oauth2.Token{
			Expiry: clock.Now().Add(time.Minute),
		}, retErr
	}

	t.Run("should return when context is cancelled", func(t *testing.T) {
		c := &ClientCredentials{
			log:          logger.NewLogger("test"),
			clock:        clock,
			fetchTokenFn: fetchTokenFn,
			closeCh:      make(chan struct{}),
			currentToken: &oauth2.Token{
				Expiry: clock.Now(),
			},
		}

		ctx, cancel := context.WithCancel(context.Background())

		c.Run(ctx)
		cancel()

		select {
		case <-called:
			t.Fatal("should not have called fetchTokenFn")
		default:
		}
	})

	t.Run("should return when closed", func(t *testing.T) {
		c := &ClientCredentials{
			log:          logger.NewLogger("test"),
			clock:        clock,
			fetchTokenFn: fetchTokenFn,
			closeCh:      make(chan struct{}),
			currentToken: &oauth2.Token{
				Expiry: clock.Now(),
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c.Run(ctx)

		// Should be able to close multiple times.
		c.Close()
		c.Close()

		select {
		case <-called:
			t.Fatal("should not have called fetchTokenFn")
		case <-c.closeCh:
		case <-time.After(time.Second * 5):
			t.Fatal("should have closed run")
		}
	})

	t.Run("should renew token when ready for renewal", func(t *testing.T) {
		c := &ClientCredentials{
			log:          logger.NewLogger("test"),
			clock:        clock,
			fetchTokenFn: fetchTokenFn,
			closeCh:      make(chan struct{}),
			currentToken: &oauth2.Token{Expiry: clock.Now().Add(time.Minute * 2)},
		}

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		c.Run(ctx)

		assert.Eventually(t, clock.HasWaiters, time.Second*5, time.Millisecond*10)
		clock.Step(time.Minute * 1)

		select {
		case <-called:
		case <-time.After(time.Second * 5):
			t.Fatal("should have called")
		}
	})

	t.Run("should call renew again after 30 seconds when it fails", func(t *testing.T) {
		c := &ClientCredentials{
			log:          logger.NewLogger("test"),
			clock:        clock,
			fetchTokenFn: fetchTokenFn,
			closeCh:      make(chan struct{}),
			currentToken: &oauth2.Token{
				Expiry: clock.Now(),
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c.Run(ctx)

		assert.Eventually(t, clock.HasWaiters, time.Second*5, time.Millisecond*10)
		clock.Step(time.Minute * 1)

		select {
		case <-called:
		case <-time.After(time.Second * 5):
			t.Fatal("should have called")
		}

		lock.Lock()
		retErr = errors.New("test error")
		lock.Unlock()

		assert.Eventually(t, clock.HasWaiters, time.Second*5, time.Millisecond*10)
		clock.Step(time.Minute * 1)

		select {
		case <-called:
		case <-time.After(time.Second * 5):
			t.Fatal("should have called")
		}

		assert.Eventually(t, clock.HasWaiters, time.Second*5, time.Millisecond*10)
		clock.Step(time.Second * 30)

		select {
		case <-called:
		case <-time.After(time.Second * 5):
			t.Fatal("should have called")
		}

		c.Close()

		select {
		case <-c.closeCh:
		case <-time.After(time.Second * 5):
			t.Fatal("should have closed run")
		}
	})
}

func Test_tokenRenewDuration(t *testing.T) {
	c := &ClientCredentials{
		clock: clock.RealClock{},
		currentToken: &oauth2.Token{
			Expiry: time.Now(),
		},
	}
	assert.InDelta(t, c.tokenRenewDuration(), time.Duration(0), float64(time.Second*5))

	c = &ClientCredentials{
		clock: clock.RealClock{},
		currentToken: &oauth2.Token{
			Expiry: time.Now().Add(time.Hour),
		},
	}
	assert.InDelta(t, c.tokenRenewDuration(), time.Minute*30, float64(time.Second*5))
}

func Test_toConfig(t *testing.T) {
	tests := map[string]struct {
		opts      ClientCredentialsOptions
		expConfig *ccreds.Config
		expErr    bool
	}{
		"openid not in scopes should error": {
			opts: ClientCredentialsOptions{
				TokenURL:     "https://localhost:8080",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				Scopes:       []string{"profile"},
				Audiences:    []string{"audience"},
			},
			expErr: true,
		},
		"non-https endpoint should error": {
			opts: ClientCredentialsOptions{
				TokenURL:     "http://localhost:8080",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				Audiences:    []string{"audience"},
			},
			expErr: true,
		},
		"bad CA certificate should error": {
			opts: ClientCredentialsOptions{
				TokenURL:     "https://localhost:8080",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				Audiences:    []string{"audience"},
				CAPEM:        []byte("ca-pem"),
			},
			expErr: true,
		},
		"no audiences should error": {
			opts: ClientCredentialsOptions{
				TokenURL:     "https://localhost:8080",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
			},
			expErr: true,
		},
		"should default scope": {
			opts: ClientCredentialsOptions{
				TokenURL:     "https://localhost:8080",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				Audiences:    []string{"audience"},
			},
			expConfig: &ccreds.Config{
				ClientID:       "client-id",
				ClientSecret:   "client-secret",
				TokenURL:       "https://localhost:8080",
				Scopes:         []string{"openid"},
				EndpointParams: url.Values{"audience": []string{"audience"}},
			},
			expErr: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			config, _, err := toConfig(test.opts)
			assert.Equalf(t, test.expErr, err != nil, "%v", err)
			assert.Equal(t, test.expConfig, config)
		})
	}
}
