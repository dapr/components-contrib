/*
Copyright 2022 The Dapr Authors
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

package jwks

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lestrrat-go/httprc"
	"github.com/lestrrat-go/jwx/v2/jwk"

	daprcrypto "github.com/dapr/components-contrib/crypto"
	"github.com/dapr/kit/fswatcher"
	"github.com/dapr/kit/logger"
)

const (
	defaultRequestTimeout            = 30 * time.Second
	metadataKeyJWKS                  = "jwks"
	metadataKeyRequestTimeoutSeconds = "requestTimeoutSeconds"

	// Minimum interval before refreshing the JWKS cache
	minRefreshInterval = 10 * time.Minute
)

type jwksCrypto struct {
	daprcrypto.LocalCryptoBaseComponent

	requestTimeout time.Duration

	jwks     jwk.Set
	jwksLock sync.Mutex

	logger logger.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

// NewJWKSCrypto returns a new crypto provider based a JWKS, either passed as metadata, or read from a file or HTTP(S) URL.
// The key argument in methods is the ID of the key in the JWKS ("kid" property).
func NewJWKSCrypto(logger logger.Logger) daprcrypto.SubtleCrypto {
	ctx, cancel := context.WithCancel(context.Background())
	k := &jwksCrypto{
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}
	k.RetrieveKeyFn = k.retrieveKeyFromSecretFn
	return k
}

// Init the crypto provider.
func (k *jwksCrypto) Init(ctx context.Context, metadata daprcrypto.Metadata) error {
	if len(metadata.Properties) == 0 {
		return errors.New("empty metadata properties")
	}

	if metadata.Properties[metadataKeyRequestTimeoutSeconds] != "" {
		timeoutSec, _ := strconv.Atoi(metadata.Properties[metadataKeyRequestTimeoutSeconds])
		if timeoutSec > 0 {
			k.requestTimeout = time.Duration(timeoutSec) * time.Second
		}
	}
	if k.requestTimeout == 0 {
		k.requestTimeout = defaultRequestTimeout
	}

	err := k.initJWKS(ctx, metadata.Properties[metadataKeyJWKS])
	if err != nil {
		return err
	}

	return nil
}

// Close implements the io.Closer interface to close the component
func (k *jwksCrypto) Close() error {
	if k.cancel != nil {
		k.cancel()
	}
	return nil
}

// Features returns the features available in this crypto provider.
func (k *jwksCrypto) Features() []daprcrypto.Feature {
	return []daprcrypto.Feature{} // No Feature supported.
}

// Init the JWKS object from the metadata property
func (k *jwksCrypto) initJWKS(ctx context.Context, md string) error {
	if len(md) == 0 {
		return errors.New("metadata property 'jwks' is required")
	}

	// If the value starts with "http://" or "https://", treat it as URL
	if strings.HasPrefix(md, "http://") || strings.HasPrefix(md, "https://") {
		return k.initJWKSFromURL(ctx, md)
	}

	// Check if the value is a valid path to a local file
	stat, err := os.Stat(md)
	if err == nil && stat != nil && !stat.IsDir() {
		return k.initJWKSFromFile(ctx, md)
	}

	// Treat the value as the actual JWKS
	// First, check if it's base64-encoded
	mdJSON, err := base64.RawStdEncoding.DecodeString(md)
	if err != nil {
		// Assume it's already JSON, not encoded
		mdJSON = []byte(md)
	}

	// Try decoding from JSON
	k.jwks, err = jwk.Parse(mdJSON)
	if err != nil {
		return errors.New("failed to parse metadata property 'jwks': not a URL, path to local file, or JSON value (optionally base64-encoded)")
	}

	return nil
}

func (k *jwksCrypto) initJWKSFromURL(ctx context.Context, url string) error {
	// Create the JWKS cache
	// We are using k.ctx here because we want this to be tied to the component's lifecycle
	cache := jwk.NewCache(k.ctx,
		jwk.WithErrSink(httprc.ErrSinkFunc(func(err error) {
			k.logger.Warnf("Error while refreshing JWKS cache: %v", err)
		})),
	)
	err := cache.Register(url, jwk.WithMinRefreshInterval(minRefreshInterval))
	if err != nil {
		return fmt.Errorf("failed to register JWKS cache: %w", err)
	}

	// Fetch the JWKS right away to start, so we can check it's valid and populate the cache
	_, err = cache.Refresh(ctx, url)
	if err != nil {
		return fmt.Errorf("failed to fetch JWKS: %w", err)
	}

	k.jwks = jwk.NewCachedSet(cache, url)
	return nil
}

func (k *jwksCrypto) initJWKSFromFile(ctx context.Context, file string) error {
	// Get the path to the folder containing the file
	path := filepath.Dir(file)

	// Start watching for changes in the filesystem
	eventCh := make(chan struct{})
	loaded := make(chan error)
	go func() {
		watchErr := fswatcher.Watch(k.ctx, path, eventCh)
		if watchErr != nil && !errors.Is(watchErr, context.Canceled) {
			// Log errors only
			k.logger.Errorf("Error while watching for changes to the local JWKS file: %v", watchErr)
		}
	}()
	go func() {
		for {
			select {
			case <-eventCh:
				// When there's a change, reload the JWKS file
				err := k.parseJWKSFile(file)
				if loaded != nil {
					// The first time, signal that the initialization was complete and pass the error
					loaded <- err
					close(loaded)
					loaded = nil
				} else {
					// Log errors only
					k.logger.Errorf("Error reading JWKS from disk: %v", err)
				}
			case <-k.ctx.Done():
				return
			}
		}
	}()

	// Trigger a refresh immediately and wait for the first reload
	eventCh <- struct{}{}

	select {
	case err := <-loaded:
		// Error could be nil if everything is fine
		return err
	case <-ctx.Done():
		return fmt.Errorf("failed to initialize JWKS from file: %w", ctx.Err())
	}
}

// Used by initJWKSFromFile to parse a JWKS file every time it's changed
func (k *jwksCrypto) parseJWKSFile(file string) error {
	k.logger.Debugf("Reloading JWKS file from disk")

	read, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("failed to read JWKS file: %v", err)
	}

	jwks, err := jwk.Parse(read)
	if err != nil {
		return fmt.Errorf("failed to parse JWKS file: %v", err)
	}

	k.jwksLock.Lock()
	k.jwks = jwks
	k.jwksLock.Unlock()

	return nil
}

// Retrieves a key (public or private or symmetric) from a Kubernetes secret.
func (k *jwksCrypto) retrieveKeyFromSecretFn(parentCtx context.Context, kid string) (jwk.Key, error) {
	k.jwksLock.Lock()
	jwks := k.jwks
	k.jwksLock.Unlock()

	if jwks == nil {
		return nil, errors.New("no JWKS loaded")
	}

	key, found := jwks.LookupKeyID(kid)
	if !found {
		return nil, daprcrypto.ErrKeyNotFound
	}
	return key, nil
}
