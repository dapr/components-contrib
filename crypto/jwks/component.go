/*
Copyright 2023 The Dapr Authors
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
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/lestrrat-go/httprc"
	"github.com/lestrrat-go/jwx/v2/jwk"

	contribCrypto "github.com/dapr/components-contrib/crypto"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/fswatcher"
	"github.com/dapr/kit/logger"
)

type jwksCrypto struct {
	contribCrypto.LocalCryptoBaseComponent

	md       jwksMetadata
	jwks     jwk.Set
	jwksLock sync.Mutex

	logger logger.Logger

	closed  atomic.Bool
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// NewJWKSCrypto returns a new crypto provider based a JWKS, either passed as metadata, or read from a file or HTTP(S) URL.
// The key argument in methods is the ID of the key in the JWKS ("kid" property).
func NewJWKSCrypto(logger logger.Logger) contribCrypto.SubtleCrypto {
	k := &jwksCrypto{
		logger:  logger,
		closeCh: make(chan struct{}),
	}
	k.RetrieveKeyFn = k.retrieveKeyFromSecretFn
	return k
}

// Init the crypto provider.
func (k *jwksCrypto) Init(ctx context.Context, metadata contribCrypto.Metadata) error {
	if len(metadata.Properties) == 0 {
		return errors.New("empty metadata properties")
	}

	// Parse the metadata
	err := k.md.InitWithMetadata(metadata)
	if err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	// Load the JWKS
	err = k.initJWKS()
	if err != nil {
		return err
	}

	return nil
}

// Close implements the io.Closer interface to close the component
func (k *jwksCrypto) Close() error {
	if k.closed.CompareAndSwap(false, true) {
		close(k.closeCh)
	}

	k.jwksLock.Lock()
	defer k.jwksLock.Unlock()
	err := k.jwks.Clear()
	k.wg.Wait()
	return err
}

// Features returns the features available in this crypto provider.
func (k *jwksCrypto) Features() []contribCrypto.Feature {
	return []contribCrypto.Feature{} // No Feature supported.
}

// Init the JWKS object from the metadata property
func (k *jwksCrypto) initJWKS() error {
	if len(k.md.JWKS) == 0 {
		return errors.New("metadata property 'jwks' is required")
	}

	// Use context based on close channel.
	ctx, cancel := context.WithCancel(context.Background())
	k.wg.Add(1)
	go func() {
		defer k.wg.Done()
		defer cancel()
		<-k.closeCh
	}()

	// If the value starts with "http://" or "https://", treat it as URL
	if strings.HasPrefix(k.md.JWKS, "http://") || strings.HasPrefix(k.md.JWKS, "https://") {
		return k.initJWKSFromURL(ctx, k.md.JWKS)
	}

	// Check if the value is a valid path to a local file
	stat, err := os.Stat(k.md.JWKS)
	if err == nil && stat != nil && !stat.IsDir() {
		return k.initJWKSFromFile(ctx, k.md.JWKS)
	}

	// Treat the value as the actual JWKS
	// First, check if it's base64-encoded (remove trailing padding chars if present first)
	mdJSON, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(k.md.JWKS, "="))
	if err != nil {
		// Assume it's already JSON, not encoded
		mdJSON = []byte(k.md.JWKS)
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
	cache := jwk.NewCache(ctx,
		jwk.WithErrSink(httprc.ErrSinkFunc(func(err error) {
			k.logger.Warnf("Error while refreshing JWKS cache: %v", err)
		})),
	)
	// We also need to create a custom HTTP client because otherwise there's no timeout.
	client := &http.Client{
		Timeout: k.md.RequestTimeout,
	}
	err := cache.Register(url,
		jwk.WithMinRefreshInterval(k.md.MinRefreshInterval),
		jwk.WithHTTPClient(client),
	)
	if err != nil {
		return fmt.Errorf("failed to register JWKS cache: %w", err)
	}

	// Fetch the JWKS right away to start, so we can check it's valid and populate the cache
	refreshCtx, refreshCancel := context.WithTimeout(ctx, k.md.RequestTimeout)
	_, err = cache.Refresh(refreshCtx, url)
	refreshCancel()
	if err != nil {
		return fmt.Errorf("failed to fetch JWKS: %w", err)
	}

	k.jwks = jwk.NewCachedSet(cache, url)
	return nil
}

func (k *jwksCrypto) initJWKSFromFile(parentCtx context.Context, file string) error {
	ctx, cancel := context.WithCancel(parentCtx)

	// Get the path to the folder containing the file
	path := filepath.Dir(file)

	// Start watching for changes in the filesystem
	eventCh := make(chan struct{})
	loadedCh := make(chan error, 1) // Needs to be buffered to prevent an (unlikely, but possible) goroutine leak

	k.wg.Add(2)

	go func() {
		defer k.wg.Done()
		watchErr := fswatcher.Watch(ctx, path, eventCh)
		if watchErr != nil && !errors.Is(watchErr, context.Canceled) {
			// Log errors only
			k.logger.Errorf("Error while watching for changes to the local JWKS file: %v", watchErr)
		}
	}()

	go func() {
		defer k.wg.Done()
		defer cancel()
		var firstDone bool
		for {
			select {
			case <-eventCh:
				// When there's a change, reload the JWKS file
				err := k.parseJWKSFile(file)
				if !firstDone {
					// The first time, signal that the initialization was complete and pass the error
					loadedCh <- err
					firstDone = true
				} else {
					// Log errors only
					k.logger.Errorf("Error reading JWKS from disk: %v", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Trigger a refresh immediately and wait for the first reload
	eventCh <- struct{}{}
	select {
	case err := <-loadedCh:
		// Error could be nil if everything is fine
		return err
	case <-ctx.Done():
		return fmt.Errorf("failed to initialize JWKS from file: %w", ctx.Err())
	}
}

// Used by initJWKSFromFile to parse a JWKS file every time it's changed
func (k *jwksCrypto) parseJWKSFile(file string) error {
	if k.closed.Load() {
		return nil
	}

	k.logger.Debugf("Reloading JWKS file from disk")

	read, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("failed to read JgoWKS file: %v", err)
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

// Retrieves a key (public or private or symmetric) from the JWKS
func (k *jwksCrypto) retrieveKeyFromSecretFn(parentCtx context.Context, kid string) (jwk.Key, error) {
	k.jwksLock.Lock()
	jwks := k.jwks
	k.jwksLock.Unlock()

	if jwks == nil {
		return nil, errors.New("no JWKS loaded")
	}

	key, found := jwks.LookupKeyID(kid)
	if !found {
		return nil, contribCrypto.ErrKeyNotFound
	}
	return key, nil
}

func (k *jwksCrypto) GetComponentMetadata() map[string]string {
	metadataStruct := jwksMetadata{}
	metadataInfo := map[string]string{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.CryptoType)
	return metadataInfo
}
