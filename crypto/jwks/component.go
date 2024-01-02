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
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/lestrrat-go/jwx/v2/jwk"

	contribCrypto "github.com/dapr/components-contrib/crypto"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/jwkscache"
	"github.com/dapr/kit/logger"
)

type jwksCrypto struct {
	contribCrypto.LocalCryptoBaseComponent

	md      jwksMetadata
	cache   *jwkscache.JWKSCache
	logger  logger.Logger
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

	// Init the JWKS cache
	k.cache = jwkscache.NewJWKSCache(k.md.JWKS, k.logger)
	k.cache.SetMinRefreshInterval(k.md.MinRefreshInterval)
	k.cache.SetRequestTimeout(k.md.RequestTimeout)

	// Start the JWKS cache in background
	startErrCh := make(chan error)
	go func() {
		startErrCh <- k.cache.Start(k.getContext())
	}()

	// Wait for the cache to be ready
	// Here we use the init context
	err = k.cache.WaitForCacheReady(ctx)
	if err != nil {
		// If we have an initialization error, return
		return err
	}

	return nil
}

// Returns a context that is canceled when the component is closed.
func (k *jwksCrypto) getContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	k.wg.Add(1)
	go func() {
		defer k.wg.Done()
		defer cancel()
		<-k.closeCh
	}()
	return ctx
}

// Close implements the io.Closer interface to close the component
func (k *jwksCrypto) Close() error {
	if k.closed.CompareAndSwap(false, true) {
		close(k.closeCh)
	}

	k.wg.Wait()
	return nil
}

// Features returns the features available in this crypto provider.
func (k *jwksCrypto) Features() []contribCrypto.Feature {
	return []contribCrypto.Feature{} // No Feature supported.
}

// Retrieves a key (public or private or symmetric) from the JWKS
func (k *jwksCrypto) retrieveKeyFromSecretFn(parentCtx context.Context, kid string) (jwk.Key, error) {
	jwks := k.cache.KeySet()
	if jwks == nil {
		return nil, errors.New("no JWKS loaded")
	}

	key, found := jwks.LookupKeyID(kid)
	if !found {
		return nil, contribCrypto.ErrKeyNotFound
	}
	return key, nil
}

func (k *jwksCrypto) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := jwksMetadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.CryptoType)
	return
}
