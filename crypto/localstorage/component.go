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

package localstorage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"

	contribCrypto "github.com/dapr/components-contrib/crypto"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	internals "github.com/dapr/kit/crypto"
	"github.com/dapr/kit/logger"
)

type localStorageCrypto struct {
	contribCrypto.LocalCryptoBaseComponent

	md     localStorageMetadata
	logger logger.Logger
}

// NewLocalStorageCrypto returns a new local storage crypto provider.
// Keys are loaded from PEM or JSON (each containing an individual JWK) files from a local folder on disk.
func NewLocalStorageCrypto(logger logger.Logger) contribCrypto.SubtleCrypto {
	k := &localStorageCrypto{
		logger: logger,
	}
	k.RetrieveKeyFn = k.retrieveKey
	return k
}

// Init the crypto provider.
func (l *localStorageCrypto) Init(_ context.Context, metadata contribCrypto.Metadata) error {
	// Parse the metadata
	err := l.md.InitWithMetadata(metadata)
	if err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	return nil
}

// Features returns the features available in this crypto provider.
func (l *localStorageCrypto) Features() []contribCrypto.Feature {
	return []contribCrypto.Feature{} // No Feature supported.
}

func (l *localStorageCrypto) Close() error {
	return nil
}

// Retrieves a key (public or private or symmetric) from a local file.
// Parameter "key" must be the name of a file inside the "path"
func (l *localStorageCrypto) retrieveKey(parentCtx context.Context, key string) (jwk.Key, error) {
	// Do not allow escaping the root path by including ".." in the key's name
	if strings.Contains(key, "..") {
		return nil, errors.New("invalid key path: cannot contain '..'")
	}

	// Load the file
	path := filepath.Join(l.md.Path, key)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load key '%s': %w", key, err)
	}

	// Check if we can determine the file type from the extension
	var contentType string
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json": // Assume a JWK
		contentType = "application/json"
	case ".pem": // Assume a PEM file
		contentType = "application/x-pem-file"
	}

	// Parse the key
	jwkObj, err := internals.ParseKey(data, contentType)
	if err == nil {
		switch jwkObj.KeyType() {
		case jwa.EC, jwa.RSA, jwa.OKP, jwa.OctetSeq:
			// Nop
		default:
			err = errors.New("invalid key type")
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to parse key from secret: %w", err)
	}

	return jwkObj, nil
}

func (localStorageCrypto) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := localStorageMetadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.CryptoType)
	return
}
