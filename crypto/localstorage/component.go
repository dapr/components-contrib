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

package localstorage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"

	daprcrypto "github.com/dapr/components-contrib/crypto"
	internals "github.com/dapr/components-contrib/internal/crypto"
	"github.com/dapr/kit/logger"
)

const metadataKeyPath = "path"

type localStorageCrypto struct {
	daprcrypto.LocalCryptoBaseComponent

	path   string
	logger logger.Logger
}

// NewLocalStorageCrypto returns a new local storage crypto provider.
// Keys are loaded from PEM or JSON (each containing an individual JWK) files from a local folder on disk.
func NewLocalStorageCrypto(logger logger.Logger) daprcrypto.SubtleCrypto {
	k := &localStorageCrypto{
		logger: logger,
	}
	k.RetrieveKeyFn = k.retrieveKey
	return k
}

// Init the crypto provider.
func (k *localStorageCrypto) Init(metadata daprcrypto.Metadata) error {
	err := k.parseMetadata(metadata.Properties)
	if err != nil {
		return err
	}

	return nil
}

func (k *localStorageCrypto) parseMetadata(md map[string]string) error {
	if md == nil {
		return errors.New("metadata map is nil")
	}

	// Ensure that the "path" metadata property is defined and it indicates an actual folder
	k.path = md[metadataKeyPath]
	if k.path == "" {
		return fmt.Errorf("metadata property %s is required", metadataKeyPath)
	}
	k.path = filepath.Clean(k.path)
	info, err := os.Stat(k.path)
	if err != nil {
		return fmt.Errorf("could not stat path '%s': %w", k.path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("path '%s' is not a directory", k.path)
	}

	return nil
}

// Features returns the features available in this crypto provider.
func (k *localStorageCrypto) Features() []daprcrypto.Feature {
	return []daprcrypto.Feature{} // No Feature supported.
}

// Retrieves a key (public or private or symmetric) from a local file.
// Parameter "key" must be the name of a file inside the "path"
func (k *localStorageCrypto) retrieveKey(parentCtx context.Context, key string) (jwk.Key, error) {
	// Load the file
	path := filepath.Join(k.path, key)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load key '%s': %w", key, err)
	}

	// Check if we can determine the file type heuristically from the extension
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
