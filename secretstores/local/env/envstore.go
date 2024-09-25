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

//nolint:goconst
package env

import (
	"context"
	"os"
	"reflect"
	"runtime"
	"strings"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

var _ secretstores.SecretStore = (*envSecretStore)(nil)

type Metadata struct {
	// Prefix to add to the env vars when reading them.
	// This is case sensitive on Linux and macOS, and case-insensitive on Windows.
	Prefix string
}

type envSecretStore struct {
	logger   logger.Logger
	metadata Metadata
}

// NewEnvSecretStore returns a new env var secret store.
func NewEnvSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &envSecretStore{
		logger: logger,
	}
}

// Init creates a Local secret store.
func (s *envSecretStore) Init(_ context.Context, meta secretstores.Metadata) error {
	if err := kitmd.DecodeMetadata(meta.Properties, &s.metadata); err != nil {
		return err
	}
	return nil
}

// GetSecret retrieves a secret from env var using provided key.
func (s *envSecretStore) GetSecret(ctx context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	var value string
	name := s.metadata.Prefix + req.Name
	if s.isKeyAllowed(name) {
		value = os.Getenv(name)
	} else {
		s.logger.Warnf("Access to env var %s is forbidden", req.Name)
	}
	return secretstores.GetSecretResponse{
		Data: map[string]string{
			req.Name: value,
		},
	}, nil
}

// BulkGetSecret retrieves all secrets in the store and returns a map of string/string values.
func (s *envSecretStore) BulkGetSecret(ctx context.Context, req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	env := os.Environ()
	r := make(map[string]map[string]string, len(env))

	// To maintain compatibility with how operating systems work, on Windows we do the prefix matching in a case-insensitive way
	prefix := s.metadata.Prefix
	if runtime.GOOS == "windows" && len(prefix) > 0 {
		prefix = strings.ToUpper(prefix)
	}

	lp := len(s.metadata.Prefix)
	for _, element := range env {
		envVariable := strings.SplitN(element, "=", 2)
		key := envVariable[0]

		// Sorry for this complex if... Essentially we want to skip variables if the prefix is set AND either one of:
		// - The key is not longer than the prefix (bounds check to prevent a panic)
		// - The key doesn't contain the prefix; this is done in a case-insensitive way on Windows (see how we changed the prefix to uppercase above), and case-sensitive way on other OS's
		if prefix != "" &&
			(len(key) <= len(prefix) ||
				(runtime.GOOS == "windows" && strings.ToUpper(key[:len(prefix)]) != prefix) ||
				(runtime.GOOS != "windows" && key[:len(prefix)] != prefix)) {
			continue
		}

		// Skip disallowed keys
		if !s.isKeyAllowed(key) {
			continue
		}

		r[key[lp:]] = map[string]string{key[lp:]: envVariable[1]}
	}

	return secretstores.BulkGetSecretResponse{
		Data: r,
	}, nil
}

// Features returns the features available in this secret store.
func (s *envSecretStore) Features() []secretstores.Feature {
	return []secretstores.Feature{} // No Feature supported.
}

func (s *envSecretStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := Metadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.SecretStoreType)
	return
}

func (s *envSecretStore) isKeyAllowed(key string) bool {
	key = strings.ToUpper(key)
	switch {
	case key == "APP_API_TOKEN":
		return false
	case strings.HasPrefix(key, "DAPR_"):
		return false
	default:
		return true
	}
}

func (s *envSecretStore) Close() error {
	return nil
}
