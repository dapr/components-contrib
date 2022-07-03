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

package keyring

import (
	"errors"
	"fmt"

	"github.com/99designs/keyring"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/config"
	"github.com/dapr/kit/logger"
)

// BackendType is alias of keyring.BackendType.
type BackendType = keyring.BackendType

// All currently supported secure storage backends.
const (
	SecretServiceBackend BackendType = keyring.SecretServiceBackend // secret-service
	KeychainBackend      BackendType = keyring.KeychainBackend      // keychain
	KeyCtlBackend        BackendType = keyring.KeyCtlBackend        // keyctl
	KWalletBackend       BackendType = keyring.KWalletBackend       // kwallet
	WinCredBackend       BackendType = keyring.WinCredBackend       // wincred
	FileBackend          BackendType = keyring.FileBackend          // file
	PassBackend          BackendType = keyring.PassBackend          // pass
)

// availableBackends is a map of backends that keyring secret store are supported.
var availableBackends = map[BackendType]bool{
	SecretServiceBackend: true,
	KeychainBackend:      true,
	KeyCtlBackend:        true,
	KWalletBackend:       true,
	WinCredBackend:       true,
	FileBackend:          true,
	PassBackend:          true,
}

type keyringSecretStoreMetaData struct {
	// BackendType is the type of the keyring secret store backend to use.
	BackendType BackendType

	// ServiceName is a generic service name that is used by backends that support the concept.
	ServiceName string

	// MacOSKeychainNameKeychainName is the name of the macOS keychain that is used.
	KeychainName string
	// KeychainTrustApplication is whether the calling application should be trusted by default by items.
	KeychainTrustApplication bool
	// KeychainSynchronizable is whether the item can be synchronized to iCloud.
	KeychainSynchronizable bool
	// KeychainAccessibleWhenUnlocked is whether the item is accessible when the device is locked.
	KeychainAccessibleWhenUnlocked bool

	// FileDir is the directory that keyring files are stored in, ~/ is resolved to the users' home dir.
	FileDir string
	// FileNeedPassword is whether the keyring file needs a password to be opened.
	FileNeedPassword bool

	// KeyCtlScope is the scope of the kernel keyring (either "user", "session", "process" or "thread").
	KeyCtlScope string

	// KeyCtlPerm is the permission mask to use for new keys.
	KeyCtlPerm uint32

	// KWalletAppID is the application id for KWallet.
	KWalletAppID string
	// KWalletFolder is the folder for KWallet.
	KWalletFolder string

	// LibSecretCollectionName is the name collection in secret-service.
	LibSecretCollectionName string

	// PassDir is the pass password-store directory, ~/ is resolved to the users' home dir.
	PassDir string
	// PassCmd is the name of the pass executable.
	PassCmd string
	// PassPrefix is a string prefix to prepend to the item path stored in pass.
	PassPrefix string

	// WinCredPrefix is a string prefix to prepend to the key name.
	WinCredPrefix string
}

type keyringSecretStore struct {
	ring   keyring.Keyring
	logger logger.Logger
}

func NewKeyringSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &keyringSecretStore{
		logger: logger,
	}
}

// Init authenticates with the actual secret store and performs other init operation
func (s *keyringSecretStore) Init(metadata secretstores.Metadata) error {
	meta, err := s.getKeyringSecretStoreMetadata(metadata)
	if err != nil {
		return err
	}

	config := keyring.Config{
		AllowedBackends:                []BackendType{meta.BackendType},
		ServiceName:                    meta.ServiceName,
		KeychainName:                   meta.KeychainName,
		KeychainTrustApplication:       meta.KeychainTrustApplication,
		KeychainAccessibleWhenUnlocked: meta.KeychainAccessibleWhenUnlocked,
		FileDir:                        meta.FileDir,
		FilePasswordFunc:               func(s string) (string, error) { return "", nil },
		KeyCtlScope:                    meta.KeyCtlScope,
		KeyCtlPerm:                     meta.KeyCtlPerm,
		KWalletAppID:                   meta.KWalletAppID,
		KWalletFolder:                  meta.KWalletFolder,
		LibSecretCollectionName:        meta.LibSecretCollectionName,
		PassDir:                        meta.PassDir,
		PassCmd:                        meta.PassCmd,
		PassPrefix:                     meta.PassPrefix,
		WinCredPrefix:                  meta.WinCredPrefix,
	}

	if meta.BackendType == FileBackend && meta.FileNeedPassword {
		config.FilePasswordFunc = keyring.TerminalPrompt
	}

	s.ring, err = keyring.Open(config)
	if err != nil && !errors.Is(err, keyring.ErrNoAvailImpl) {
		return err
	}
	if errors.Is(err, keyring.ErrNoAvailImpl) {
		return fmt.Errorf("current environment not support %s", meta.BackendType)
	}

	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (s *keyringSecretStore) GetSecret(req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	item, err := s.ring.Get(req.Name)
	if err != nil {
		return secretstores.GetSecretResponse{}, err
	}
	return secretstores.GetSecretResponse{
		Data: map[string]string{
			req.Name: string(item.Data),
		},
	}, nil
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
func (s *keyringSecretStore) BulkGetSecret(req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	keys, err := s.ring.Keys()
	if err != nil {
		return secretstores.BulkGetSecretResponse{}, err
	}

	data := make(map[string]map[string]string, len(keys))
	for _, key := range keys {
		item, err := s.ring.Get(key)
		if err != nil {
			return secretstores.BulkGetSecretResponse{}, err
		}

		data[key] = map[string]string{key: string(item.Data)}
	}

	return secretstores.BulkGetSecretResponse{Data: data}, nil
}

func (s *keyringSecretStore) getKeyringSecretStoreMetadata(spec secretstores.Metadata) (*keyringSecretStoreMetaData, error) {
	var meta keyringSecretStoreMetaData
	err := config.Decode(spec.Properties, &meta)
	if err != nil {
		return nil, err
	}

	if !availableBackends[meta.BackendType] {
		return nil, fmt.Errorf("invalid keyring backend type %s", meta.BackendType)
	}

	// if backendType is keychain and serviceName is empty, keyring.Keys has bug.
	if meta.BackendType == KeychainBackend && meta.ServiceName == "" {
		return nil, errors.New("metadata serviceName is required for keychain backend")
	}

	// if backendType is file and fileDir is empty, just keyring.Keys will returns
	// error, No directory provided for file keyring.
	if meta.BackendType == FileBackend && meta.FileDir == "" {
		return nil, errors.New("metadata fileDir is required for file backend")
	}

	return &meta, nil
}
