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

package vault

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/vault/api"
	"golang.org/x/net/http2"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	defaultVaultAddress          string = "https://127.0.0.1:8200"
	defaultVaultEnginePath       string = "secret"
	componentVaultAddress        string = "vaultAddr"
	componentCaCert              string = "caCert"
	componentCaPath              string = "caPath"
	componentCaPem               string = "caPem"
	componentSkipVerify          string = "skipVerify"
	componentTLSServerName       string = "tlsServerName"
	componentVaultToken          string = "vaultToken"
	componentVaultTokenMountPath string = "vaultTokenMountPath"
	componentVaultKVPrefix       string = "vaultKVPrefix"
	componentVaultKVUsePrefix    string = "vaultKVUsePrefix"
	defaultVaultKVPrefix         string = "dapr"
	vaultEnginePath              string = "enginePath"
	vaultValueType               string = "vaultValueType"
	versionID                    string = "version_id"

	DataStr string = "data"

	authMethodToken      string = "token"
	authMethodKubernetes string = "kubernetes"

	vaultClientTimeout = 60 * time.Second
)

type valueType string

const (
	valueTypeMap  valueType = "map"
	valueTypeText valueType = "text"
)

var _ secretstores.SecretStore = (*vaultSecretStore)(nil)

func (v valueType) isMapType() bool {
	return v == valueTypeMap
}

var ErrNotFound = errors.New("secret key or version not exist")

// vaultSecretStore is a secret store implementation for HashiCorp Vault.
type vaultSecretStore struct {
	client          *api.Client
	vaultKVPrefix   string
	vaultEnginePath string
	vaultValueType  valueType

	logger logger.Logger

	// mu guards client and orders the renewal loop's wg.Go against Close.
	mu       sync.Mutex
	bgCtx    context.Context
	bgCancel context.CancelFunc
	wg       sync.WaitGroup
}

type VaultMetadata struct {
	CaCert        string
	CaPath        string
	CaPem         string
	SkipVerify    string
	TLSServerName string

	VaultAddr        string
	VaultKVPrefix    string
	VaultKVUsePrefix bool
	EnginePath       string
	VaultValueType   string

	VaultAuthMethod              string
	VaultToken                   string
	VaultTokenMountPath          string
	VaultKubernetesRole          string
	VaultKubernetesMountPath     string
	VaultServiceAccountTokenPath string
}

// NewHashiCorpVaultSecretStore returns a new HashiCorp Vault secret store.
func NewHashiCorpVaultSecretStore(logger logger.Logger) secretstores.SecretStore {
	return newVaultSecretStore(logger)
}

func newVaultSecretStore(logger logger.Logger) *vaultSecretStore {
	bgCtx, bgCancel := context.WithCancel(context.Background())
	return &vaultSecretStore{
		logger:   logger,
		bgCtx:    bgCtx,
		bgCancel: bgCancel,
	}
}

// Init creates a HashiCorp Vault client.
func (v *vaultSecretStore) Init(ctx context.Context, meta secretstores.Metadata) error {
	m := VaultMetadata{
		VaultKVUsePrefix: true,
		VaultAuthMethod:  authMethodToken,
	}
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return err
	}

	if m.VaultAddr == "" {
		m.VaultAddr = defaultVaultAddress
	}

	v.vaultEnginePath = defaultVaultEnginePath
	if m.EnginePath != "" {
		v.vaultEnginePath = m.EnginePath
	}

	v.vaultValueType = valueTypeMap
	if m.VaultValueType != "" {
		switch valueType(m.VaultValueType) {
		case valueTypeMap:
		case valueTypeText:
			v.vaultValueType = valueTypeText
		default:
			return fmt.Errorf("vault init error, invalid value type %s, accepted values are map or text", m.VaultValueType)
		}
	}

	vaultKVPrefix := m.VaultKVPrefix
	if !m.VaultKVUsePrefix {
		vaultKVPrefix = ""
	} else if vaultKVPrefix == "" {
		vaultKVPrefix = defaultVaultKVPrefix
	}
	v.vaultKVPrefix = vaultKVPrefix

	if m.SkipVerify == "true" {
		v.logger.Warnf("hashicorp vault: you are using 'skipVerify' to skip server config verification, which is unsafe")
	}

	config, err := newClientConfig(&m)
	if err != nil {
		return err
	}

	// Known limitation: fails on an unparsable VAULT_* variable even though the
	// values are ignored, since NewClient always runs api.DefaultConfig().
	client, err := api.NewClient(config)
	if err != nil {
		return fmt.Errorf("couldn't create vault client: %w", err)
	}
	// NewClient applies VAULT_TOKEN, VAULT_NAMESPACE and VAULT_HEADERS.
	client.ClearToken()
	client.ClearNamespace()
	client.SetHeaders(http.Header{api.RequestHeaderName: []string{"true"}})

	switch m.VaultAuthMethod {
	case "", authMethodToken:
		token, err := readVaultToken(&m)
		if err != nil {
			return err
		}
		client.SetToken(token)
	case authMethodKubernetes:
		if m.VaultKubernetesRole == "" {
			return errors.New("vaultKubernetesRole is required when vaultAuthMethod is kubernetes")
		}
		if m.VaultToken != "" || m.VaultTokenMountPath != "" {
			return errors.New("vaultToken and vaultTokenMountPath must not be set when vaultAuthMethod is kubernetes")
		}
		if err := v.initKubernetesAuth(ctx, client, &m); err != nil {
			return err
		}
	default:
		return fmt.Errorf("vault init error, invalid auth method %s, accepted values are token or kubernetes", m.VaultAuthMethod)
	}

	v.mu.Lock()
	v.client = client
	v.mu.Unlock()

	return nil
}

// newClientConfig builds the config from metadata only. api.DefaultConfig()
// would also apply VAULT_* and HTTP(S)_PROXY environment variables.
func newClientConfig(m *VaultMetadata) (*api.Config, error) {
	transport := &http.Transport{
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}
	if err := http2.ConfigureTransport(transport); err != nil {
		return nil, fmt.Errorf("couldn't configure http2: %w", err)
	}

	config := &api.Config{
		Address: m.VaultAddr,
		HttpClient: &http.Client{
			Transport: transport,
			// The SDK follows redirects itself.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		Timeout:    vaultClientTimeout,
		MaxRetries: 0, // backward compatibility: the store never retried before it used the SDK
	}

	if err := config.ConfigureTLS(metadataToTLSConfig(m)); err != nil {
		return nil, fmt.Errorf("couldn't configure tls: %w", err)
	}

	return config, nil
}

func (v *vaultSecretStore) getClient() *api.Client {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.client
}

func metadataToTLSConfig(meta *VaultMetadata) *api.TLSConfig {
	tlsConf := &api.TLSConfig{
		Insecure:      meta.SkipVerify == "true",
		TLSServerName: meta.TLSServerName,
	}
	// ConfigureTLS loads the CA before applying Insecure, so a stale CA setting
	// would fail Init even though skipVerify never uses it.
	if tlsConf.Insecure {
		return tlsConf
	}

	// Set only one CA field: go-rootcerts applies CACert > CACertBytes > CAPath,
	// the reverse of the documented caPem > caPath > caCert.
	switch {
	case meta.CaPem != "":
		tlsConf.CACertBytes = []byte(meta.CaPem)
	case meta.CaPath != "":
		tlsConf.CAPath = meta.CaPath
	case meta.CaCert != "":
		tlsConf.CACert = meta.CaCert
	}

	return tlsConf
}

// getSecret retrieves a secret using a key and returns a map of decrypted string/string values.
// KVv2.Get isn't used because it requires "data" to be an object, which
// breaks text mode.
func (v *vaultSecretStore) getSecret(ctx context.Context, secret, version string) (map[string]string, error) {
	path := v.vaultEnginePath + "/data/"
	if v.vaultKVPrefix != "" {
		path += v.vaultKVPrefix + "/"
	}
	path += secret

	client := v.getClient()
	if client == nil {
		return nil, errors.New("hashicorp vault: component not initialized")
	}

	resp, err := client.Logical().ReadWithDataWithContext(ctx, path, map[string][]string{"version": {version}})
	if err != nil {
		v.logger.Debugf("hashicorp vault: get secret %s failed: %v", secret, err)
		return nil, fmt.Errorf("couldn't get secret: %w", err)
	}
	if resp == nil || resp.Data == nil {
		return nil, fmt.Errorf("getSecret %s failed %w", secret, ErrNotFound)
	}

	// Soft-deleted and destroyed versions come back with "data": null.
	dataRaw, ok := resp.Data[DataStr]
	if !ok || dataRaw == nil {
		return nil, fmt.Errorf("getSecret %s failed %w", secret, ErrNotFound)
	}

	if v.vaultValueType.isMapType() {
		dataMap, ok := dataRaw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("unexpected type for secret data at %s", secret)
		}
		data := make(map[string]string, len(dataMap))
		for k, val := range dataMap {
			if val == nil {
				// null is read as "" for compatibility.
				data[k] = ""
				continue
			}
			s, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("value for key %s in secret %s is not a string", k, secret)
			}
			data[k] = s
		}
		return data, nil
	}

	// Strings as-is, anything else as JSON.
	switch d := dataRaw.(type) {
	case string:
		return map[string]string{secret: d}, nil
	default:
		b, err := json.Marshal(d)
		if err != nil {
			return nil, fmt.Errorf("couldn't encode secret %s as text: %w", secret, err)
		}
		return map[string]string{secret: string(b)}, nil
	}
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (v *vaultSecretStore) GetSecret(ctx context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	// version 0 represent for latest version
	version := "0"
	if value, ok := req.Metadata[versionID]; ok {
		version = value
	}
	data, err := v.getSecret(ctx, req.Name, version)
	if err != nil {
		return secretstores.GetSecretResponse{Data: nil}, err
	}

	return secretstores.GetSecretResponse{Data: data}, nil
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
func (v *vaultSecretStore) BulkGetSecret(ctx context.Context, req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	version := "0"
	if value, ok := req.Metadata[versionID]; ok {
		version = value
	}

	resp := secretstores.BulkGetSecretResponse{
		Data: map[string]map[string]string{},
	}

	keys, err := v.listKeysUnderPath(ctx, "")
	if err != nil {
		return secretstores.BulkGetSecretResponse{}, err
	}

	for _, key := range keys {
		secretData, err := v.getSecret(ctx, key, version)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				// version not exist skip
				continue
			}

			return secretstores.BulkGetSecretResponse{Data: nil}, err
		}
		resp.Data[key] = secretData
	}

	return resp, nil
}

// listKeysUnderPath get all the keys recursively under a given path.(returned keys including path as prefix)
// path should not has `/` prefix.
func (v *vaultSecretStore) listKeysUnderPath(ctx context.Context, path string) ([]string, error) {
	listPath := v.vaultEnginePath + "/metadata/"
	if v.vaultKVPrefix != "" {
		listPath += v.vaultKVPrefix + "/"
	}
	listPath += path

	client := v.getClient()
	if client == nil {
		return nil, errors.New("hashicorp vault: component not initialized")
	}

	secret, err := client.Logical().ListWithContext(ctx, listPath)
	if err != nil {
		v.logger.Debugf("hashicorp vault: list keys at %s failed: %v", listPath, err)
		return nil, fmt.Errorf("couldn't list keys: %w", err)
	}
	if secret == nil || secret.Data == nil {
		return nil, fmt.Errorf("list keys couldn't get successful response at %s", listPath)
	}

	// A missing or null "keys" field means an empty directory.
	keysField, hasKeys := secret.Data["keys"]
	if !hasKeys || keysField == nil {
		return []string{}, nil
	}
	keysRaw, ok := keysField.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected list response shape at %s", listPath)
	}

	res := make([]string, 0, len(keysRaw))
	for _, kr := range keysRaw {
		key, ok := kr.(string)
		if !ok {
			continue
		}
		if v.isSecretPath(key) {
			res = append(res, path+key)
		} else {
			subKeys, err := v.listKeysUnderPath(ctx, path+key)
			if err != nil {
				return nil, err
			}
			res = append(res, subKeys...)
		}
	}

	return res, nil
}

// isSecretPath checks if the key is a valid secret path or it is part of the secret path.
func (v *vaultSecretStore) isSecretPath(key string) bool {
	return !strings.HasSuffix(key, "/")
}

// readVaultToken returns vaultToken, or the token read from vaultTokenMountPath.
func readVaultToken(m *VaultMetadata) (string, error) {
	// Test that at least one of them are set if not return error
	if m.VaultToken == "" && m.VaultTokenMountPath == "" {
		return "", errors.New("token mount path and token not set")
	}

	// Test that both are not set. If so return error
	if m.VaultToken != "" && m.VaultTokenMountPath != "" {
		return "", errors.New("token mount path and token both set")
	}

	if m.VaultToken != "" {
		return m.VaultToken, nil
	}

	data, err := os.ReadFile(m.VaultTokenMountPath)
	if err != nil {
		return "", fmt.Errorf("couldn't read vault token from mount path %s err: %s", m.VaultTokenMountPath, err)
	}

	return string(bytes.TrimSpace(data)), nil
}

// Features returns the features available in this secret store.
func (v *vaultSecretStore) Features() []secretstores.Feature {
	if v.vaultValueType == valueTypeText {
		return []secretstores.Feature{}
	}

	return []secretstores.Feature{secretstores.FeatureMultipleKeyValuesPerSecret}
}

func (v *vaultSecretStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := VaultMetadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.SecretStoreType)
	return
}

func (v *vaultSecretStore) Close() error {
	v.mu.Lock()
	v.bgCancel()
	v.mu.Unlock()

	v.wg.Wait()
	return nil
}
