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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"

	"github.com/hashicorp/vault/api"

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
	// client is written once, at the end of a successful Init(), and read
	// by every GetSecret/BulkGetSecret call. Both can happen concurrently
	// if a caller invokes those before Init() has returned (against the
	// component contract, but not otherwise prevented), so reads/writes go
	// through the mu-guarded getClient/Init assignment below rather than
	// accessing this field directly.
	client              *api.Client
	vaultAddress        string
	vaultToken          string
	vaultTokenMountPath string
	vaultKVPrefix       string
	vaultEnginePath     string
	vaultValueType      valueType

	logger logger.Logger

	// mu guards closed and the compound "check closed, then either start
	// the renewal goroutine or cancel bgCtx/closeCh" sequences in Init()
	// and Close() against each other. A plain atomic bool isn't enough
	// here: checking it and then acting on the result (e.g. starting the
	// renewal goroutine) has to be atomic with Close()'s own check-and-act,
	// otherwise Close() can observe "not started yet" and return while the
	// goroutine starts immediately after -- see initKubernetesAuth.
	mu       sync.Mutex
	closed   bool
	bgCtx    context.Context
	bgCancel context.CancelFunc
	closeCh  chan struct{}
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
	return &vaultSecretStore{
		logger: logger,
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

	// Get Vault address
	address := m.VaultAddr
	if address == "" {
		address = defaultVaultAddress
	}

	v.vaultAddress = address

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

	v.mu.Lock()
	v.bgCtx, v.bgCancel = context.WithCancel(context.Background())
	v.closeCh = make(chan struct{})
	if v.closed {
		// Close() already ran concurrently with this Init() call, before
		// these fields existed for it to cancel/close. Cancel them right
		// away, still under the same lock Close() uses, so any renewal
		// goroutine started below sees a closed component immediately
		// instead of running with a bgCtx/closeCh that no future Close()
		// call can ever cancel (closed is already latched true, so
		// Close()'s guard won't fire again).
		v.bgCancel()
		close(v.closeCh)
	}
	v.mu.Unlock()

	config := api.DefaultConfig()
	if config.Error != nil {
		return fmt.Errorf("couldn't build vault client config: %w", config.Error)
	}

	// api.DefaultConfig() calls ReadEnvironment() internally, which lets a
	// handful of VAULT_* environment variables silently take effect:
	// VAULT_AGENT_ADDR reroutes all traffic through a local Vault Agent,
	// VAULT_SKIP_VERIFY can disable certificate verification even when
	// skipVerify isn't set, and VAULT_CACERT/VAULT_CAPATH/VAULT_CLIENT_CERT
	// can replace the trusted CA pool or enable client-cert auth. This
	// component's metadata must be the sole source of truth for how it
	// connects to Vault, so undo all of that before applying it below.
	config.AgentAddress = ""
	if transport, ok := config.HttpClient.Transport.(*http.Transport); ok && transport.TLSClientConfig != nil {
		transport.TLSClientConfig.InsecureSkipVerify = false
		transport.TLSClientConfig.RootCAs = nil
		transport.TLSClientConfig.Certificates = nil
		transport.TLSClientConfig.GetClientCertificate = nil
		transport.TLSClientConfig.ServerName = ""
	}

	if m.SkipVerify == "true" {
		v.logger.Warnf("hashicorp vault: you are using 'skipVerify' to skip server config verification, which is unsafe")
	}

	config.Address = v.vaultAddress
	if tlsErr := config.ConfigureTLS(metadataToTLSConfig(&m)); tlsErr != nil {
		return fmt.Errorf("couldn't configure tls: %w", tlsErr)
	}

	client, err := api.NewClient(config)
	if err != nil {
		return fmt.Errorf("couldn't create vault client: %w", err)
	}
	// api.NewClient() also picks up VAULT_NAMESPACE from the environment and
	// scopes every request (including the kubernetes-auth login below) to it.
	// This component has no metadata field for namespace, so a stray
	// VAULT_NAMESPACE in the environment must not silently redirect requests
	// to a Vault Enterprise namespace nothing in the metadata asked for.
	client.ClearNamespace()

	switch m.VaultAuthMethod {
	case "", authMethodToken:
		v.vaultToken = m.VaultToken
		v.vaultTokenMountPath = m.VaultTokenMountPath
		if err := v.initVaultToken(); err != nil {
			return err
		}
		client.SetToken(v.vaultToken)
	case authMethodKubernetes:
		if m.VaultKubernetesRole == "" {
			return errors.New("vaultKubernetesRole is required when vaultAuthMethod is kubernetes")
		}
		if m.VaultToken != "" || m.VaultTokenMountPath != "" {
			return errors.New("vaultToken and vaultTokenMountPath must not be set when vaultAuthMethod is kubernetes")
		}
		// Use the caller's ctx (which the Dapr runtime may bound with a
		// component-init timeout) for the blocking first login only. The
		// background renewal loop that initKubernetesAuth starts outlives
		// this Init() call and uses v.bgCtx instead.
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

// getClient returns the current Vault client, or nil if Init() hasn't
// assigned one yet (e.g. GetSecret/BulkGetSecret called before Init() has
// returned). Goes through mu since Init() assigns v.client under the same
// lock, to avoid a data race between that assignment and concurrent reads
// here.
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

	// Preserve the documented precedence: caPem > caPath > caCert.
	// Only ever set one of the CA fields -- go-rootcerts (used internally by
	// the SDK's ConfigureTLS) applies precedence CACert > CACertBytes > CAPath,
	// the opposite order, so passing more than one through would silently
	// invert the documented contract.
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
//
// This uses the Logical() API directly rather than the SDK's higher-level
// KVv2 helper: KVv2.Get/GetVersion require the secret's "data" field to be a
// JSON object, which breaks text-mode secrets, where the "data" field's raw
// JSON value (object, string, or otherwise) is stringified as-is.
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

	// A nil "data" field is how a soft-deleted or destroyed KV v2 version
	// comes back: Vault responds 404 with a body that still carries
	// {"data": {"data": null, "metadata": {...}}}, and the SDK surfaces that
	// as a regular secret rather than an error. Treat it as ErrNotFound so
	// BulkGetSecret can skip such entries instead of failing the whole read.
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
				// Matches the previous jsoniter/encoding-json-based implementation,
				// which decoded straight into a map[string]string: a JSON null
				// value silently becomes an empty string rather than an error.
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

	// Text mode: stringify the "data" field the same way the previous
	// jsoniter-based implementation did -- objects/arrays/numbers are
	// re-serialized to their compact JSON form, plain JSON strings are used
	// as-is.
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

	// A missing or null "keys" field is how Vault represents a path with no
	// children -- treat it as zero results, not an error. Only a "keys"
	// field present with an unexpected (non-array) type is a genuine
	// malformed-response error.
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

// initVaultToken reads the vault token from the file if token is defined by mount path.
func (v *vaultSecretStore) initVaultToken() error {
	// Test that at least one of them are set if not return error
	if v.vaultToken == "" && v.vaultTokenMountPath == "" {
		return errors.New("token mount path and token not set")
	}

	// Test that both are not set. If so return error
	if v.vaultToken != "" && v.vaultTokenMountPath != "" {
		return errors.New("token mount path and token both set")
	}

	if v.vaultToken != "" {
		return nil
	}

	data, err := os.ReadFile(v.vaultTokenMountPath)
	if err != nil {
		return fmt.Errorf("couldn't read vault token from mount path %s err: %s", v.vaultTokenMountPath, err)
	}
	v.vaultToken = string(bytes.TrimSpace(data))

	return nil
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
	if !v.closed {
		v.closed = true
		if v.bgCancel != nil {
			v.bgCancel()
		}
		if v.closeCh != nil {
			close(v.closeCh)
		}
	}
	v.mu.Unlock()

	v.wg.Wait()
	return nil
}
