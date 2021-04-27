// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package vault

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	jsoniter "github.com/json-iterator/go"
	"golang.org/x/net/http2"
)

const (
	defaultVaultAddress          string = "https://127.0.0.1:8200"
	defaultVaultEngineName       string = "secret"
	componentVaultAddress        string = "vaultAddr"
	componentCaCert              string = "caCert"
	componentCaPath              string = "caPath"
	componentCaPem               string = "caPem"
	componentSkipVerify          string = "skipVerify"
	componentTLSServerName       string = "tlsServerName"
	componentVaultTokenMountPath string = "vaultTokenMountPath"
	componentVaultKVPrefix       string = "vaultKVPrefix"
	defaultVaultKVPrefix         string = "dapr"
	vaultHTTPHeader              string = "X-Vault-Token"
	vaultHTTPRequestHeader       string = "X-Vault-Request"
	vaultEngineName              string = "engineName"
	DataStr                      string = "data"
)

// vaultSecretStore is a secret store implementation for HashiCorp Vault
type vaultSecretStore struct {
	client              *http.Client
	vaultAddress        string
	vaultTokenMountPath string
	vaultKVPrefix       string

	json jsoniter.API

	logger logger.Logger
}

// tlsConfig is TLS configuration to interact with HashiCorp Vault
type tlsConfig struct {
	vaultCAPem      string
	vaultCACert     string
	vaultCAPath     string
	vaultSkipVerify bool
	vaultServerName string
}

// vaultListKVResponse is the response data from Vault KV.
type vaultListKVResponse struct {
	Data struct {
		Keys []string `json:"keys"`
	} `json:"data"`
}

// NewHashiCorpVaultSecretStore returns a new HashiCorp Vault secret store
func NewHashiCorpVaultSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &vaultSecretStore{
		client: &http.Client{},
		logger: logger,
		json:   jsoniter.ConfigFastest,
	}
}

// Init creates a HashiCorp Vault client
func (v *vaultSecretStore) Init(metadata secretstores.Metadata) error {
	props := metadata.Properties

	// Get Vault address
	address := props[componentVaultAddress]
	if address == "" {
		v.vaultAddress = defaultVaultAddress
	}

	v.vaultAddress = address

	// Generate TLS config
	tlsConf := metadataToTLSConfig(props)

	client, err := v.createHTTPClient(tlsConf)
	if err != nil {
		return fmt.Errorf("couldn't create client using config: %s", err)
	}

	v.client = client

	tokenMountPath := props[componentVaultTokenMountPath]
	if tokenMountPath == "" {
		return fmt.Errorf("token mount path not set")
	}

	v.vaultTokenMountPath = tokenMountPath

	vaultKVPrefix := props[componentVaultKVPrefix]
	if vaultKVPrefix == "" {
		vaultKVPrefix = defaultVaultKVPrefix
	}

	v.vaultKVPrefix = vaultKVPrefix

	return nil
}

func metadataToTLSConfig(props map[string]string) *tlsConfig {
	tlsConf := tlsConfig{}

	// Configure TLS settings
	skipVerify := props[componentSkipVerify]
	tlsConf.vaultSkipVerify = false
	if skipVerify == "true" {
		tlsConf.vaultSkipVerify = true
	}

	tlsConf.vaultCACert = props[componentCaCert]
	tlsConf.vaultCAPem = props[componentCaPem]
	tlsConf.vaultCAPath = props[componentCaPath]
	tlsConf.vaultServerName = props[componentTLSServerName]

	return &tlsConf
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values
func (v *vaultSecretStore) getSecret(engine, secret string) (string, error) {
	token, err := v.readVaultToken()
	if err != nil {
		return "", err
	}

	// Create get secret url
	// TODO: Add support for versioned secrets when the secretstore request has support for it
	vaultSecretPathAddr := fmt.Sprintf("%s/v1/%s/data/%s/%s?version=0", v.vaultAddress, engine, v.vaultKVPrefix, secret)

	httpReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, vaultSecretPathAddr, nil)
	if err != nil {
		return "", fmt.Errorf("couldn't generate request: %s", err)
	}
	// Set vault token.
	httpReq.Header.Set(vaultHTTPHeader, token)
	// Set X-Vault-Request header
	httpReq.Header.Set(vaultHTTPRequestHeader, "true")

	httpresp, err := v.client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("couldn't get secret: %s", err)
	}

	defer httpresp.Body.Close()

	if httpresp.StatusCode != 200 {
		var b bytes.Buffer
		io.Copy(&b, httpresp.Body)

		return "", fmt.Errorf("couldn't to get successful response: %#v, %s",
			httpresp, b.String())
	}

	b, err := ioutil.ReadAll(httpresp.Body)
	if err != nil {
		return "", fmt.Errorf("couldn't read response: %s", err)
	}

	// Only using secret data and ignore metadata
	// TODO: add support for metadata response when secretstores support it.
	res := v.json.Get(b, DataStr, DataStr).ToString()

	return res, nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values
func (v *vaultSecretStore) GetSecret(req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	engineName := req.Metadata[vaultEngineName]
	if engineName == "" {
		engineName = defaultVaultEngineName
	}
	d, err := v.getSecret(engineName, req.Name)
	if err != nil {
		return secretstores.GetSecretResponse{Data: nil}, err
	}

	return secretstores.GetSecretResponse{
		Data: map[string]string{
			req.Name: d,
		},
	}, nil
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values
func (v *vaultSecretStore) BulkGetSecret(req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	engineName := req.Metadata[vaultEngineName]
	if engineName == "" {
		engineName = defaultVaultEngineName
	}
	token, err := v.readVaultToken()
	if err != nil {
		return secretstores.BulkGetSecretResponse{Data: nil}, err
	}

	// Create list secrets url
	vaultSecretsPathAddr := fmt.Sprintf("%s/v1/secret/metadata/%s", v.vaultAddress, v.vaultKVPrefix)

	httpReq, err := http.NewRequestWithContext(context.Background(), "LIST", vaultSecretsPathAddr, nil)
	if err != nil {
		return secretstores.BulkGetSecretResponse{Data: nil}, fmt.Errorf("couldn't generate request: %s", err)
	}

	// Set vault token.
	httpReq.Header.Set(vaultHTTPHeader, token)
	// Set X-Vault-Request header
	httpReq.Header.Set(vaultHTTPRequestHeader, "true")
	httpresp, err := v.client.Do(httpReq)
	if err != nil {
		return secretstores.BulkGetSecretResponse{Data: nil}, fmt.Errorf("couldn't get secret: %s", err)
	}

	defer httpresp.Body.Close()

	if httpresp.StatusCode != 200 {
		var b bytes.Buffer
		io.Copy(&b, httpresp.Body)

		return secretstores.BulkGetSecretResponse{Data: nil}, fmt.Errorf("couldn't get successful response: %#v, %s",
			httpresp, b.String())
	}

	var d vaultListKVResponse

	if err := json.NewDecoder(httpresp.Body).Decode(&d); err != nil {
		return secretstores.BulkGetSecretResponse{Data: nil}, fmt.Errorf("couldn't decode response body: %s", err)
	}

	resp := secretstores.BulkGetSecretResponse{
		Data: map[string]map[string]string{},
	}

	for _, key := range d.Data.Keys {
		keyValues := map[string]string{}
		secrets, err := v.getSecret(engineName, key)
		if err != nil {
			return secretstores.BulkGetSecretResponse{Data: nil}, err
		}

		keyValues[key] = secrets

		resp.Data[key] = keyValues
	}

	return resp, nil
}

func (v *vaultSecretStore) readVaultToken() (string, error) {
	data, err := ioutil.ReadFile(v.vaultTokenMountPath)
	if err != nil {
		return "", fmt.Errorf("couldn't read vault token: %s", err)
	}

	return string(bytes.TrimSpace(data)), nil
}

func (v *vaultSecretStore) createHTTPClient(config *tlsConfig) (*http.Client, error) {
	rootCAPools, err := v.getRootCAsPools(config.vaultCAPem, config.vaultCAPath, config.vaultCACert)
	if err != nil {
		return nil, err
	}

	tlsClientConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    rootCAPools,
	}

	if config.vaultSkipVerify {
		tlsClientConfig.InsecureSkipVerify = true
	}

	if config.vaultServerName != "" {
		tlsClientConfig.ServerName = config.vaultServerName
	}

	// Setup http transport
	transport := &http.Transport{
		TLSClientConfig: tlsClientConfig,
	}

	// Configure http2 client
	err = http2.ConfigureTransport(transport)
	if err != nil {
		return nil, errors.New("failed to configure http2")
	}

	return &http.Client{
		Transport: transport,
	}, nil
}

// getRootCAsPools returns root CAs when you give it CA Pem file, CA path, and CA Certificate. Default is system certificates.
func (v *vaultSecretStore) getRootCAsPools(vaultCAPem string, vaultCAPath string, vaultCACert string) (*x509.CertPool, error) {
	if vaultCAPem != "" {
		certPool := x509.NewCertPool()
		cert := []byte(vaultCAPem)
		if ok := certPool.AppendCertsFromPEM(cert); !ok {
			return nil, fmt.Errorf("couldn't read PEM")
		}

		return certPool, nil
	}

	if vaultCAPath != "" {
		certPool := x509.NewCertPool()
		if err := readCertificateFolder(certPool, vaultCAPath); err != nil {
			return nil, err
		}

		return certPool, nil
	}

	if vaultCACert != "" {
		certPool := x509.NewCertPool()
		if err := readCertificateFile(certPool, vaultCACert); err != nil {
			return nil, err
		}

		return certPool, nil
	}

	certPool, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("couldn't read system certs: %s", err)
	}

	return certPool, err
}

// readCertificateFile reads the certificate at given path
func readCertificateFile(certPool *x509.CertPool, path string) error {
	// Read certificate file
	pemFile, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("couldn't read CA file from disk: %s", err)
	}

	if ok := certPool.AppendCertsFromPEM(pemFile); !ok {
		return fmt.Errorf("couldn't read PEM")
	}

	return nil
}

// readCertificateFolder scans a folder for certificates
func readCertificateFolder(certPool *x509.CertPool, path string) error {
	err := filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		return readCertificateFile(certPool, p)
	})
	if err != nil {
		return fmt.Errorf("couldn't read certificates at %s: %s", path, err)
	}

	return nil
}
