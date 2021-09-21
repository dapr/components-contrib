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
	"strconv"

	"golang.org/x/net/http2"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

const (
	defaultVaultAddress          string = "https://127.0.0.1:8200"
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
	vaultHTTPHeader              string = "X-Vault-Token"
	vaultHTTPRequestHeader       string = "X-Vault-Request"
)

// vaultSecretStore is a secret store implementation for HashiCorp Vault.
type vaultSecretStore struct {
	client              *http.Client
	vaultAddress        string
	vaultToken          string
	vaultTokenMountPath string
	vaultKVPrefix       string

	logger logger.Logger
}

// tlsConfig is TLS configuration to interact with HashiCorp Vault.
type tlsConfig struct {
	vaultCAPem      string
	vaultCACert     string
	vaultCAPath     string
	vaultSkipVerify bool
	vaultServerName string
}

// vaultKVResponse is the response data from Vault KV.
type vaultKVResponse struct {
	Data struct {
		Data map[string]string `json:"data"`
	} `json:"data"`
}

// vaultListKVResponse is the response data from Vault KV.
type vaultListKVResponse struct {
	Data struct {
		Keys []string `json:"keys"`
	} `json:"data"`
}

// NewHashiCorpVaultSecretStore returns a new HashiCorp Vault secret store.
func NewHashiCorpVaultSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &vaultSecretStore{
		client: &http.Client{},
		logger: logger,
	}
}

// Init creates a HashiCorp Vault client.
func (v *vaultSecretStore) Init(metadata secretstores.Metadata) error {
	props := metadata.Properties

	// Get Vault address
	address := props[componentVaultAddress]
	if address == "" {
		address = defaultVaultAddress
	}

	v.vaultAddress = address

	v.vaultToken = props[componentVaultToken]
	v.vaultTokenMountPath = props[componentVaultTokenMountPath]

	// Test that at least one of them are set if not return error
	if v.vaultToken == "" && v.vaultTokenMountPath == "" {
		return fmt.Errorf("token mount path and token not set")
	}

	// Test that both are not set. If so return error
	if v.vaultToken != "" && v.vaultTokenMountPath != "" {
		return fmt.Errorf("token mount path and token both set")
	}

	vaultKVUsePrefix := props[componentVaultKVUsePrefix]
	vaultKVPrefix := props[componentVaultKVPrefix]
	convertedVaultKVUsePrefix := true
	if vaultKVUsePrefix != "" {
		if v, err := strconv.ParseBool(vaultKVUsePrefix); err == nil {
			convertedVaultKVUsePrefix = v
		} else if err != nil {
			return fmt.Errorf("unable to convert Use Prefix to boolean")
		}
	}

	if !convertedVaultKVUsePrefix {
		vaultKVPrefix = ""
	} else if vaultKVPrefix == "" {
		vaultKVPrefix = defaultVaultKVPrefix
	}

	v.vaultKVPrefix = vaultKVPrefix

	// Generate TLS config
	tlsConf := metadataToTLSConfig(props)

	client, err := v.createHTTPClient(tlsConf)
	if err != nil {
		return fmt.Errorf("couldn't create client using config: %s", err)
	}

	v.client = client

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

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (v *vaultSecretStore) getSecret(secret string) (*vaultKVResponse, error) {
	token, err := v.readVaultToken()
	if err != nil {
		return nil, err
	}

	// Create get secret url
	// TODO: Add support for versioned secrets when the secretstore request has support for it
	vaultSecretPathAddr := fmt.Sprintf("%s/v1/secret/data/%s/%s?version=0", v.vaultAddress, v.vaultKVPrefix, secret)

	httpReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, vaultSecretPathAddr, nil)
	// Set vault token.
	httpReq.Header.Set(vaultHTTPHeader, token)
	// Set X-Vault-Request header
	httpReq.Header.Set(vaultHTTPRequestHeader, "true")
	if err != nil {
		return nil, fmt.Errorf("couldn't generate request: %s", err)
	}

	httpresp, err := v.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("couldn't get secret: %s", err)
	}

	defer httpresp.Body.Close()

	if httpresp.StatusCode != 200 {
		var b bytes.Buffer
		io.Copy(&b, httpresp.Body)
		v.logger.Debugf("getSecret %s couldn't get successful response: %#v, %s", secret, httpresp, b.String())

		return nil, fmt.Errorf("couldn't get successful response, status code %d, body %s",
			httpresp.StatusCode, b.String())
	}

	var d vaultKVResponse

	if err := json.NewDecoder(httpresp.Body).Decode(&d); err != nil {
		return nil, fmt.Errorf("couldn't decode response body: %s", err)
	}

	return &d, nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (v *vaultSecretStore) GetSecret(req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	d, err := v.getSecret(req.Name)
	if err != nil {
		return secretstores.GetSecretResponse{Data: nil}, err
	}

	resp := secretstores.GetSecretResponse{
		Data: map[string]string{},
	}

	// Only using secret data and ignore metadata
	// TODO: add support for metadata response when secretstores support it.
	for k, v := range d.Data.Data {
		resp.Data[k] = v
	}

	return resp, nil
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
func (v *vaultSecretStore) BulkGetSecret(req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
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
		v.logger.Debugf("list keys couldn't get successful response: %#v, %s", httpresp, b.String())

		return secretstores.BulkGetSecretResponse{Data: nil}, fmt.Errorf("list keys couldn't get successful response, status code %d, body %s",
			httpresp.StatusCode, b.String())
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
		secrets, err := v.getSecret(key)
		if err != nil {
			return secretstores.BulkGetSecretResponse{Data: nil}, err
		}

		for k, v := range secrets.Data.Data {
			keyValues[k] = v
		}
		resp.Data[key] = keyValues
	}

	return resp, nil
}

func (v *vaultSecretStore) readVaultToken() (string, error) {
	if v.vaultToken != "" {
		return v.vaultToken, nil
	}

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

// readCertificateFile reads the certificate at given path.
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

// readCertificateFolder scans a folder for certificates.
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
