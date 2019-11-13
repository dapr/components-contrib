// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"crypto/tls"

	"io/ioutil"

	"crypto/x509"

	"encoding/pem"

	"github.com/hazelcast/hazelcast-go-client/core"
)

// SSLConfig is SSL configuration for client.
// SSLConfig has tls.Config embedded in it so that users can set any field
// of tls config as they wish. SSL config also has some helpers such as SetCaPath, AddClientCertAndKeyPath to
// make configuration easier for users.
type SSLConfig struct {
	*tls.Config
	enabled bool
}

// NewSSLConfig returns SSLConfig.
func NewSSLConfig() *SSLConfig {
	return &SSLConfig{
		Config: new(tls.Config),
	}
}

// SetCaPath sets CA file path.
// It returns an error if file cannot be loaded.
func (sc *SSLConfig) SetCaPath(path string) error {
	// load CA cert
	caCert, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		return core.NewHazelcastIOError("error while loading the CA file, make sure the path exits and "+
			"the format is pem", nil)
	}

	sc.RootCAs = caCertPool
	return nil
}

// AddClientCertAndKeyPath adds client certificate path and client private key path to tls config.
// The files in the given paths must contain PEM encoded data.
// In order to add multiple client certificate-key pairs one should call this function for each of them.
// If certificates is empty then no certificate will be sent to
// the server. If this is unacceptable to the server then it may abort
// the handshake.
// For mutual authentication at least one client certificate should be added.
// It returns an error if any of files cannot be loaded.
func (sc *SSLConfig) AddClientCertAndKeyPath(clientCertPath string, clientPrivateKeyPath string) error {
	cert, err := tls.LoadX509KeyPair(clientCertPath, clientPrivateKeyPath)
	if err != nil {
		return err
	}
	sc.Certificates = append(sc.Certificates, cert)
	return nil
}

// AddClientCertAndEncryptedKeyPath decrypts the keyfile with the given password and
// adds client certificate path and the decrypted client private key to tls config.
// The files in the given paths must contain PEM encoded data.
// The key file should have a DEK-info header otherwise an error will be returned.
// In order to add multiple client certificate-key pairs one should call this function for each of them.
// If certificates is empty then no certificate will be sent to
// the server. If this is unacceptable to the server then it may abort
// the handshake.
// For mutual authentication at least one client certificate should be added.
// It returns an error if any of files cannot be loaded.
func (sc *SSLConfig) AddClientCertAndEncryptedKeyPath(clientCertPath string, clientPrivateKeyPath string,
	password string) error {
	certPEMBlock, err := ioutil.ReadFile(clientCertPath)
	if err != nil {
		return err
	}
	privatePEM, err := ioutil.ReadFile(clientPrivateKeyPath)
	if err != nil {
		return err
	}
	privatePEMBlock, _ := pem.Decode(privatePEM)
	der, err := x509.DecryptPEMBlock(privatePEMBlock, []byte(password))
	if err != nil {
		return err
	}
	privKey, err := x509.ParsePKCS1PrivateKey(der)
	if err != nil {
		return err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: privatePEMBlock.Type, Bytes: x509.MarshalPKCS1PrivateKey(privKey)})
	cert, err := tls.X509KeyPair(certPEMBlock, keyPEM)

	if err != nil {
		return err
	}
	sc.Certificates = append(sc.Certificates, cert)
	return nil
}

// SetEnabled sets enabled field.
// In order to use SSL connection, this field should be enabled.
func (sc *SSLConfig) SetEnabled(enabled bool) {
	sc.enabled = enabled
}

// Enabled returns true if SSL is enabled, false otherwise.
func (sc *SSLConfig) Enabled() bool {
	return sc.enabled
}
