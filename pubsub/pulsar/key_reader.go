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

package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar/crypto"
)

// DataKeyReader is a custom implementation of KeyReader
type DataKeyReader struct {
	publicKey  string
	privateKey string
}

// NewDataKeyReader returns a new instance of DataKeyReader
func NewDataKeyReader(publicKey, privateKey string) *DataKeyReader {
	return &DataKeyReader{
		publicKey:  publicKey,
		privateKey: privateKey,
	}
}

// PublicKey read public key from string
func (d *DataKeyReader) PublicKey(keyName string, keyMeta map[string]string) (*crypto.EncryptionKeyInfo, error) {
	return readKey(keyName, d.publicKey, keyMeta)
}

// PrivateKey read private key from string
func (d *DataKeyReader) PrivateKey(keyName string, keyMeta map[string]string) (*crypto.EncryptionKeyInfo, error) {
	return readKey(keyName, d.privateKey, keyMeta)
}

func readKey(keyName, key string, keyMeta map[string]string) (*crypto.EncryptionKeyInfo, error) {
	return crypto.NewEncryptionKeyInfo(keyName, []byte(key), keyMeta), nil
}
