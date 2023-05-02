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

package keyvault

import (
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"

	internals "github.com/dapr/kit/crypto"
)

var (
	validEncryptionAlgs map[string]struct{}
	validSignatureAlgs  map[string]struct{}
	encryptionAlgsList  []string
	signatureAlgsList   []string

	// Used to initialize validEncryptionAlgs and validSignatureAlgs lazily when the first component of this kind is initialized
	algsParsed sync.Once
)

// GetJWKEncryptionAlgorithm returns a JSONWebKeyEncryptionAlgorithm constant is the algorithm is a supported one.
func GetJWKEncryptionAlgorithm(algorithm string) *azkeys.JSONWebKeyEncryptionAlgorithm {
	// Special case for AES-CBC, since we treat A[NNN]CBC as having PKCS#7 padding, and A[NNN]CBC-NOPAD as not using padding
	switch algorithm {
	case internals.Algorithm_A128CBC, internals.Algorithm_A192CBC, internals.Algorithm_A256CBC:
		// Append "PAD", e.g. "A128CBCPAD"
		algorithm += "PAD"
	case internals.Algorithm_A128CBC_NOPAD, internals.Algorithm_A192CBC_NOPAD, internals.Algorithm_A256CBC_NOPAD:
		// Remove the "-NOPAD" suffix, e.g. "A128CBC"
		algorithm = algorithm[:len(algorithm)-6]
	}

	if _, ok := validEncryptionAlgs[algorithm]; ok {
		return to.Ptr(azkeys.JSONWebKeyEncryptionAlgorithm(algorithm))
	} else {
		return nil
	}
}

// GetJWKSignatureAlgorithm returns a JSONWebKeySignatureAlgorithm constant is the algorithm is a supported one.
func GetJWKSignatureAlgorithm(algorithm string) *azkeys.JSONWebKeySignatureAlgorithm {
	if _, ok := validSignatureAlgs[algorithm]; ok {
		return to.Ptr(azkeys.JSONWebKeySignatureAlgorithm(algorithm))
	} else {
		return nil
	}
}

type algorithms interface {
	azkeys.JSONWebKeyEncryptionAlgorithm | azkeys.JSONWebKeySignatureAlgorithm
}

// IsAlgorithmAsymmetric returns true if the algorithm identifier is asymmetric.
func IsAlgorithmAsymmetric[T algorithms](algorithm T) bool {
	algStr := string(algorithm)
	switch algStr[0:2] {
	case "RS", "ES", "PS":
		// RSNULL is a reserved keyword
		return algStr != "RSNULL"
	default:
		return false
	}
}
