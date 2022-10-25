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

package keyvault

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/keyvault/azkeys"
)

var (
	validEncryptionAlgs map[string]struct{}
	validSignatureAlgs  map[string]struct{}
)

func init() {
	listEncryption := azkeys.PossibleJSONWebKeyEncryptionAlgorithmValues()
	validEncryptionAlgs = make(map[string]struct{}, len(listEncryption))
	for _, v := range listEncryption {
		validEncryptionAlgs[string(v)] = struct{}{}
	}

	listSignature := azkeys.PossibleJSONWebKeySignatureAlgorithmValues()
	validSignatureAlgs = make(map[string]struct{}, len(listSignature))
	for _, v := range listEncryption {
		validSignatureAlgs[string(v)] = struct{}{}
	}
}

// GetJWKEncryptionAlgorithm returns a JSONWebKeyEncryptionAlgorithm constant is the algorithm is a supported one.
func GetJWKEncryptionAlgorithm(algorithm string) *azkeys.JSONWebKeyEncryptionAlgorithm {
	if _, ok := validEncryptionAlgs[algorithm]; ok {
		return to.Ptr(azkeys.JSONWebKeyEncryptionAlgorithm(algorithm))
	} else {
		return nil
	}
}

// GetJWKSignatureAlgorithm returns a JSONWebKeySignatureAlgorithm constant is the algorithm is a supported one.
func GetJWKSignatureAlgorithm(algorithm string) *azkeys.JSONWebKeySignatureAlgorithm {
	if _, ok := validEncryptionAlgs[algorithm]; ok {
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
