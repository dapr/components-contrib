/*
Copyright 2026 The Dapr Authors
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

package kms

import (
	"context"
	"fmt"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/lestrrat-go/jwx/v2/jwk"

	internals "github.com/dapr/kit/crypto"
)

// Cloud KMS fixes the scheme, hash and purpose of each asymmetric key version.
func cloudKMSAlgorithm(algorithm kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm) (string, error) {
	switch algorithm {
	case kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_2048_SHA1, kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_3072_SHA1, kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_4096_SHA1:
		return internals.Algorithm_RSA_OAEP, nil
	case kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_2048_SHA256, kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_3072_SHA256, kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_4096_SHA256:
		return internals.Algorithm_RSA_OAEP_256, nil
	case kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_4096_SHA512:
		return internals.Algorithm_RSA_OAEP_512, nil
	case kmspb.CryptoKeyVersion_RSA_SIGN_PKCS1_2048_SHA256, kmspb.CryptoKeyVersion_RSA_SIGN_PKCS1_3072_SHA256, kmspb.CryptoKeyVersion_RSA_SIGN_PKCS1_4096_SHA256:
		return internals.Algorithm_RS256, nil
	case kmspb.CryptoKeyVersion_RSA_SIGN_PKCS1_4096_SHA512:
		return internals.Algorithm_RS512, nil
	case kmspb.CryptoKeyVersion_RSA_SIGN_PSS_2048_SHA256, kmspb.CryptoKeyVersion_RSA_SIGN_PSS_3072_SHA256, kmspb.CryptoKeyVersion_RSA_SIGN_PSS_4096_SHA256:
		return internals.Algorithm_PS256, nil
	case kmspb.CryptoKeyVersion_RSA_SIGN_PSS_4096_SHA512:
		return internals.Algorithm_PS512, nil
	case kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256:
		return internals.Algorithm_ES256, nil
	case kmspb.CryptoKeyVersion_EC_SIGN_P384_SHA384:
		return internals.Algorithm_ES384, nil
	case kmspb.CryptoKeyVersion_EC_SIGN_ED25519:
		return internals.Algorithm_EdDSA, nil
	default:
		return "", fmt.Errorf("unsupported Cloud KMS key algorithm: %s", algorithm)
	}
}

func validatePublicKeyAlgorithm(key jwk.Key, requested string) error {
	actual := key.Algorithm().String()
	if actual != requested {
		return fmt.Errorf("algorithm %s is incompatible with Cloud KMS key algorithm %s", requested, actual)
	}
	return nil
}

func (k *kmsCrypto) validateKeyAlgorithm(ctx context.Context, key, requested string) error {
	publicKey, err := k.GetKey(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to retrieve public key: %w", err)
	}
	return validatePublicKeyAlgorithm(publicKey, requested)
}
