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
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"errors"
	"math/big"

	"github.com/Azure/azure-sdk-for-go/sdk/keyvault/azkeys"
)

// JSONWebKey extends azkeys.JSONWebKey to add methods to export the key.
type JSONWebKey struct {
	azkeys.JSONWebKey
}

// Public returns the public key included the object, as a crypto.PublicKey object.
// This method returns an error if it's invoked on a JSONWebKey object representing a symmetric key.
func (key JSONWebKey) Public() (crypto.PublicKey, error) {
	if key.Kty == nil {
		return nil, errors.New("property Kty is nil")
	}

	switch {
	case IsRSAKey(*key.Kty):
		return key.publicRSA()
	case IsECKey(*key.Kty):
		return key.publicEC()
	}

	return nil, errors.New("unsupported key type")
}

func (key JSONWebKey) publicRSA() (*rsa.PublicKey, error) {
	res := &rsa.PublicKey{}

	// N = modulus
	if len(key.N) == 0 {
		return nil, errors.New("property N is empty")
	}
	res.N = &big.Int{}
	res.N.SetBytes(key.N)

	// e = public exponent
	if len(key.E) == 0 {
		return nil, errors.New("property e is empty")
	}
	res.E = int(big.NewInt(0).SetBytes(key.E).Uint64())

	return res, nil
}

func (key JSONWebKey) publicEC() (*ecdsa.PublicKey, error) {
	res := &ecdsa.PublicKey{}

	if key.Crv == nil {
		return nil, errors.New("property Crv is nil")
	}
	switch *key.Crv {
	case azkeys.JSONWebKeyCurveNameP256:
		res.Curve = elliptic.P256()
	case azkeys.JSONWebKeyCurveNameP384:
		res.Curve = elliptic.P384()
	case azkeys.JSONWebKeyCurveNameP521:
		res.Curve = elliptic.P521()
	case azkeys.JSONWebKeyCurveNameP256K:
		return nil, errors.New("curves of type P-256K are not supported by this method")
	}

	// X coordinate
	if len(key.X) == 0 {
		return nil, errors.New("property X is empty")
	}
	res.X = &big.Int{}
	res.X.SetBytes(key.X)

	// Y coordinate
	if len(key.Y) == 0 {
		return nil, errors.New("property Y is empty")
	}
	res.Y = &big.Int{}
	res.Y.SetBytes(key.Y)

	return res, nil
}

// IsRSAKey returns true if the key is an RSA key (RSA or RSA-HSM).
func IsRSAKey(kt azkeys.JSONWebKeyType) bool {
	return kt == azkeys.JSONWebKeyTypeRSA || kt == azkeys.JSONWebKeyTypeRSAHSM
}

// IsECKey returns true if the key is an EC key (EC or EC-HSM).
func IsECKey(kt azkeys.JSONWebKeyType) bool {
	return kt == azkeys.JSONWebKeyTypeEC || kt == azkeys.JSONWebKeyTypeECHSM
}

// IsSymmetricKey returns true if the key is a symmetric key (Oct or OctHSM).
func IsSymmetricKey(kt azkeys.JSONWebKeyType) bool {
	return kt == azkeys.JSONWebKeyTypeOct || kt == azkeys.JSONWebKeyTypeOctHSM
}

// IsAsymmetric returns true if the key type is asymmetric.
func IsAsymmetric(kt azkeys.JSONWebKeyType) bool {
	return IsECKey(kt) || IsRSAKey(kt)
}
