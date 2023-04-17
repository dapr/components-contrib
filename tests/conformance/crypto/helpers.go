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

package crypto

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"hash"
	"io"
	"log"
	"strings"
	"testing"

	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

type keybag struct {
	private   keylist
	public    keylist
	symmetric keylist
}

type keylist map[string][]string

func (l keylist) addKey(key string, algorithms ...string) {
	for _, alg := range algorithms {
		if l[alg] == nil {
			l[alg] = []string{key}
		} else {
			l[alg] = append(l[alg], key)
		}
	}
}

//nolint:unused
func (l keylist) testForAlgorithm(t *testing.T, algorithm string, tf func(keyName string) func(t *testing.T)) {
	t.Helper()

	for i, keyName := range l[algorithm] {
		t.Run(fmt.Sprintf("key: %d", i), tf(keyName))
	}
}

func (l keylist) testForAllAlgorithms(t *testing.T, tf func(algorithm, keyName string) func(t *testing.T)) {
	t.Helper()

	l.testForAllAlgorithmsInList(t, nil, tf)
}

func (l keylist) testForAllAlgorithmsInList(t *testing.T, list any, tf func(algorithm, keyName string) func(t *testing.T)) {
	t.Helper()

	var listSlice []string
	switch x := list.(type) {
	case nil:
		// nop
	case []string:
		listSlice = x
	case string:
		listSlice = strings.Split(x, " ")
	}

	for alg, keys := range l {
		if len(listSlice) > 0 && !slices.Contains(listSlice, alg) {
			continue
		}
		t.Run("algorithm: "+alg, func(t *testing.T) {
			for i, keyName := range keys {
				t.Run(fmt.Sprintf("key: %d", i), tf(alg, keyName))
			}
		})
	}
}

func newKeybagFromConfig(config TestConfig) keybag {
	// Parse all keys from the config
	bag := keybag{
		public:    make(keylist),
		private:   make(keylist),
		symmetric: make(keylist),
	}
	for _, k := range config.Keys {
		switch k.KeyType {
		case "symmetric":
			bag.symmetric.addKey(k.Name, k.Algorithms...)
		case "public":
			bag.public.addKey(k.Name, k.Algorithms...)
		case "private":
			bag.private.addKey(k.Name, k.Algorithms...)
		default:
			log.Printf("WARN: found key with invalid type: '%s'\n", k.KeyType)
		}
	}
	return bag
}

func randomBytes(t *testing.T, size int) []byte {
	if size == 0 {
		return nil
	}

	b := make([]byte, size)
	l, err := io.ReadFull(rand.Reader, b)
	require.NoError(t, err)
	require.Equal(t, size, l)
	return b
}

func requireKeyPublic(t *testing.T, key jwk.Key) {
	var rawKey any
	err := key.Raw(&rawKey)
	require.NoError(t, err)

	switch rawKey.(type) {
	case ed25519.PublicKey,
		rsa.PublicKey,
		*rsa.PublicKey,
		ecdsa.PublicKey,
		*ecdsa.PublicKey:
		// all good - nop
	default:
		t.Errorf("key is not a public key: %T", rawKey)
	}
}

// Returns the size of the IV/nonce for the given algorithm
func nonceSizeForAlgorithm(alg string) int {
	switch alg {
	case "A128CBC", "A192CBC", "A256CBC", "A128CBC-HS256", "A192CBC-HS384", "A256CBC-HS512":
		return 16
	case "A128GCM", "A192GCM", "A256GCM", "C20P", "C20PKW", "A128GCMKW", "A192GCMKW", "A256GCMKW":
		return 12
	case "XC20P", "XC20PKW":
		return 24
	case "A128KW", "A192KW", "A256KW":
		return 0
	default:
		return 0
	}
}

// Returns true if the algorithm uses tags
func hasTag(alg string) bool {
	switch alg {
	case "A128GCM", "A192GCM", "A256GCM", "C20P", "XC20P", "C20PKW", "XC20PKW", "A128GCMKW", "A192GCMKW", "A256GCMKW", "A128CBC-HS256", "A192CBC-HS384", "A256CBC-HS512":
		return true
	default:
		return false
	}
}

// Returns a digest of the message for signing with the given algorithm
func hashMessageForSigning(message []byte, alg string) []byte {
	// For EdDSA, we need to pass the raw message as "digest", as it gets hashed internally by the algorithm
	if alg == "EdDSA" {
		return message
	}

	// Calculate the SHA hash depending on the size
	var h hash.Hash
	switch alg {
	case "ES256", "PS256", "RS256", "HS256":
		h = crypto.SHA256.New()
	case "ES384", "PS384", "RS384", "HS384":
		h = crypto.SHA384.New()
	case "ES512", "PS512", "RS512", "HS512":
		h = crypto.SHA512.New()
	default:
		panic("Unsupported algorithm")
	}

	_, err := h.Write(message)
	if err != nil {
		panic(err)
	}

	return h.Sum(nil)
}
