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

package crypto

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestEncryptSymmetricAESCBC(t *testing.T) {
	type args struct {
		plaintext []byte
		algorithm string
		key       []byte
		iv        []byte
	}
	type test struct {
		name           string
		args           args
		wantCiphertext []byte
		wantErr        bool
	}
	tests := []test{
		{
			name: "key size mismatch",
			args: args{
				algorithm: Algorithm_A128CBC,
				key:       []byte{0x00, 0x01},
				iv:        mustDecodeHexString("000102030405060708090a0b0c0d0e0f"),
				plaintext: mustDecodeHexString("6bc1bee22e409f96e93d7e117393172a"),
			},
			wantErr: true,
		},
		{
			name: "nonce size mismatch",
			args: args{
				algorithm: Algorithm_A128CBC,
				key:       mustDecodeHexString("2b7e151628aed2a6abf7158809cf4f3c"),
				iv:        []byte{0x00, 0x01},
				plaintext: mustDecodeHexString("6bc1bee22e409f96e93d7e117393172a"),
			},
			wantErr: true,
		},
	}

	// Test vectors from NIST publication SP800-38A
	for _, v := range readTestVectors("symmetric-test-vectors.json", "aes-cbc") {
		tests = append(tests, test{
			name: v.Name,
			args: args{
				algorithm: v.Algorithm,
				key:       mustDecodeHexString(v.Key),
				iv:        mustDecodeHexString(v.Nonce),
				plaintext: mustDecodeHexString(v.Plaintext),
			},
			wantCiphertext: mustDecodeHexString(v.Ciphertext),
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCiphertext, err := encryptSymmetricAESCBC(tt.args.plaintext, tt.args.algorithm, tt.args.key, tt.args.iv)
			if (err != nil) != tt.wantErr {
				t.Errorf("encryptSymmetricAESCBC() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotCiphertext, tt.wantCiphertext) {
				t.Errorf("encryptSymmetricAESCBC() = %v, want %v", gotCiphertext, tt.wantCiphertext)
			}
		})
	}
}

func TestDecryptSymmetricAESCBC(t *testing.T) {
	type args struct {
		ciphertext []byte
		algorithm  string
		key        []byte
		iv         []byte
	}
	type test struct {
		name          string
		args          args
		wantPlaintext []byte
		wantErr       bool
	}
	tests := []test{}

	// Test vectors from NIST publication SP800-38A
	for _, v := range readTestVectors("symmetric-test-vectors.json", "aes-cbc") {
		tests = append(tests, test{
			name: v.Name,
			args: args{
				algorithm:  v.Algorithm,
				key:        mustDecodeHexString(v.Key),
				iv:         mustDecodeHexString(v.Nonce),
				ciphertext: mustDecodeHexString(v.Ciphertext),
			},
			wantPlaintext: mustDecodeHexString(v.Plaintext),
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPlaintext, err := decryptSymmetricAESCBC(tt.args.ciphertext, tt.args.algorithm, tt.args.key, tt.args.iv)
			if (err != nil) != tt.wantErr {
				t.Errorf("decryptSymmetricAESCBC() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotPlaintext, tt.wantPlaintext) {
				t.Errorf("decryptSymmetricAESCBC() = %v, want %v", gotPlaintext, tt.wantPlaintext)
			}
		})
	}
}

func TestEncryptSymmetricAESGCM(t *testing.T) {
	type args struct {
		plaintext      []byte
		algorithm      string
		key            []byte
		nonce          []byte
		associatedData []byte
	}
	type test struct {
		name           string
		args           args
		wantCiphertext []byte
		wantTag        []byte
		wantErr        bool
	}
	tests := []test{}

	// Test vectors from NIST publication SP800-38d
	for _, v := range readTestVectors("symmetric-test-vectors.json", "aes-gcm") {
		tests = append(tests, test{
			name: v.Name,
			args: args{
				algorithm:      v.Algorithm,
				key:            mustDecodeHexString(v.Key),
				nonce:          mustDecodeHexString(v.Nonce),
				plaintext:      mustDecodeHexString(v.Plaintext),
				associatedData: mustDecodeHexString(v.AssociatedData),
			},
			wantCiphertext: mustDecodeHexString(v.Ciphertext),
			wantTag:        mustDecodeHexString(v.Tag),
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCiphertext, gotTag, err := encryptSymmetricAESGCM(tt.args.plaintext, tt.args.algorithm, tt.args.key, tt.args.nonce, tt.args.associatedData)
			if (err != nil) != tt.wantErr {
				t.Errorf("encryptSymmetricAESGCM() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotCiphertext, tt.wantCiphertext) {
				t.Errorf("encryptSymmetricAESGCM() gotCiphertext = %v, want %v", gotCiphertext, tt.wantCiphertext)
			}
			if !reflect.DeepEqual(gotTag, tt.wantTag) {
				t.Errorf("encryptSymmetricAESGCM() gotTag = %v, want %v", gotTag, tt.wantTag)
			}
		})
	}
}

func TestDecryptSymmetricAESGCM(t *testing.T) {
	type args struct {
		ciphertext     []byte
		algorithm      string
		key            []byte
		nonce          []byte
		tag            []byte
		associatedData []byte
	}
	type test struct {
		name          string
		args          args
		wantPlaintext []byte
		wantErr       bool
	}
	tests := []test{}

	// Test vectors from NIST publication SP800-38d
	for _, v := range readTestVectors("symmetric-test-vectors.json", "aes-gcm") {
		tests = append(tests, test{
			name: v.Name,
			args: args{
				algorithm:      v.Algorithm,
				key:            mustDecodeHexString(v.Key),
				nonce:          mustDecodeHexString(v.Nonce),
				ciphertext:     mustDecodeHexString(v.Ciphertext),
				tag:            mustDecodeHexString(v.Tag),
				associatedData: mustDecodeHexString(v.AssociatedData),
			},
			wantPlaintext: mustDecodeHexString(v.Plaintext),
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPlaintext, err := decryptSymmetricAESGCM(tt.args.ciphertext, tt.args.algorithm, tt.args.key, tt.args.nonce, tt.args.tag, tt.args.associatedData)
			if (err != nil) != tt.wantErr {
				t.Errorf("decryptSymmetricAESGCM() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotPlaintext, tt.wantPlaintext) && len(gotPlaintext) != 0 && len(tt.wantPlaintext) != 0 {
				t.Errorf("decryptSymmetricAESGCM() = %v, want %v", gotPlaintext, tt.wantPlaintext)
			}
		})
	}
}

func TestEncryptSymmetricAESKW(t *testing.T) {
	type args struct {
		plaintext []byte
		algorithm string
		key       []byte
	}
	type test struct {
		name           string
		args           args
		wantCiphertext []byte
		wantErr        bool
	}
	tests := []test{}

	// Test cases from RFC3394
	for _, v := range readTestVectors("symmetric-test-vectors.json", "aes-kw") {
		tests = append(tests, test{
			name: v.Name,
			args: args{
				algorithm: v.Algorithm,
				key:       mustDecodeHexString(v.Key),
				plaintext: mustDecodeHexString(v.Plaintext),
			},
			wantCiphertext: mustDecodeHexString(v.Ciphertext),
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCiphertext, err := encryptSymmetricAESKW(tt.args.plaintext, tt.args.algorithm, tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("encryptSymmetricAESKW() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotCiphertext, tt.wantCiphertext) {
				t.Errorf("encryptSymmetricAESKW() = %v, want %v", gotCiphertext, tt.wantCiphertext)
			}
		})
	}
}

func TestDecryptSymmetricAESKW(t *testing.T) {
	type args struct {
		ciphertext []byte
		algorithm  string
		key        []byte
	}
	type test struct {
		name          string
		args          args
		wantPlaintext []byte
		wantErr       bool
	}
	tests := []test{}

	// Test cases from RFC3394
	for _, v := range readTestVectors("symmetric-test-vectors.json", "aes-kw") {
		tests = append(tests, test{
			name: v.Name,
			args: args{
				algorithm:  v.Algorithm,
				key:        mustDecodeHexString(v.Key),
				ciphertext: mustDecodeHexString(v.Ciphertext),
			},
			wantPlaintext: mustDecodeHexString(v.Plaintext),
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPlaintext, err := decryptSymmetricAESKW(tt.args.ciphertext, tt.args.algorithm, tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("decryptSymmetricAESKW() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotPlaintext, tt.wantPlaintext) {
				t.Errorf("decryptSymmetricAESKW() = %v, want %v", gotPlaintext, tt.wantPlaintext)
			}
		})
	}
}

func TestEncryptSymmetricChaCha20Poly1305(t *testing.T) {
	type args struct {
		plaintext      []byte
		algorithm      string
		key            []byte
		nonce          []byte
		associatedData []byte
	}
	type test struct {
		name           string
		args           args
		wantCiphertext []byte
		wantTag        []byte
		wantErr        bool
	}
	tests := []test{}

	for _, v := range readTestVectors("symmetric-test-vectors.json", "chacha20-poly1305") {
		tests = append(tests, test{
			name: v.Name,
			args: args{
				algorithm:      v.Algorithm,
				key:            mustDecodeHexString(v.Key),
				nonce:          mustDecodeHexString(v.Nonce),
				plaintext:      mustDecodeHexString(v.Plaintext),
				associatedData: mustDecodeHexString(v.AssociatedData),
			},
			wantCiphertext: mustDecodeHexString(v.Ciphertext),
			wantTag:        mustDecodeHexString(v.Tag),
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCiphertext, gotTag, err := encryptSymmetricChaCha20Poly1305(tt.args.plaintext, tt.args.algorithm, tt.args.key, tt.args.nonce, tt.args.associatedData)
			if (err != nil) != tt.wantErr {
				t.Errorf("encryptSymmetricChaCha20Poly1305() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotCiphertext, tt.wantCiphertext) {
				t.Errorf("encryptSymmetricChaCha20Poly1305() gotCiphertext = %v, want %v", gotCiphertext, tt.wantCiphertext)
			}
			if !reflect.DeepEqual(gotTag, tt.wantTag) {
				t.Errorf("encryptSymmetricChaCha20Poly1305() gotTag = %v, want %v", gotTag, tt.wantTag)
			}
		})
	}
}

func TestDecryptSymmetricChaCha20Poly1305(t *testing.T) {
	type args struct {
		ciphertext     []byte
		algorithm      string
		key            []byte
		nonce          []byte
		tag            []byte
		associatedData []byte
	}
	type test struct {
		name          string
		args          args
		wantPlaintext []byte
		wantErr       bool
	}
	tests := []test{}

	for _, v := range readTestVectors("symmetric-test-vectors.json", "chacha20-poly1305") {
		tests = append(tests, test{
			name: v.Name,
			args: args{
				algorithm:      v.Algorithm,
				key:            mustDecodeHexString(v.Key),
				nonce:          mustDecodeHexString(v.Nonce),
				ciphertext:     mustDecodeHexString(v.Ciphertext),
				tag:            mustDecodeHexString(v.Tag),
				associatedData: mustDecodeHexString(v.AssociatedData),
			},
			wantPlaintext: mustDecodeHexString(v.Plaintext),
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPlaintext, err := decryptSymmetricChaCha20Poly1305(tt.args.ciphertext, tt.args.algorithm, tt.args.key, tt.args.nonce, tt.args.tag, tt.args.associatedData)
			if (err != nil) != tt.wantErr {
				t.Errorf("decryptSymmetricChaCha20Poly1305() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotPlaintext, tt.wantPlaintext) && len(gotPlaintext) != 0 && len(tt.wantPlaintext) != 0 {
				t.Errorf("decryptSymmetricChaCha20Poly1305() = %v, want %v", gotPlaintext, tt.wantPlaintext)
			}
		})
	}
}

type testVector struct {
	Name           string `json:"name"`
	Algorithm      string `json:"algorithm"`
	Key            string `json:"key"`
	Nonce          string `json:"nonce"`
	AssociatedData string `json:"associatedData"`
	Plaintext      string `json:"plaintext"`
	Ciphertext     string `json:"ciphertext"`
	Tag            string `json:"tag"`
}

func readTestVectors(fileName string, vectorsName string) []testVector {
	f, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	read := map[string][]testVector{}
	err = json.NewDecoder(f).Decode(&read)
	if err != nil {
		panic(err)
	}

	return read[vectorsName]
}

func mustDecodeHexString(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}
