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
	"reflect"
	"testing"
)

func TestEncryptSymmetricAESCBC(t *testing.T) {
	type args struct {
		plaintext []byte
		algorithm string
		key       []byte
		nonce     []byte
	}

	tests := []struct {
		name           string
		args           args
		wantCiphertext []byte
		wantErr        bool
	}{
		{
			name: "key size mismatch",
			args: args{
				algorithm: Algorithm_A128CBC,
				key:       []byte{0x00, 0x01},
				nonce:     mustDecodeHexString("000102030405060708090a0b0c0d0e0f"),
				plaintext: mustDecodeHexString("6bc1bee22e409f96e93d7e117393172a"),
			},
			wantErr: true,
		},
		{
			name: "nonce size mismatch",
			args: args{
				algorithm: Algorithm_A128CBC,
				key:       mustDecodeHexString("2b7e151628aed2a6abf7158809cf4f3c"),
				nonce:     []byte{0x00, 0x01},
				plaintext: mustDecodeHexString("6bc1bee22e409f96e93d7e117393172a"),
			},
			wantErr: true,
		},

		// Next test vectors from NIST publication SP800-38A
		{
			name: "NIST test vector 1",
			args: args{
				algorithm: Algorithm_A128CBC,
				key:       mustDecodeHexString("2b7e151628aed2a6abf7158809cf4f3c"),
				nonce:     mustDecodeHexString("000102030405060708090a0b0c0d0e0f"),
				plaintext: mustDecodeHexString("6bc1bee22e409f96e93d7e117393172a"),
			},
			wantCiphertext: mustDecodeHexString("7649abac8119b246cee98e9b12e9197d"),
		},
		{
			name: "NIST test vector 2",
			args: args{
				algorithm: Algorithm_A128CBC,
				key:       mustDecodeHexString("2b7e151628aed2a6abf7158809cf4f3c"),
				nonce:     mustDecodeHexString("7649abac8119b246cee98e9b12e9197d"),
				plaintext: mustDecodeHexString("ae2d8a571e03ac9c9eb76fac45af8e51"),
			},
			wantCiphertext: mustDecodeHexString("5086cb9b507219ee95db113a917678b2"),
		},
		{
			name: "NIST test vector 3",
			args: args{
				algorithm: Algorithm_A128CBC,
				key:       mustDecodeHexString("2b7e151628aed2a6abf7158809cf4f3c"),
				nonce:     mustDecodeHexString("5086cb9b507219ee95db113a917678b2"),
				plaintext: mustDecodeHexString("30c81c46a35ce411e5fbc1191a0a52ef"),
			},
			wantCiphertext: mustDecodeHexString("73bed6b8e3c1743b7116e69e22229516"),
		},
		{
			name: "NIST test vector 4",
			args: args{
				algorithm: Algorithm_A128CBC,
				key:       mustDecodeHexString("2b7e151628aed2a6abf7158809cf4f3c"),
				nonce:     mustDecodeHexString("73bed6b8e3c1743b7116e69e22229516"),
				plaintext: mustDecodeHexString("f69f2445df4f9b17ad2b417be66c3710"),
			},
			wantCiphertext: mustDecodeHexString("3ff1caa1681fac09120eca307586e1a7"),
		},
		{
			name: "NIST test vector 5",
			args: args{
				algorithm: Algorithm_A192CBC,
				key:       mustDecodeHexString("8e73b0f7da0e6452c810f32b809079e562f8ead2522c6b7b"),
				nonce:     mustDecodeHexString("000102030405060708090a0b0c0d0e0f"),
				plaintext: mustDecodeHexString("6bc1bee22e409f96e93d7e117393172a"),
			},
			wantCiphertext: mustDecodeHexString("4f021db243bc633d7178183a9fa071e8"),
		},
		{
			name: "NIST test vector 6",
			args: args{
				algorithm: Algorithm_A192CBC,
				key:       mustDecodeHexString("8e73b0f7da0e6452c810f32b809079e562f8ead2522c6b7b"),
				nonce:     mustDecodeHexString("4f021db243bc633d7178183a9fa071e8"),
				plaintext: mustDecodeHexString("ae2d8a571e03ac9c9eb76fac45af8e51"),
			},
			wantCiphertext: mustDecodeHexString("b4d9ada9ad7dedf4e5e738763f69145a"),
		},
		{
			name: "NIST test vector 7",
			args: args{
				algorithm: Algorithm_A192CBC,
				key:       mustDecodeHexString("8e73b0f7da0e6452c810f32b809079e562f8ead2522c6b7b"),
				nonce:     mustDecodeHexString("b4d9ada9ad7dedf4e5e738763f69145a"),
				plaintext: mustDecodeHexString("30c81c46a35ce411e5fbc1191a0a52ef"),
			},
			wantCiphertext: mustDecodeHexString("571b242012fb7ae07fa9baac3df102e0"),
		},
		{
			name: "NIST test vector 8",
			args: args{
				algorithm: Algorithm_A192CBC,
				key:       mustDecodeHexString("8e73b0f7da0e6452c810f32b809079e562f8ead2522c6b7b"),
				nonce:     mustDecodeHexString("571b242012fb7ae07fa9baac3df102e0"),
				plaintext: mustDecodeHexString("f69f2445df4f9b17ad2b417be66c3710"),
			},
			wantCiphertext: mustDecodeHexString("08b0e27988598881d920a9e64f5615cd"),
		},
		{
			name: "NIST test vector 9",
			args: args{
				algorithm: Algorithm_A256CBC,
				key:       mustDecodeHexString("603deb1015ca71be2b73aef0857d77811f352c073b6108d72d9810a30914dff4"),
				nonce:     mustDecodeHexString("000102030405060708090a0b0c0d0e0f"),
				plaintext: mustDecodeHexString("6bc1bee22e409f96e93d7e117393172a"),
			},
			wantCiphertext: mustDecodeHexString("f58c4c04d6e5f1ba779eabfb5f7bfbd6"),
		},
		{
			name: "NIST test vector 10",
			args: args{
				algorithm: Algorithm_A256CBC,
				key:       mustDecodeHexString("603deb1015ca71be2b73aef0857d77811f352c073b6108d72d9810a30914dff4"),
				nonce:     mustDecodeHexString("f58c4c04d6e5f1ba779eabfb5f7bfbd6"),
				plaintext: mustDecodeHexString("ae2d8a571e03ac9c9eb76fac45af8e51"),
			},
			wantCiphertext: mustDecodeHexString("9cfc4e967edb808d679f777bc6702c7d"),
		},
		{
			name: "NIST test vector 11",
			args: args{
				algorithm: Algorithm_A256CBC,
				key:       mustDecodeHexString("603deb1015ca71be2b73aef0857d77811f352c073b6108d72d9810a30914dff4"),
				nonce:     mustDecodeHexString("9cfc4e967edb808d679f777bc6702c7d"),
				plaintext: mustDecodeHexString("30c81c46a35ce411e5fbc1191a0a52ef"),
			},
			wantCiphertext: mustDecodeHexString("39f23369a9d9bacfa530e26304231461"),
		},
		{
			name: "NIST test vector 12",
			args: args{
				algorithm: Algorithm_A256CBC,
				key:       mustDecodeHexString("603deb1015ca71be2b73aef0857d77811f352c073b6108d72d9810a30914dff4"),
				nonce:     mustDecodeHexString("39f23369a9d9bacfa530e26304231461"),
				plaintext: mustDecodeHexString("f69f2445df4f9b17ad2b417be66c3710"),
			},
			wantCiphertext: mustDecodeHexString("b2eb05e2c39be9fcda6c19078c6a9d1b"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCiphertext, err := encryptSymmetricAESCBC(tt.args.plaintext, tt.args.algorithm, tt.args.key, tt.args.nonce)
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

func TestEncryptSymmetricAESGCM(t *testing.T) {
	type args struct {
		plaintext      []byte
		algorithm      string
		key            []byte
		nonce          []byte
		associatedData []byte
	}
	tests := []struct {
		name           string
		args           args
		wantCiphertext []byte
		wantTag        []byte
		wantErr        bool
	}{
		// Next test vectors from NIST publication SP800-38d

		{
			name: "NIST test vector 1",
			args: args{
				algorithm: Algorithm_A128GCM,
				key:       mustDecodeHexString("00000000000000000000000000000000"),
				nonce:     mustDecodeHexString("000000000000000000000000"),
				plaintext: []byte{},
			},
			wantCiphertext: []byte{},
			wantTag:        mustDecodeHexString("58e2fccefa7e3061367f1d57a4e7455a"),
		},
		{
			name: "NIST test vector 2",
			args: args{
				algorithm: Algorithm_A128GCM,
				key:       mustDecodeHexString("00000000000000000000000000000000"),
				nonce:     mustDecodeHexString("000000000000000000000000"),
				plaintext: mustDecodeHexString("00000000000000000000000000000000"),
			},
			wantCiphertext: mustDecodeHexString("0388dace60b6a392f328c2b971b2fe78"),
			wantTag:        mustDecodeHexString("ab6e47d42cec13bdf53a67b21257bddf"),
		},
		{
			name: "NIST test vector 3",
			args: args{
				algorithm: Algorithm_A128GCM,
				key:       mustDecodeHexString("feffe9928665731c6d6a8f9467308308"),
				nonce:     mustDecodeHexString("cafebabefacedbaddecaf888"),
				plaintext: mustDecodeHexString("d9313225f88406e5a55909c5aff5269a86a7a9531534f7da2e4c303d8a318a721c3c0c95956809532fcf0e2449a6b525b16aedf5aa0de657ba637b391aafd255"),
			},
			wantCiphertext: mustDecodeHexString("42831ec2217774244b7221b784d0d49ce3aa212f2c02a4e035c17e2329aca12e21d514b25466931c7d8f6a5aac84aa051ba30b396a0aac973d58e091473f5985"),
			wantTag:        mustDecodeHexString("4d5c2af327cd64a62cf35abd2ba6fab4"),
		},
		{
			name: "NIST test vector 4",
			args: args{
				algorithm:      Algorithm_A128GCM,
				key:            mustDecodeHexString("feffe9928665731c6d6a8f9467308308"),
				nonce:          mustDecodeHexString("cafebabefacedbaddecaf888"),
				plaintext:      mustDecodeHexString("d9313225f88406e5a55909c5aff5269a86a7a9531534f7da2e4c303d8a318a721c3c0c95956809532fcf0e2449a6b525b16aedf5aa0de657ba637b39"),
				associatedData: mustDecodeHexString("feedfacedeadbeeffeedfacedeadbeefabaddad2"),
			},
			wantCiphertext: mustDecodeHexString("42831ec2217774244b7221b784d0d49ce3aa212f2c02a4e035c17e2329aca12e21d514b25466931c7d8f6a5aac84aa051ba30b396a0aac973d58e091"),
			wantTag:        mustDecodeHexString("5bc94fbc3221a5db94fae95ae7121a47"),
		},
		{
			name: "NIST test vector 5",
			args: args{
				algorithm: Algorithm_A256GCM,
				key:       mustDecodeHexString("0000000000000000000000000000000000000000000000000000000000000000"),
				nonce:     mustDecodeHexString("000000000000000000000000"),
				plaintext: []byte{},
			},
			wantCiphertext: []byte{},
			wantTag:        mustDecodeHexString("530f8afbc74536b9a963b4f1c4cb738b"),
		},
		{
			name: "NIST test vector 6",
			args: args{
				algorithm: Algorithm_A256GCM,
				key:       mustDecodeHexString("0000000000000000000000000000000000000000000000000000000000000000"),
				nonce:     mustDecodeHexString("000000000000000000000000"),
				plaintext: mustDecodeHexString("00000000000000000000000000000000"),
			},
			wantCiphertext: mustDecodeHexString("cea7403d4d606b6e074ec5d3baf39d18"),
			wantTag:        mustDecodeHexString("d0d1c8a799996bf0265b98b5d48ab919"),
		},
		{
			name: "NIST test vector 7",
			args: args{
				algorithm: Algorithm_A256GCM,
				key:       mustDecodeHexString("feffe9928665731c6d6a8f9467308308feffe9928665731c6d6a8f9467308308"),
				nonce:     mustDecodeHexString("cafebabefacedbaddecaf888"),
				plaintext: mustDecodeHexString("d9313225f88406e5a55909c5aff5269a86a7a9531534f7da2e4c303d8a318a721c3c0c95956809532fcf0e2449a6b525b16aedf5aa0de657ba637b391aafd255"),
			},
			wantCiphertext: mustDecodeHexString("522dc1f099567d07f47f37a32a84427d643a8cdcbfe5c0c97598a2bd2555d1aa8cb08e48590dbb3da7b08b1056828838c5f61e6393ba7a0abcc9f662898015ad"),
			wantTag:        mustDecodeHexString("b094dac5d93471bdec1a502270e3cc6c"),
		},
		{
			name: "NIST test vector 8",
			args: args{
				algorithm:      Algorithm_A256GCM,
				key:            mustDecodeHexString("feffe9928665731c6d6a8f9467308308feffe9928665731c6d6a8f9467308308"),
				nonce:          mustDecodeHexString("cafebabefacedbaddecaf888"),
				plaintext:      mustDecodeHexString("d9313225f88406e5a55909c5aff5269a86a7a9531534f7da2e4c303d8a318a721c3c0c95956809532fcf0e2449a6b525b16aedf5aa0de657ba637b39"),
				associatedData: mustDecodeHexString("feedfacedeadbeeffeedfacedeadbeefabaddad2"),
			},
			wantCiphertext: mustDecodeHexString("522dc1f099567d07f47f37a32a84427d643a8cdcbfe5c0c97598a2bd2555d1aa8cb08e48590dbb3da7b08b1056828838c5f61e6393ba7a0abcc9f662"),
			wantTag:        mustDecodeHexString("76fc6ece0f4e1768cddf8853bb2d551b"),
		},
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

func TestEncryptSymmetricAESKW(t *testing.T) {
	type args struct {
		plaintext []byte
		algorithm string
		key       []byte
	}
	tests := []struct {
		name           string
		args           args
		wantCiphertext []byte
		wantErr        bool
	}{
		// Test cases from RFC3394
		{
			name: "4.1 Wrap 128 bits of Key Data with a 128-bit KEK",
			args: args{
				algorithm: Algorithm_A128KW,
				key:       mustDecodeHexString("000102030405060708090a0b0c0d0e0f"),
				plaintext: mustDecodeHexString("00112233445566778899aabbccddeeff"),
			},
			wantCiphertext: mustDecodeHexString("1fa68b0a8112b447aef34bd8fb5a7b829d3e862371d2cfe5"),
		},
		{
			name: "4.2 Wrap 128 bits of Key Data with a 192-bit KEK",
			args: args{
				algorithm: Algorithm_A192KW,
				key:       mustDecodeHexString("000102030405060708090a0b0c0d0e0f1011121314151617"),
				plaintext: mustDecodeHexString("00112233445566778899aabbccddeeff"),
			},
			wantCiphertext: mustDecodeHexString("96778b25ae6ca435f92b5b97c050aed2468ab8a17ad84e5d"),
		},
		{
			name: "4.3 Wrap 128 bits of Key Data with a 256-bit KEK",
			args: args{
				algorithm: Algorithm_A256KW,
				key:       mustDecodeHexString("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"),
				plaintext: mustDecodeHexString("00112233445566778899aabbccddeeff"),
			},
			wantCiphertext: mustDecodeHexString("64e8c3f9ce0f5ba263e9777905818a2a93c8191e7d6e8ae7"),
		},
		{
			name: "4.4 Wrap 192 bits of Key Data with a 192-bit KEK",
			args: args{
				algorithm: Algorithm_A192KW,
				key:       mustDecodeHexString("000102030405060708090a0b0c0d0e0f1011121314151617"),
				plaintext: mustDecodeHexString("00112233445566778899aabbccddeeff0001020304050607"),
			},
			wantCiphertext: mustDecodeHexString("031d33264e15d33268f24ec260743edce1c6c7ddee725a936ba814915c6762d2"),
		},
		{
			name: "4.5 Wrap 192 bits of Key Data with a 256-bit KEK",
			args: args{
				algorithm: Algorithm_A256KW,
				key:       mustDecodeHexString("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"),
				plaintext: mustDecodeHexString("00112233445566778899aabbccddeeff0001020304050607"),
			},
			wantCiphertext: mustDecodeHexString("a8f9bc1612c68b3ff6e6f4fbe30e71e4769c8b80a32cb8958cd5d17d6b254da1"),
		},
		{
			name: "4.6 Wrap 256 bits of Key Data with a 256-bit KEK",
			args: args{
				algorithm: Algorithm_A256KW,
				key:       mustDecodeHexString("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"),
				plaintext: mustDecodeHexString("00112233445566778899aabbccddeeff000102030405060708090a0b0c0d0e0f"),
			},
			wantCiphertext: mustDecodeHexString("28c9f404c4b810f4cbccb35cfb87f8263f5786e2d80ed326cbc7f0e71a99f43bfb988b9b7a02dd21"),
		},
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

func mustDecodeHexString(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}
