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

// Package keywrap provides an AES-KW keywrap implementation as defined in RFC-3394.
package aeskw

/*!
Adapted from https://github.com/NickBall/go-aes-key-wrap/tree/1c3aa3e4dfc5b00bec9983bd1de6a71b3d52cd6d
Copyright (c) 2017 Nick Ball
License: MIT (https://github.com/NickBall/go-aes-key-wrap/blob/1c3aa3e4dfc5b00bec9983bd1de6a71b3d52cd6d/LICENSE)
*/

import (
	"crypto/cipher"
	"crypto/subtle"
	"encoding/binary"
	"errors"
)

// defaultIV as specified in RFC-3394
var defaultIV = []byte{0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6}

// Wrap encrypts the provided key data (cek) with the given AES cipher (and corresponding key), using the AES Key Wrap algorithm (RFC-3394)
func Wrap(block cipher.Block, cek []byte) ([]byte, error) {
	if len(cek)%8 != 0 {
		return nil, errors.New("cek must be in 8-byte blocks")
	}

	// Initialize variables
	a := make([]byte, 8)
	copy(a, defaultIV)
	n := len(cek) / 8

	// Calculate intermediate
	r := make([][]byte, n)
	for i := range r {
		r[i] = make([]byte, 8)
		copy(r[i], cek[i*8:])
	}

	for j := 0; j <= 5; j++ {
		for i := 1; i <= n; i++ {
			b := arrConcat(a, r[i-1])
			block.Encrypt(b, b)

			t := (n * j) + i
			tBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(tBytes, uint64(t))

			copy(a, arrXor(b[:len(b)/2], tBytes))
			copy(r[i-1], b[len(b)/2:])
		}
	}

	// Output
	c := make([]byte, (n+1)*8)
	copy(c, a)
	for i := 1; i <= n; i++ {
		for j := range r[i-1] {
			c[(i*8)+j] = r[i-1][j]
		}
	}
	return c, nil
}

// Unwrap decrypts the provided cipher text with the given AES cipher (and corresponding key), using the AES Key Wrap algorithm (RFC-3394).
// The decrypted cipher text is verified using the default IV and will return an error if validation fails.
func Unwrap(block cipher.Block, cipherText []byte) ([]byte, error) {
	// Initialize variables
	a := make([]byte, 8)
	n := (len(cipherText) / 8) - 1

	r := make([][]byte, n)
	for i := range r {
		r[i] = make([]byte, 8)
		copy(r[i], cipherText[(i+1)*8:])
	}
	copy(a, cipherText[:8])

	// Compute intermediate values
	for j := 5; j >= 0; j-- {
		for i := n; i >= 1; i-- {
			t := (n * j) + i
			tBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(tBytes, uint64(t))

			b := arrConcat(arrXor(a, tBytes), r[i-1])
			block.Decrypt(b, b)

			copy(a, b[:len(b)/2])
			copy(r[i-1], b[len(b)/2:])
		}
	}

	if subtle.ConstantTimeCompare(a, defaultIV) != 1 {
		return nil, errors.New("integrity check failed - unexpected IV")
	}

	// Output
	c := arrConcat(r...)
	return c, nil
}

func arrConcat(arrays ...[]byte) []byte {
	out := make([]byte, len(arrays[0]))
	copy(out, arrays[0])
	for _, array := range arrays[1:] {
		out = append(out, array...)
	}

	return out
}

func arrXor(arrL []byte, arrR []byte) []byte {
	out := make([]byte, len(arrL))
	for x := range arrL {
		out[x] = arrL[x] ^ arrR[x]
	}
	return out
}
