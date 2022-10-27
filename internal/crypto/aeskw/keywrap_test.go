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

package aeskw

/*!
Adapted from https://github.com/NickBall/go-aes-key-wrap/tree/1c3aa3e4dfc5b00bec9983bd1de6a71b3d52cd6d
Copyright (c) 2017 Nick Ball
License: MIT (https://github.com/NickBall/go-aes-key-wrap/blob/1c3aa3e4dfc5b00bec9983bd1de6a71b3d52cd6d/LICENSE)
*/

import (
	"crypto/aes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

type input struct {
	Case     string
	Kek      string
	Data     string
	Expected string
}

func TestWrapRfc3394Vectors(t *testing.T) {
	vectors := []input{
		{
			Case:     "4.1 Wrap 128 bits of Key Data with a 128-bit KEK",
			Kek:      "000102030405060708090A0B0C0D0E0F",
			Data:     "00112233445566778899AABBCCDDEEFF",
			Expected: "1FA68B0A8112B447AEF34BD8FB5A7B829D3E862371D2CFE5",
		},
		{
			Case:     "4.2 Wrap 128 bits of Key Data with a 192-bit KEK",
			Kek:      "000102030405060708090A0B0C0D0E0F1011121314151617",
			Data:     "00112233445566778899AABBCCDDEEFF",
			Expected: "96778B25AE6CA435F92B5B97C050AED2468AB8A17AD84E5D",
		},
		{
			Case:     "4.3 Wrap 128 bits of Key Data with a 256-bit KEK",
			Kek:      "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F",
			Data:     "00112233445566778899AABBCCDDEEFF",
			Expected: "64E8C3F9CE0F5BA263E9777905818A2A93C8191E7D6E8AE7",
		},
		{
			Case:     "4.4 Wrap 192 bits of Key Data with a 192-bit KEK",
			Kek:      "000102030405060708090A0B0C0D0E0F1011121314151617",
			Data:     "00112233445566778899AABBCCDDEEFF0001020304050607",
			Expected: "031D33264E15D33268F24EC260743EDCE1C6C7DDEE725A936BA814915C6762D2",
		},
		{
			Case:     "4.5 Wrap 192 bits of Key Data with a 256-bit KEK",
			Kek:      "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F",
			Data:     "00112233445566778899AABBCCDDEEFF0001020304050607",
			Expected: "A8F9BC1612C68B3FF6E6F4FBE30E71E4769C8B80A32CB8958CD5D17D6B254DA1",
		},
		{
			Case:     "4.6 Wrap 256 bits of Key Data with a 256-bit KEK",
			Kek:      "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F",
			Data:     "00112233445566778899AABBCCDDEEFF000102030405060708090A0B0C0D0E0F",
			Expected: "28C9F404C4B810F4CBCCB35CFB87F8263F5786E2D80ED326CBC7F0E71A99F43BFB988B9B7A02DD21",
		},
	}

	for _, v := range vectors {
		t.Log("Testcase", "\t", v.Case)
		t.Log("kek", "\t = ", v.Kek)
		t.Log("data", "\t = ", v.Data)
		t.Log("exp", "\t = ", v.Expected)

		kek := mustHexDecode(v.Kek)
		data := mustHexDecode(v.Data)
		exp := mustHexDecode(v.Expected)

		cipher, err := aes.NewCipher(kek)
		if !assert.NoError(t, err, "NewCipher should not fail!") {
			continue
		}

		actual, err := Wrap(cipher, data)
		if !assert.NoError(t, err, "Wrap should not throw error with valid input") {
			continue
		}
		if !assert.Equal(t, exp, actual, "Wrap Mismatch: Actual wrapped ciphertext should equal expected for test case '%s'", v.Case) {
			continue
		}

		actualUnwrapped, err := Unwrap(cipher, actual)
		if !assert.NoError(t, err, "Unwrap should not throw error with valid input") {
			continue
		}
		if !assert.Equal(t, data, actualUnwrapped, "Unwrap Mismatch: Actual unwrapped ciphertext should equal the original data for test case '%s'", v.Case) {
			continue
		}
	}
}

func mustHexDecode(s string) (b []byte) {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}
